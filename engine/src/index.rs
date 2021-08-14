use std::sync::Arc;

use serde::{Serialize, Deserialize};
use anyhow::{Error, Result};

use tokio::sync::oneshot;
use tokio::sync::Semaphore;

use crossbeam::channel;
use crossbeam::queue::SegQueue;

use tantivy::schema::{Schema, Field, NamedFieldDocument};
use tantivy::query::{QueryParser, Query, Occur, FuzzyTermQuery, BooleanQuery};
use tantivy::{Document, IndexWriter, Term, IndexReader, ReloadPolicy, LeasedItem, Searcher, DocAddress, Score};
use tantivy::{Index, IndexBuilder, Executor};
use tantivy::collector::TopDocs;
use tantivy::query::MoreLikeThisQuery;

use crate::structures::{IndexStorageType, LoadedIndex, QueryPayload, QueryMode, RefAddress};
use std::convert::TryFrom;

/// A writing operation to be sent to the `IndexWriterWorker`.
#[derive(Debug)]
enum WriterOp {
    /// Commits the current changes and flushes to storage.
    Commit,

    /// Removes any changes since the last commit.
    Rollback,

    /// Adds a document to the index.
    AddDocument(Document),

    /// Deletes any documents matching the given term.
    DeleteTerm(Term),

    /// Removes all documents from the index.
    DeleteAll,

    /// Shutdown the handler.
    __Shutdown,
}

/// A background task that applies write operations to the index.
///
/// This system uses the actor model receiving a stream of messages
/// and processes them in order of being sent.
///
/// Messages are ran in a new thread.
pub struct IndexWriterWorker {
    index_name: String,
    writer: IndexWriter,
    waiters: Arc<SegQueue<oneshot::Sender<()>>>,
    rx: channel::Receiver<WriterOp>,
}

impl IndexWriterWorker {
    /// Starts processing messages until a shutdown operation is sent.
    ///
    /// This processes operations in waves before waking up waiters,
    /// this means all operations currently in the queue will be processed
    /// first before any waiters are woken up to send more data.
    fn start(mut self) {
        loop {
            if self.process_messages() {
                break;
            };

            // Wake up waiters once a message has been removed.
            while let Some(waiter) = self.waiters.pop() {
                let _ = waiter.send(());
            }
        }

        // Unlock waiters so that they dont deadlock the system.
        while let Some(waiter) = self.waiters.pop() {
            let _ = waiter.send(());
        }
    }

    /// Purges all pending operations from the receiver.
    fn process_messages(&mut self) -> bool {
        while let Ok(msg) = self.rx.try_recv() {
            match self.handle_msg(msg) {
                Err(e) => error!(
                    "[ WRITER @ {} ] failed handling writer operation on index due to error: {:?}",
                    &self.index_name, e,
                ),
                Ok(true) => return true,
                _ => {}
            }
        }

        false
    }

    fn handle_msg(&mut self, op: WriterOp) -> Result<bool> {
        let (transaction_id, type_) = match op {
            WriterOp::__Shutdown => return Ok(true),
            WriterOp::Commit => (self.writer.commit()?, "COMMIT"),
            WriterOp::Rollback => (self.writer.rollback()?, "ROLLBACK"),
            WriterOp::AddDocument(docs) => (self.writer.add_document(docs), "ADD-DOCUMENT"),
            WriterOp::DeleteAll => (self.writer.delete_all_documents()?, "DELETE-ALL"),
            WriterOp::DeleteTerm(term) => (self.writer.delete_term(term), "DELETE-TERM"),
        };


        info!(
            "[ WRITER @ {} ][ {} ] completed operation {}",
            &self.index_name, transaction_id, type_
        );

        Ok(false)
    }
}

/// A simple wrapper handler around a set of queues and a worker.
///
/// This manages creating the waiters and scheduling the operations
/// in a new thread.
struct IndexWriterHandler {
    index_name: String,
    writer_thread: std::thread::JoinHandle<()>,
    writer_waiters: Arc<SegQueue<oneshot::Sender<()>>>,
    writer_sender: crossbeam::channel::Sender<WriterOp>,
}

impl IndexWriterHandler {
    /// Creates a new writer handler from a given index name and
    /// a given index writer.
    ///
    /// This creates a bounded queue with a capacity of 20 and
    /// spawns a worker in a new thread.
    fn create(index_name: String, writer: IndexWriter) -> Self {
        let name = index_name.clone();
        let waiters = Arc::new(SegQueue::new());
        let (tx, rx) = channel::bounded(20);
        let worker = IndexWriterWorker {
            index_name: index_name.clone(),
            writer,
            waiters: waiters.clone(),
            rx,
        };

        let handle = std::thread::Builder::new()
            .name(format!("index-worker-{}", &index_name))
            .spawn(move || {
            let id = std::thread::current().id();
            info!(
                "[ WRITER @ {} ] writer thread started with id {:?}",
                name, id
            );
            worker.start()
        }).expect("spawn worker thread");

        Self {
            index_name,
            writer_thread: handle,
            writer_sender: tx,
            writer_waiters: waiters,
        }
    }

    /// Sends a message to the writer worker
    ///
    /// If there is space in the queue this will complete immediately
    /// otherwise this will wait until it's woken up again.
    async fn send_op(&self, op: WriterOp) -> anyhow::Result<()> {
        let mut op = op;
        loop {
            op = match self.writer_sender.try_send(op) {
                Ok(()) => return Ok(()),
                Err(channel::TrySendError::Disconnected(_)) => {
                    return Err(Error::msg("writer worker has shutdown"))
                }
                Err(channel::TrySendError::Full(v)) => v,
            };

            debug!(
                "[ WRITER @ {} ] operation queue full, waiting for wakeup",
                &self.index_name
            );

            let (resolve, waiter) = oneshot::channel();
            self.writer_waiters.push(resolve);
            let _ = waiter.await;
        }
    }
}



/// A async manager around the tantivy index reader.
///
/// This system executes the read operations in a given thread pool
/// managed by rayon which will allow a concurrency upto the set
/// `max_concurrency`.
///
/// If the system is at it's maximum concurrency already and search
/// is called again, it will temporarily suspend operations until
/// a reader has been freed.
struct IndexReaderHandler {
    /// The internal tantivy index reader.
    reader: IndexReader,

    /// The reader thread pool executor.
    ///
    /// If the number of reader threads is > 1 this is a MultiThreaded executor
    /// otherwise it's SingleThreaded.
    executor: Arc<Executor>,

    /// A concurrency semaphore.
    limiter: Semaphore,

    /// The execution thread pool.
    thread_pool: rayon::ThreadPool,

    parser: QueryParser,

    search_fields: Vec<Field>,

    quick_schema: Arc<Schema>
}

impl IndexReaderHandler {
    /// Creates a new reader handler from an existing tantivy index reader.
    ///
    /// This will spawn a thread pool with `n` amount of threads equal
    /// to the set `max_concurrency`.
    fn create(
        index_name: String,
        max_concurrency: usize,
        reader: IndexReader,
        reader_threads: usize,
        parser: QueryParser,
        search_fields: Vec<Field>,
        quick_schema: Arc<Schema>,
    ) -> Result<Self> {
        let limiter = Semaphore::new(max_concurrency);

        let name = index_name.clone();
        let thread_pool = {
            rayon::ThreadPoolBuilder::new()
                .num_threads(max_concurrency)
                .thread_name(move |n| format!("index-{}-worker-{}", name.clone(), n))
                .build()?
        };

        let executor = if reader_threads > 1 {
            Executor::multi_thread(reader_threads, "index-reader-")?
        } else {
            Executor::single_thread()
        };

        let executor =  Arc::new(executor);

        Ok(Self {
            reader,
            executor,
            limiter,
            thread_pool,
            parser,
            search_fields,
            quick_schema,
        })
    }

    /// Searches the index with a given query.
    ///
    /// The index will use fuzzy matching based on levenshtein distance
    /// if set to true.
    async fn search(&self, payload: QueryPayload) -> Result<()> {
        let _permit = self.limiter.acquire().await?;

        let (resolve, waiter) = oneshot::channel();

        let doc = if let Some(doc) = payload.ref_document {
            Some(RefAddress::try_from(doc)?)
        } else {
            None
        };

        let order_by = if let Some(field) = payload.order_by {
            // We choose to ignore the order by if the field doesnt exist.
            // While this may be surprising to be at first as long as it's
            // document this should be fine.
            self.quick_schema.get_field(&field)
        } else {
            None
        };

        let schema = self.quick_schema.clone();
        let limit = payload.limit;
        let offset = payload.offset;
        let query = self.parse_query(
            payload.query,
            doc,
            payload.mode,
        )?;
        let searcher = self.reader.searcher();
        let executor = self.executor.clone();

        self.thread_pool.spawn(move || {
            let res = search(
                query,
                searcher,
                executor,
                limit,
                offset,
                schema,
                order_by,
            );
            let _ = resolve.send(res);
        });

        let _ = waiter.await;

        todo!()
    }

    fn parse_query(
        &self,
        query: Option<String>,
        ref_document: Option<RefAddress>,
        mode: QueryMode,
    ) -> Result<Box<dyn Query>> {
        let start = std::time::Instant::now();
        let out = match (mode, &query, &ref_document) {
            (QueryMode::Normal, None, _) =>
                Err(Error::msg("query mode was `Normal` but query string is `None`")),
            (QueryMode::Normal, Some(query), _) =>
                Ok(self.parser.parse_query(query)?),
            (QueryMode::Fuzzy, None, _) =>
                Err(Error::msg("query mode was `Fuzzy` but query string is `None`")),
            (QueryMode::Fuzzy, Some(query), _) =>
                Ok(self.parse_fuzzy_query(query)),
            (QueryMode::MoreLikeThis, _, None) =>
                Err(Error::msg("query mode was `MoreLikeThis` but reference document is `None`")),
            (QueryMode::MoreLikeThis, _, Some(ref_document)) =>
                Ok(self.parse_more_like_this(ref_document)),
        };

        debug!(
            "constructing query {:?} or ref_doc {:?} with mode={:?} took {:?}",
            query, ref_document, &mode, start.elapsed(),
        );

        return out;
    }

    fn parse_fuzzy_query(&self, query: &str) -> Box<dyn Query> {
        let mut parts: Vec<(Occur, Box<dyn Query>)> = Vec::new();

        for search_term in query.to_lowercase().split(" ") {
            if search_term.is_empty() {
                continue;
            }

            for field in self.search_fields.iter() {
                parts.push((
                    Occur::Should,
                    Box::new(FuzzyTermQuery::new_prefix(
                        Term::from_field_text(*field, search_term),
                        1,
                        true,
                    )),
                ))
            }
        };

        Box::new(BooleanQuery::from(parts))
    }

    fn parse_more_like_this(&self, ref_document: &RefAddress) -> Box<dyn Query> {
        let query = MoreLikeThisQuery::builder()
            .with_min_doc_frequency(1)
            .with_max_doc_frequency(10)
            .with_min_term_frequency(1)
            .with_min_word_length(2)
            .with_max_word_length(5)
            .with_boost_factor(1.0)
            .with_stop_words(vec!["for".to_string()])
            .with_document(ref_document.as_doc_address());

        Box::new(query)
    }
}


/// Represents a single query result.
#[derive(Serialize)]
pub struct QueryHit {
    /// The address of the given document, this can be used for
    /// 'more like this' queries.
    ref_address: String,

    /// The content of the document itself.
    doc: NamedFieldDocument,
}

/// Represents the overall query result(s)
#[derive(Serialize)]
pub struct QueryResults {
    /// The retrieved documents.
    hits: Vec<QueryHit>,

    /// The total amount of documents
    count: usize,

    /// The amount of time taken to search in seconds.
    time_taken: f64,
}


macro_rules! search {
    ( $search:expr, $schema:expr, $top_docs:expr ) => {{
        let count = $top_docs.len();

        let mut hits = Vec::with_capacity(count);
        for (_, ref_address) in $top_docs {
            let retrieved_doc = $search.doc(ref_address)?;
            let doc = $schema.to_named_doc(&retrieved_doc);
            hits.push(QueryHit{ ref_address: RefAddress::from(ref_address).into(), doc });
        }

        (count, hits)
    }}
}


/// Executes a search for a given query with a given searcher, limit and schema.
///
/// This will process and time the execution time to build into the exportable
/// data.
fn search(
    query: Box<dyn Query>,
    searcher: LeasedItem<Searcher>,
    executor: Arc<Executor>,
    limit: usize,
    offset: usize,
    schema: Arc<Schema>,
    order_by: Option<Field>,
) -> Result<QueryResults> {
    let start = std::time::Instant::now();

    let collector = TopDocs::with_limit(limit)
        .and_offset(offset);

    let out = searcher.search_with_executor(&query, &collector, &executor)?;
    let (count, hits) = search!(searcher, schema, out);

    let elapsed = start.elapsed();
    let time_taken = elapsed.as_secs_f64();

    debug!("search took {:?} with limit: {}", elapsed, limit);

    Ok(QueryResults{
        time_taken,
        hits,
        count,
    })
}


/// A search engine index.
///
/// Each index maintains a rayon thread pool which searches are executed
/// as well as an worker thread which is used to interact with the index writer.
///
/// The amount of threads `n` is determined by the the `max_concurrency` parameter.
///
/// ### Mutating the index behaviour:
/// This system simple schedules the operations in the order they are invoked
/// however, this system does not wait for the operation to be completed.
/// This essentially follows the behaviour of eventual consistency; The operations
/// are guaranteed to be applied within some time in the near future.
pub struct IndexHandler {
    /// The name of the index.
    name: String,

    /// The internal tantivy index.
    index: Index,

    /// The internal tantivy schema.
    schema: Schema,

    /// A writer actor to handle the index writer.
    writer: IndexWriterHandler,

    /// The index reader handler
    reader: IndexReaderHandler,
}

impl IndexHandler {
    /// Creates a new index handler from a given loaded index.
    ///
    /// This constructs both the Tantivy index, thread pool and worker thread.
    ///
    /// ### Important note about performance:
    /// The concurrency limit should be set according to the machine
    /// this system is being deployed on hence being a required field.
    /// The amount of threads spawned is equal the the max  concurrency + 1
    /// as well as the tokio runtime threads.
    pub fn build_loaded(loader: LoadedIndex) -> Result<Self> {
        let quick_schema = Arc::new(loader.schema.clone());
        let index = IndexBuilder::default().schema(loader.schema.clone());

        let index = match loader.storage_type {
            IndexStorageType::TempFile => index.create_from_tempdir()?,
            IndexStorageType::Memory => index.create_in_ram()?,
            IndexStorageType::FileSystem(path) => index.create_in_dir(path)?,
        };

        // We need to extract out the fields from name to id.
        let mut search_fields = vec![];
        for field in loader.search_fields {
            if let Some(field) = loader.schema.get_field(&field) {
                search_fields.push(field);
            } else {
                return Err(Error::msg(format!(
                    "no field exists for index {} with the current schema,\
                     did you forget to define it in the schema?", &field
                )))
            };
        }

        let mut parser = QueryParser::for_index(&index, search_fields.clone());
        for (name, factor) in loader.boost_fields {
            if let Some(field) = loader.schema.get_field(&name) {
                parser.set_field_boost(field, factor);
            } else {
                return Err(Error::msg(format!(
                    "no field exists for index {} with the current schema,\
                     did you forget to define it in the schema?", &name
                )))
            };
        }

        let writer = index.writer_with_num_threads(loader.writer_threads, loader.writer_buffer)?;
        let reader = index.reader_builder()
            .num_searchers(loader.max_concurrency as usize)
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()?;

        let worker_handler = IndexWriterHandler::create(loader.name.clone(), writer);

        let reader_handler = IndexReaderHandler::create(
            loader.name.clone(),
            loader.max_concurrency as usize,
            reader,
            loader.reader_threads as usize,
            parser,
            search_fields,
            quick_schema,
        )?;

        Ok(Self {
            name: loader.name,
            index,
            schema: loader.schema,
            writer: worker_handler,
            reader: reader_handler,
        })
    }

    /// Submits a document to be processed by the index writer.
    pub async fn add_document(&self, document: Document) -> Result<()> {
        self.writer.send_op(WriterOp::AddDocument(document)).await
    }

    /// Submits many documents to the index writer.
    ///
    /// This is just an alias for adding documents in a loop.
    pub async fn add_many_documents(&self, documents: Vec<Document>) -> Result<()> {
        for doc in documents {
            self.add_document(doc).await?
        }

        Ok(())
    }

    /// Submits the delete all operation to the index writer.
    ///
    /// This will delete all documents in the index which were
    /// added since the last commit.
    pub async fn delete_documents(&self) -> Result<()> {
        self.writer.send_op(WriterOp::DeleteAll).await
    }

    /// Submits the delete term operation to the index writer.
    ///
    /// This will delete all documents matching the term which were
    /// added since the last commit.
    pub async fn delete_term(&self, term: Term) -> Result<()> {
        self.writer.send_op(WriterOp::DeleteTerm(term)).await
    }

    /// Submits the commit operation to the index writer.
    ///
    /// This will finalize any operations and save the changes, flushing them
    /// to disk.
    ///
    /// Any additions and deletions will become visible to readers once
    /// the operation is complete.
    pub async fn commit(&self) -> Result<()> {
        self.writer.send_op(WriterOp::Commit).await
    }

    /// Submits the rollback operation to the index writer.
    ///
    /// This will undo / drop any changes made between the last commit
    /// and the rollback operation.
    pub async fn rollback(&self) -> Result<()> {
        self.writer.send_op(WriterOp::Rollback).await
    }

    /// Searches the index with the given query.
    pub async fn search(&self, payload: QueryPayload) -> Result<()> {
        self.reader.search(payload).await
    }

}
