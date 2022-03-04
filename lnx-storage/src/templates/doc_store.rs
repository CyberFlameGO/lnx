use std::collections::HashSet;
use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use lnx_common::types::document::{DocId, Document};

use super::change_log::ChangeLogStore;
use crate::types::SegmentId;


#[async_trait]
pub trait DocStore: ChangeLogStore + Send + Sync + 'static {
    /// Adds a set of documents to the store.
    async fn add_documents(
        &self,
        docs: &[(DocId, Document)],
    ) -> Result<HashSet<SegmentId>>;

    /// Removes a set of documents from the store.
    async fn remove_documents(&self, docs: Vec<DocId>) -> Result<HashSet<SegmentId>>;

    /// Clears all documents from the store.
    async fn clear_documents(&self) -> Result<()>;

    /// Fetches a set of documents with the provided Ids.
    ///
    /// If `fields` is `None` all fields on the document should be returned
    /// which are inline with the schema.
    async fn fetch_document(
        &self,
        fields: Option<Vec<String>>,
        docs: DocId,
    ) -> Result<Option<(DocId, SegmentId, Document)>>;

    /// Used to get all documents from the store.
    async fn iter_documents(
        &self,
        fields: Option<Vec<String>>,
        chunk_size: usize,
        segment_id: Option<SegmentId>,
    ) -> Result<DocumentIterator>;
}

pub struct DocumentIterator {
    rx: mpsc::Receiver<Vec<(DocId, SegmentId, Document)>>,
    handle: JoinHandle<()>,
}

impl DocumentIterator {
    /// Produces a iterator from a given receiver and tokio handle.
    ///
    /// The handle is used for task cleanup after the iterator has been dropped.
    pub fn from_rx_and_handle(
        rx: mpsc::Receiver<Vec<(DocId, SegmentId, Document)>>,
        handle: JoinHandle<()>,
    ) -> DocumentIterator {
        Self { rx, handle }
    }

    /// Get the next chunk from the query.
    pub async fn next(&mut self) -> Option<Vec<(DocId, SegmentId, Document)>> {
        self.rx.recv().await
    }
}

impl Drop for DocumentIterator {
    fn drop(&mut self) {
        self.rx.close();
        self.handle.abort();
    }
}
