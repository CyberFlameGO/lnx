use std::collections::HashSet;
use std::path::Path;
use serde_json::Value;
use lnx_common::index::context::IndexContext;
use lnx_common::types::document::{DocId, Document};
use lnx_storage::templates::setup::SetupForIndex;
use lnx_storage::async_trait;
use lnx_storage::templates::change_log::{ChangeLogEntry, ChangeLogIterator, ChangeLogStore};
use lnx_storage::templates::doc_store::{DocStore, DocumentIterator};
use lnx_storage::templates::meta_store::{MetaStore, Synonyms};
use lnx_storage::types::{SegmentId, Timestamp};

pub struct ScyllaIndexStore {

}

#[async_trait]
impl SetupForIndex for ScyllaIndexStore {
    async fn setup(ctx: IndexContext, config: Value) -> anyhow::Result<()> {
        todo!()
    }
}

#[async_trait]
impl DocStore for ScyllaIndexStore {
    async fn add_documents(&self, docs: &[(DocId, Document)]) -> anyhow::Result<HashSet<SegmentId>> {
        todo!()
    }

    async fn remove_documents(&self, docs: Vec<DocId>) -> anyhow::Result<HashSet<SegmentId>> {
        todo!()
    }

    async fn clear_documents(&self) -> anyhow::Result<()> {
        todo!()
    }

    async fn fetch_document(&self, fields: Option<Vec<String>>, docs: DocId) -> anyhow::Result<Option<(DocId, SegmentId, Document)>> {
        todo!()
    }

    async fn iter_documents(&self, fields: Option<Vec<String>>, chunk_size: usize, segment_id: Option<SegmentId>) -> anyhow::Result<DocumentIterator> {
        todo!()
    }
}

#[async_trait]
impl ChangeLogStore for ScyllaIndexStore {
    async fn setup(&self) -> anyhow::Result<()> {
        todo!()
    }

    async fn append_changes(&self, logs: ChangeLogEntry) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_pending_changes(&self, from: Timestamp) -> anyhow::Result<ChangeLogIterator> {
        todo!()
    }

    async fn count_pending_changes(&self, from: Timestamp) -> anyhow::Result<usize> {
        todo!()
    }
}

#[async_trait]
impl MetaStore for ScyllaIndexStore {
    async fn setup(&self) -> anyhow::Result<()> {
        todo!()
    }

    async fn add_stopwords(&self, words: Vec<String>) -> anyhow::Result<()> {
        todo!()
    }

    async fn remove_stopwords(&self, words: Vec<String>) -> anyhow::Result<()> {
        todo!()
    }

    async fn fetch_stopwords(&self) -> anyhow::Result<Vec<String>> {
        todo!()
    }

    async fn add_synonyms(&self, words: Vec<Synonyms>) -> anyhow::Result<()> {
        todo!()
    }

    async fn remove_synonyms(&self, words: Vec<String>) -> anyhow::Result<()> {
        todo!()
    }

    async fn fetch_synonyms(&self) -> anyhow::Result<Vec<Synonyms>> {
        todo!()
    }

    async fn set_update_timestamp(&self, timestamp: time::duration::Duration) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_last_update_timestamp(&self) -> anyhow::Result<Option<time::duration::Duration>> {
        todo!()
    }

    async fn load_index_from_peer(&self, out_dir: &Path) -> anyhow::Result<()> {
        todo!()
    }

    async fn heartbeat(&self, purge_delta: time::duration::Duration) -> anyhow::Result<()> {
        todo!()
    }
}