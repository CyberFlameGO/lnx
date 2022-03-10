use anyhow::Result;
use lnx_common::index::context::IndexContext;
use lnx_storage::stores::IndexStore;

mod indexers;
mod task_handler;
mod configure;

pub use indexers::{Indexer, WeakIndexer};


pub async fn new(ctx: IndexContext, index: IndexStore) -> Result<Indexer> {
    todo!()
}