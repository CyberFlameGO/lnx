use std::ops::Deref;
use std::sync::Arc;

use dashmap::DashMap;
use lnx_common::index::base::Index;
use lnx_common::index::context::IndexContext;
use lnx_storage::polling::PollingMode;
use lnx_storage::stores::IndexStore;
use lnx_storage::templates::doc_store::DocStore;
use lnx_storage::templates::meta_store::MetaStore;
use lnx_storage::templates::setup::SetupForIndex;
use once_cell::sync::OnceCell;
use scylladb_backend::ScyllaIndexStore;

use crate::backends::IndexStorageConfig;

static INDEXES: OnceCell<DashMap<String, IndexStore>> = OnceCell::new();

#[inline]
/// Gets the index store for the given index if it exists.
pub fn get(index_name: &str) -> Option<IndexStore> {
    Some(INDEXES.get()?.get(index_name)?.deref().clone())
}

#[inline]
/// Removes the index and returns it if it exists.
///
/// This can be used to shutdown and cleanup the index.
pub fn remove(index_name: &str) -> Option<IndexStore> {
    Some(INDEXES.get()?.remove(index_name)?.1)
}

#[inline]
/// Creates a new index from the given context, index, polling mode and
/// storage backend configuration.
pub async fn new(
    ctx: IndexContext,
    index: Index,
    polling_mode: PollingMode,
    config: IndexStorageConfig,
) -> anyhow::Result<()> {
    let doc_store: Arc<dyn DocStore> = match config.clone() {
        IndexStorageConfig::Scylla(cfg) => {
            Arc::new(ScyllaIndexStore::setup(ctx.clone(), cfg).await?)
        },
    };

    let store = IndexStore::new(ctx, index, polling_mode, doc_store);

    let indexes = INDEXES.get_or_init(DashMap::new);
    indexes.insert(ctx.name().to_string(), store);

    Ok(())
}
