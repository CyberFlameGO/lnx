use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use lnx_common::index::base::Index;
use lnx_common::index::context::IndexContext;
use lnx_utils::{FromBytes, ToBytes};

use crate::templates::doc_store::DocStore;

#[derive(Clone)]
/// A Index's storage and configuration settings.
///
/// Additional settings can be added via the `store` and `load` method.
pub struct IndexStore {
    ctx: IndexContext,
    index: Index,
    store: Arc<dyn DocStore>,
    meta_store: sled::Db,
}

impl Deref for IndexStore {
    type Target = Arc<dyn DocStore>;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

impl IndexStore {
    pub fn new(
        ctx: IndexContext,
        index: Index,
        store: Arc<dyn DocStore>,
        meta_store: sled::Db,
    ) -> Self {
        Self {
            ctx,
            index,
            store,
            meta_store,
        }
    }

    #[inline]
    pub fn index(&self) -> Index {
        self.index.clone()
    }

    #[inline]
    pub fn ctx(&self) -> &IndexContext {
        &self.ctx
    }

    #[inline]
    pub fn meta_store(&self) -> &sled::Db {
        &self.meta_store
    }

    /// Updates the current settings for the given key.
    ///
    /// These changes are reflected in the database.
    #[instrument(name = "index-settings-store", skip(self, settings))]
    pub async fn store<T: ToBytes>(&self, key: &str, settings: T) -> Result<()> {
        let instant = Instant::now();
        self.update_settings(key, settings.to_bytes()?).await?;
        info!("Settings store took {:?}", instant.elapsed());

        Ok(())
    }

    /// Removes the current settings for the given key.
    ///
    /// These changes are reflected in the database.
    #[instrument(name = "index-settings-remove", skip(self))]
    pub async fn remove(&self, key: &str) -> Result<()> {
        let instant = Instant::now();
        self.remove_settings(key).await?;
        info!("Settings update took {:?}", instant.elapsed());

        Ok(())
    }

    /// Loads the latest current settings for the given key.
    ///
    /// These changes are reflected in the database.
    #[instrument(name = "index-settings-load", skip(self))]
    pub async fn load<T: FromBytes>(&self, key: &str) -> Result<Option<T>> {
        let instant = Instant::now();
        let settings = self.load_settings(key).await?;
        info!("Settings load took {:?}", instant.elapsed());

        settings
            .map(|buff| T::from_bytes(&buff).map_err(anyhow::Error::from))
            .transpose()
    }

    /// Shuts down and removes all of the index content from the local store.
    ///
    /// Note:
    ///     This does nto remove any information from the global storage backend.
    pub async fn destroy(self, base_path: &Path) -> Result<()> {
        self.ctx.clear_local_data(base_path)?;

        Ok(())
    }
}
