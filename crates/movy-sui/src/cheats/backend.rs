use std::{
    cell::UnsafeCell,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use sui_types::{
    TypeTag,
    base_types::{MoveObjectType, ObjectID},
};

use crate::database::cache::CachedSnapshot;

// CheatBackend is a global backend that a Move VM globally holds
pub struct CheatBackendInner {
    pub storage: CachedSnapshot,
    pub tys: BTreeMap<MoveObjectType, BTreeSet<ObjectID>>,
    pub latest_objects_by_types: BTreeMap<MoveObjectType, ObjectID>,
    pub taken: BTreeSet<ObjectID>,
}

impl CheatBackendInner {
    pub fn new(storage: CachedSnapshot) -> Self {
        let mut tys: BTreeMap<MoveObjectType, BTreeSet<ObjectID>> = BTreeMap::new();

        for (id, mp) in storage.objects.iter() {
            for (_, obj) in mp.iter() {
                if let Some(obj) = obj {
                    if let Some(ty) = obj.type_() {
                        tys.entry(ty.clone()).or_default().insert(*id);
                    }
                }
            }
        }
        Self {
            storage,
            tys,
            latest_objects_by_types: BTreeMap::new(),
            taken: BTreeSet::new(),
        }
    }

    // Must be called before every ptb execution
    pub fn reset(&mut self) {}

    pub fn end_transaction(&mut self) {}
}

// Tight wrapper of CheatBackendInner to satisfy the contract of Sui natives
// Note it is ub to call cheat backend concurrently.
pub struct CheatBackend {
    inner: Arc<UnsafeCell<CheatBackendInner>>,
}

impl Clone for CheatBackend {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

// SAFETY: We guarantee single-threaded access
unsafe impl Send for CheatBackend {}
unsafe impl Sync for CheatBackend {}

impl CheatBackend {
    pub fn new(storage: CachedSnapshot) -> Self {
        Self {
            inner: Arc::new(UnsafeCell::new(CheatBackendInner::new(storage))),
        }
    }

    pub fn inner(&self) -> &CheatBackendInner {
        // SAFETY: No concurrent access exists by design.
        unsafe { &*self.inner.get() }
    }

    pub fn inner_mut(&self) -> &mut CheatBackendInner {
        // SAFETY: No concurrent access exists by design.
        unsafe { &mut *self.inner.get() }
    }
}
