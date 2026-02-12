use std::{cell::UnsafeCell, sync::Arc};

// CheatBackend is a global backend that a Move VM globally holds
pub struct CheatBackendInner<T> {
    pub db: T,
}

impl<T> CheatBackendInner<T> {
    pub fn new(db: T) -> Self {
        Self { db }
    }

    // Must be called before every ptb execution
    pub fn reset(&mut self) {}

    pub fn end_transaction(&mut self) {}
}

// Tight wrapper of CheatBackendInner to satisfy the contract of Sui natives
// Note it is ub to call cheat backend concurrently.
pub struct CheatBackend<T> {
    inner: Arc<UnsafeCell<CheatBackendInner<T>>>,
}

impl<T> Clone for CheatBackend<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

// SAFETY: We guarantee single-threaded access
unsafe impl<T> Send for CheatBackend<T> {}
unsafe impl<T> Sync for CheatBackend<T> {}

impl<T> CheatBackend<T> {
    pub fn new(db: T) -> Self {
        Self {
            inner: Arc::new(UnsafeCell::new(CheatBackendInner::new(db))),
        }
    }

    pub fn inner(&self) -> &CheatBackendInner<T> {
        // SAFETY: No concurrent access exists by design.
        unsafe { &*self.inner.get() }
    }

    pub fn inner_mut(&self) -> &mut CheatBackendInner<T> {
        // SAFETY: No concurrent access exists by design.
        unsafe { &mut *self.inner.get() }
    }
}
