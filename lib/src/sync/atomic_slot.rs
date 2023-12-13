//! Wrapper that allows non-blocking concurrent read or replace of the underlying value.

use crate::deadlock::BlockingMutex;
use std::{mem, ops::Deref, sync::Arc};

pub(crate) struct AtomicSlot<T>(BlockingMutex<Arc<T>>);

impl<T> AtomicSlot<T> {
    pub fn new(value: T) -> Self {
        Self(BlockingMutex::new(Arc::new(value)))
    }

    /// Obtain read access to the underlying value without blocking.
    pub fn read(&self) -> Guard<T> {
        Guard(self.0.lock().unwrap().clone())
    }

    /// Atomically replace the current value with the provided one and returns the previous one.
    pub fn swap(&self, value: T) -> Guard<T> {
        let value = Arc::new(value);
        Guard(mem::replace(&mut *self.0.lock().unwrap(), value))
    }
}

/// Handle to provide read access to a value stored in `AtomicSlot`.
pub(crate) struct Guard<T>(Arc<T>);

impl<T> From<T> for Guard<T> {
    fn from(value: T) -> Self {
        Self(Arc::new(value))
    }
}

impl<T> Deref for Guard<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}
