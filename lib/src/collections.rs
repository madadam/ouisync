//! Drop-in replacements for `std::collections::{HashMap, HashSet, hash_map, hash_set}` which
//! uses `RandomState` from the `ouisync-rand` crate which makes them deterministic when the
//! `simulation` feature is enabled.

pub use self::{hash_map::HashMap, hash_set::HashSet};

pub mod hash_map {
    pub use rand::RandomState;
    pub use std::collections::hash_map::Entry;

    pub type HashMap<K, V, S = RandomState> = std::collections::HashMap<K, V, S>;
}

pub mod hash_set {
    pub use std::collections::hash_set::IntoIter;

    use super::hash_map::RandomState;

    pub type HashSet<T, S = RandomState> = std::collections::HashSet<T, S>;
}