use std::sync::atomic::AtomicU64;

static ID: AtomicU64 = AtomicU64::new(0);

/// Retrieve a new, globally-unique ID.
pub fn new_id() -> u64 {
    ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}
