use std::sync::atomic::AtomicU32;

static ID: AtomicU32 = AtomicU32::new(0);

/// Retrieve a new, globally-unique ID.
pub fn new_id() -> u32 {
    ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}
