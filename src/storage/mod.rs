//! Storage layout abstraction.
//! Long-term goal: Abstracts filesystem or blob storage for index files and blobs.

pub struct StorageManager {
    pub path: String,
}

impl StorageManager {
    pub fn new(path: String) -> Self {
        Self { path }
    }
}
