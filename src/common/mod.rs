//! Shared types and utilities.
//! Long-term goal: Shared types, utilities, and error handling for the entire node.

pub type Result<T> = std::result::Result<T, anyhow::Error>;
