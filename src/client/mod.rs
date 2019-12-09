// This module contains the definition of `Client`, `ClientId` and `Rifl`.
pub mod client;

// This module contains the definition of `Workload`.
pub mod workload;

// Re-exports.
pub use client::{Client, ClientId, Rifl, RiflGen};
pub use workload::Workload;
