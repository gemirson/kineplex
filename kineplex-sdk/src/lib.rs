//! KinePlex SDK - Client library for submitting graphs and managing executions

pub mod client;
pub mod types;

pub use client::KinePlexClient;
pub use types::{GraphSubmitRequest, GraphStatusResponse};

use kineplex_core::{GraphId, GraphStatus};
use serde::{Deserialize, Serialize};

/// SDK version
pub const SDK_VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    #[test]
    fn test_version() {
        assert!(!super::SDK_VERSION.is_empty());
    }
}