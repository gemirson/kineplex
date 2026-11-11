pub mod delta;
pub mod iceberg;
pub mod monoid;
pub mod ssi;

pub use delta::DeltaLogSerializer;
pub use iceberg::IcebergManifestLattice;
pub use monoid::UnifiedDataFileAction;
pub use ssi::can_commute_concurrent_commit;

/// Contrato formal para motores de commit transacional unificado Lakehouse
pub trait LakehouseCommitEngine: Send + Sync {
    fn serialize_delta_log(&self, actions: &[UnifiedDataFileAction]) -> Vec<String>;

    fn validate_ssi_commutativity(
        &self,
        concurrent_actions: &[UnifiedDataFileAction],
        read_bounds_min: &[f64],
        read_bounds_max: &[f64],
    ) -> bool;
}

pub struct UnifiedLakehouseEngine;

impl LakehouseCommitEngine for UnifiedLakehouseEngine {
    fn serialize_delta_log(&self, actions: &[UnifiedDataFileAction]) -> Vec<String> {
        DeltaLogSerializer::serialize_actions(actions)
    }

    fn validate_ssi_commutativity(
        &self,
        concurrent_actions: &[UnifiedDataFileAction],
        read_bounds_min: &[f64],
        read_bounds_max: &[f64],
    ) -> bool {
        can_commute_concurrent_commit(concurrent_actions, read_bounds_min, read_bounds_max)
    }
}
