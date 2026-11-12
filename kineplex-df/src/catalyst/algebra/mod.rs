//! # Catalyst Algébrico (`catalyst::algebra`)
//!
//! Passes opcionais do otimizador, ligados por `CatalystCompilerOptimizer::with_algebraic(true)`.
//! Com a flag desligada (padrão) nada aqui é executado e o comportamento é idêntico ao anterior.
//!
//! Design: `docs/plans/catalyst_algebra_design.md`.

pub mod absdom;
pub mod canon;

pub use absdom::{
    eval_interval, simplify_expr_with_absdom, simplify_stage_with_absdom, AbstractEnv, Interval,
};
pub use canon::{canonicalize_expr, canonicalize_stage, canonicalize_stage_targeted};
