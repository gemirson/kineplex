//! # Catalyst Compiler Optimizer (`catalyst::optimizer`)
//!
//! Técnicas clássicas e modernas de compiladores aplicadas à otimização de consultas
//! e expressões do Catalyst Engine com Simpatia Mecânica de Hardware:
//!
//! 1. **Constant Folding & Algebraic Simplification:**
//!    Avaliação em tempo de compilação de nós literais e identidades algébricas:
//!    `x + 0 => x`, `x * 1 => x`, `x * 0 => 0`, `x / 1 => x`, `x - x => 0`,
//!    `x == x => true`, `when(true, a, b) => a`, `!(!x) => x`.
//!
//! 2. **Strength Reduction (Redução de Força Aritmética):**
//!    Substituição de operações aritméticas de alta latência (como divisão e módulo que levam
//!    25-40 ciclos de clock no silício) por operações bitwise de 1 ciclo:
//!    - `x % 2^k => x & (2^k - 1)`
//!    - `x * 2^k => x << k`
//!    - `x / 2^k => x >> k`
//!
//! 3. **Common Subexpression Elimination (CSE):**
//!    Identificação de sub-árvores idênticas através de hash estrutural determinístico.
//!    Sub-expressões compartilhadas são computadas uma única vez e mantidas em registradores.
//!
//! 4. **Predicate Pushdown (Descida de Filtros):**
//!    Filtros e congruências aritméticas são empurrados para antes de projeções pesadas
//!    e kernels de fatores financeiros B3/BACEN, descartando linhas antes do processamento.
//!
//! 5. **Dead Column & Dead Code Elimination (DCE):**
//!    Poda de nós `WithColumn` e projeções intermediárias que nunca são lidas a jusante.

use crate::catalyst::{DataType, Expression, ScalarValue};
use crate::sql::LogicalStage;
use std::collections::{HashMap, HashSet};

/// Métricas e estatísticas das passagens de compilação aplicadas.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OptimizationStats {
    pub constants_folded: usize,
    pub strength_reductions: usize,
    pub common_subexpressions: usize,
    pub predicates_pushed: usize,
    pub dead_columns_pruned: usize,
    pub passes_run: usize,
}

impl OptimizationStats {
    pub fn total_transformations(&self) -> usize {
        self.constants_folded
            + self.strength_reductions
            + self.common_subexpressions
            + self.predicates_pushed
            + self.dead_columns_pruned
    }
}

/// Otimizador Catalyst com arquitetura multi-pass até ponto fixo.
pub struct CatalystCompilerOptimizer {
    pub max_iterations: usize,
    /// Liga os passes de `catalyst::algebra` (canonicalização por simetria etc.).
    /// Padrão `false`: com a flag desligada o comportamento é idêntico ao anterior.
    pub algebraic: bool,
}

impl Default for CatalystCompilerOptimizer {
    fn default() -> Self {
        Self {
            max_iterations: 8,
            algebraic: false,
        }
    }
}

impl CatalystCompilerOptimizer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Habilita ou desabilita os passes algébricos opcionais (`catalyst::algebra`).
    pub fn with_algebraic(mut self, enabled: bool) -> Self {
        self.algebraic = enabled;
        self
    }

    /// Estima a Curvatura de Ricci discreta (Ollivier-Ricci) do plano de consulta:
    /// $\kappa = 1 - W_1(\mu_{in}, \mu_{out}) / d$
    /// - $\kappa > 0$: Estágios contrativos (Filtros, Agregações, Projeções enxutas com contração métrica)
    /// - $\kappa < 0$: Estágios dispersivos (Joins expansivos, Uniões com dilatação de espaço)
    pub fn plan_ricci_curvature(stage: &LogicalStage) -> f64 {
        match stage {
            LogicalStage::Filter { input, .. } => {
                0.5 + 0.5 * Self::plan_ricci_curvature(input)
            }
            LogicalStage::Select { input, projections } => {
                let cur = Self::plan_ricci_curvature(input);
                if projections.len() <= 3 { cur + 0.2 } else { cur }
            }
            LogicalStage::WithColumn { input, .. } => {
                Self::plan_ricci_curvature(input) * 0.95
            }
            LogicalStage::Join { left, right, join_type, .. } => {
                let kl = Self::plan_ricci_curvature(left);
                let kr = Self::plan_ricci_curvature(right);
                match join_type {
                    crate::ingnes_df::JoinType::Inner => (kl + kr) * 0.5,
                    crate::ingnes_df::JoinType::LeftSemi | crate::ingnes_df::JoinType::LeftAnti => (kl + kr) * 0.5 + 0.3,
                    _ => (kl + kr) * 0.5 - 0.4,
                }
            }
            LogicalStage::Union { left, right, .. } => {
                let kl = Self::plan_ricci_curvature(left);
                let kr = Self::plan_ricci_curvature(right);
                ((kl + kr) * 0.5) - 0.3
            }
            LogicalStage::Range { .. } => 0.0,
        }
    }

    /// Otimiza uma árvore de `LogicalStage` executando passagens de compilador até ponto fixo.
    pub fn optimize(&self, plan: &LogicalStage) -> (LogicalStage, OptimizationStats) {
        let mut current_plan = plan.clone();
        let mut total_stats = OptimizationStats::default();

        // Pass 0 (opcional): canonicalização por simetria, adotada apenas nos `Select` em que
        // expõe subexpressões compartilhadas lucrativas para o CSE. Roda uma única vez antes do
        // ponto fixo: é idempotente e contar subexpressões a cada iteração estourava o teto de
        // tempo de otimização (medido pelo `algebra_gate`).
        if self.algebraic {
            current_plan = crate::catalyst::algebra::canonicalize_stage_targeted(&current_plan);
        }

        for pass in 0..self.max_iterations {
            let mut pass_stats = OptimizationStats::default();

            // Pass 1: Otimização de Expressões em todos os nós (Constant Folding & Strength Reduction)
            current_plan = self.optimize_stage_expressions(&current_plan, &mut pass_stats);

            // Pass 2: Predicate Pushdown (Empurra Filter abaixo de WithColumn / Projections)
            current_plan = self.push_down_predicates(current_plan, &mut pass_stats);

            // Pass 2.5 (opcional): Interpretação Abstrata & Poda de Galois de Intervalos (Absdom)
            if self.algebraic {
                let mut env = crate::catalyst::algebra::AbstractEnv::new();
                let (simplified, changed) = crate::catalyst::algebra::simplify_stage_with_absdom(&current_plan, &mut env);
                if changed {
                    pass_stats.constants_folded += 1;
                    current_plan = simplified;
                }
            }

            // Pass 3: Common Subexpression Elimination (CSE)
            current_plan = self.eliminate_common_subexpressions(current_plan, &mut pass_stats);

            // Pass 4: Dead Column Elimination (DCE)
            current_plan = self.eliminate_dead_columns(&current_plan, None, &mut pass_stats);

            total_stats.constants_folded += pass_stats.constants_folded;
            total_stats.strength_reductions += pass_stats.strength_reductions;
            total_stats.predicates_pushed += pass_stats.predicates_pushed;
            total_stats.common_subexpressions += pass_stats.common_subexpressions;
            total_stats.dead_columns_pruned += pass_stats.dead_columns_pruned;
            total_stats.passes_run = pass + 1;

            if pass_stats.total_transformations() == 0 {
                break; // Atingiu o ponto fixo (fixpoint)
            }
        }

        (current_plan, total_stats)
    }

    /// Otimiza as expressões contidas nos estágios.
    fn optimize_stage_expressions(
        &self,
        stage: &LogicalStage,
        stats: &mut OptimizationStats,
    ) -> LogicalStage {
        match stage {
            LogicalStage::Join {
                left,
                right,
                left_key,
                right_key,
                join_type,
            } => {
                let opt_left = self.optimize_stage_expressions(left, stats);
                let opt_right = self.optimize_stage_expressions(right, stats);
                LogicalStage::Join {
                    left: Box::new(opt_left),
                    right: Box::new(opt_right),
                    left_key: left_key.clone(),
                    right_key: right_key.clone(),
                    join_type: *join_type,
                }
            }
            LogicalStage::Union {
                left,
                right,
                distinct,
            } => {
                let opt_left = self.optimize_stage_expressions(left, stats);
                let opt_right = self.optimize_stage_expressions(right, stats);
                LogicalStage::Union {
                    left: Box::new(opt_left),
                    right: Box::new(opt_right),
                    distinct: *distinct,
                }
            }
            LogicalStage::Range {
                start,
                end,
                step,
                num_partitions,
            } => LogicalStage::Range {
                start: *start,
                end: *end,
                step: *step,
                num_partitions: *num_partitions,
            },
            LogicalStage::Select { input, projections } => {
                let opt_input = self.optimize_stage_expressions(input, stats);
                let opt_proj = projections
                    .iter()
                    .map(|(name, expr)| (name.clone(), self.optimize_expr(expr, stats)))
                    .collect();
                LogicalStage::Select {
                    input: Box::new(opt_input),
                    projections: opt_proj,
                }
            }
            LogicalStage::WithColumn {
                input,
                column_name,
                expression,
            } => {
                let opt_input = self.optimize_stage_expressions(input, stats);
                let opt_expr = self.optimize_expr(expression, stats);
                LogicalStage::WithColumn {
                    input: Box::new(opt_input),
                    column_name: column_name.clone(),
                    expression: opt_expr,
                }
            }
            LogicalStage::Filter { input, condition } => {
                let opt_input = self.optimize_stage_expressions(input, stats);
                let opt_cond = self.optimize_expr(condition, stats);
                LogicalStage::Filter {
                    input: Box::new(opt_input),
                    condition: opt_cond,
                }
            }
        }
    }

    /// Otimiza recursivamente uma única expressão aplicando Constant Folding e Strength Reduction.
    pub fn optimize_expr(&self, expr: &Expression, stats: &mut OptimizationStats) -> Expression {
        let folded = self.fold_constants(expr, stats);
        self.reduce_strength(&folded, stats)
    }

    /// Passagem 1: Constant Folding & Identidades Algébricas.
    pub fn fold_constants(&self, expr: &Expression, stats: &mut OptimizationStats) -> Expression {
        match expr {
            Expression::Literal(_) | Expression::Column(_) => expr.clone(),

            // Negação Booleana
            Expression::Not(inner) => {
                let opt_inner = self.fold_constants(inner, stats);
                match opt_inner {
                    Expression::Literal(ScalarValue::Boolean(b)) => {
                        stats.constants_folded += 1;
                        Expression::Literal(ScalarValue::Boolean(!b))
                    }
                    Expression::Not(double_inner) => {
                        stats.constants_folded += 1;
                        *double_inner
                    }
                    other => Expression::Not(Box::new(other)),
                }
            }

            // Arredondamento
            Expression::Round(inner, decimals) => {
                let opt_inner = self.fold_constants(inner, stats);
                if let Expression::Literal(ScalarValue::Double(v)) = opt_inner {
                    stats.constants_folded += 1;
                    let factor = 10_f64.powi(*decimals as i32);
                    let inv = 1.0 / factor;
                    let rounded = (v * factor).round() * inv;
                    Expression::Literal(ScalarValue::Double(rounded))
                } else {
                    Expression::Round(Box::new(opt_inner), *decimals)
                }
            }

            // Cast
            Expression::Cast { expr: inner, target_type } => {
                let opt_inner = self.fold_constants(inner, stats);
                if let Expression::Literal(lit) = &opt_inner {
                    if let Some(casted) = fold_cast_literal(lit, *target_type) {
                        stats.constants_folded += 1;
                        return Expression::Literal(casted);
                    }
                }
                Expression::Cast {
                    expr: Box::new(opt_inner),
                    target_type: *target_type,
                }
            }

            // Operador Condicional When/Otherwise
            Expression::WhenOtherwise {
                condition,
                if_true,
                if_false,
            } => {
                let opt_cond = self.fold_constants(condition, stats);
                let opt_t = self.fold_constants(if_true, stats);
                let opt_f = self.fold_constants(if_false, stats);

                if let Expression::Literal(ScalarValue::Boolean(cond_bool)) = opt_cond {
                    stats.constants_folded += 1;
                    if cond_bool {
                        return opt_t;
                    } else {
                        return opt_f;
                    }
                }

                if opt_t == opt_f {
                    stats.constants_folded += 1;
                    return opt_t;
                }

                Expression::WhenOtherwise {
                    condition: Box::new(opt_cond),
                    if_true: Box::new(opt_t),
                    if_false: Box::new(opt_f),
                }
            }

            // Adição
            Expression::Add(l, r) => {
                let opt_l = self.fold_constants(l, stats);
                let opt_r = self.fold_constants(r, stats);

                // Dobra literais constantes
                if let (Expression::Literal(lv), Expression::Literal(rv)) = (&opt_l, &opt_r) {
                    if let Some(res) = fold_binary_add(lv, rv) {
                        stats.constants_folded += 1;
                        return Expression::Literal(res);
                    }
                }

                // Identidades algébricas: x + 0 => x, 0 + x => x
                if let Expression::Literal(rv) = &opt_r {
                    if rv.is_zero() {
                        stats.constants_folded += 1;
                        return opt_l;
                    }
                }
                if let Expression::Literal(lv) = &opt_l {
                    if lv.is_zero() {
                        stats.constants_folded += 1;
                        return opt_r;
                    }
                }

                Expression::Add(Box::new(opt_l), Box::new(opt_r))
            }

            // Subtração
            Expression::Sub(l, r) => {
                let opt_l = self.fold_constants(l, stats);
                let opt_r = self.fold_constants(r, stats);

                if let (Expression::Literal(lv), Expression::Literal(rv)) = (&opt_l, &opt_r) {
                    if let Some(res) = fold_binary_sub(lv, rv) {
                        stats.constants_folded += 1;
                        return Expression::Literal(res);
                    }
                }

                // Identidades: x - 0 => x
                if let Expression::Literal(rv) = &opt_r {
                    if rv.is_zero() {
                        stats.constants_folded += 1;
                        return opt_l;
                    }
                }

                // x - x => 0
                if opt_l == opt_r {
                    stats.constants_folded += 1;
                    return Expression::Literal(ScalarValue::Long(0));
                }

                Expression::Sub(Box::new(opt_l), Box::new(opt_r))
            }

            // Multiplicação
            Expression::Mul(l, r) => {
                let opt_l = self.fold_constants(l, stats);
                let opt_r = self.fold_constants(r, stats);

                if let (Expression::Literal(lv), Expression::Literal(rv)) = (&opt_l, &opt_r) {
                    if let Some(res) = fold_binary_mul(lv, rv) {
                        stats.constants_folded += 1;
                        return Expression::Literal(res);
                    }
                }

                // Identidades: x * 1 => x, 1 * x => x
                if let Expression::Literal(rv) = &opt_r {
                    if rv.is_one() {
                        stats.constants_folded += 1;
                        return opt_l;
                    }
                    if rv.is_zero() {
                        stats.constants_folded += 1;
                        return Expression::Literal(ScalarValue::Long(0));
                    }
                }
                if let Expression::Literal(lv) = &opt_l {
                    if lv.is_one() {
                        stats.constants_folded += 1;
                        return opt_r;
                    }
                    if lv.is_zero() {
                        stats.constants_folded += 1;
                        return Expression::Literal(ScalarValue::Long(0));
                    }
                }

                Expression::Mul(Box::new(opt_l), Box::new(opt_r))
            }

            // Divisão
            Expression::Div(l, r) => {
                let opt_l = self.fold_constants(l, stats);
                let opt_r = self.fold_constants(r, stats);

                if let (Expression::Literal(lv), Expression::Literal(rv)) = (&opt_l, &opt_r) {
                    if !rv.is_zero() {
                        if let Some(res) = fold_binary_div(lv, rv) {
                            stats.constants_folded += 1;
                            return Expression::Literal(res);
                        }
                    }
                }

                // Identidade: x / 1 => x
                if let Expression::Literal(rv) = &opt_r {
                    if rv.is_one() {
                        stats.constants_folded += 1;
                        return opt_l;
                    }
                }

                Expression::Div(Box::new(opt_l), Box::new(opt_r))
            }

            // Módulo
            Expression::Mod(l, r) => {
                let opt_l = self.fold_constants(l, stats);
                let opt_r = self.fold_constants(r, stats);

                if let (Expression::Literal(lv), Expression::Literal(rv)) = (&opt_l, &opt_r) {
                    if !rv.is_zero() {
                        if let Some(res) = fold_binary_mod(lv, rv) {
                            stats.constants_folded += 1;
                            return Expression::Literal(res);
                        }
                    }
                }

                // x % 1 => 0
                if let Expression::Literal(rv) = &opt_r {
                    if rv.is_one() {
                        stats.constants_folded += 1;
                        return Expression::Literal(ScalarValue::Long(0));
                    }
                }

                Expression::Mod(Box::new(opt_l), Box::new(opt_r))
            }

            // Bitwise AND
            Expression::BitAnd(l, r) => {
                let opt_l = self.fold_constants(l, stats);
                let opt_r = self.fold_constants(r, stats);

                if let (Expression::Literal(lv), Expression::Literal(rv)) = (&opt_l, &opt_r) {
                    if let (Some(li), Some(ri)) = (lv.as_i64(), rv.as_i64()) {
                        stats.constants_folded += 1;
                        return Expression::Literal(ScalarValue::Long(li & ri));
                    }
                }

                // x & 0 => 0, 0 & x => 0
                if let Expression::Literal(rv) = &opt_r {
                    if rv.is_zero() {
                        stats.constants_folded += 1;
                        return Expression::Literal(ScalarValue::Long(0));
                    }
                }
                if let Expression::Literal(lv) = &opt_l {
                    if lv.is_zero() {
                        stats.constants_folded += 1;
                        return Expression::Literal(ScalarValue::Long(0));
                    }
                }

                // x & x => x
                if opt_l == opt_r {
                    stats.constants_folded += 1;
                    return opt_l;
                }

                Expression::BitAnd(Box::new(opt_l), Box::new(opt_r))
            }

            // Bitwise OR
            Expression::BitOr(l, r) => {
                let opt_l = self.fold_constants(l, stats);
                let opt_r = self.fold_constants(r, stats);

                if let (Expression::Literal(lv), Expression::Literal(rv)) = (&opt_l, &opt_r) {
                    if let (Some(li), Some(ri)) = (lv.as_i64(), rv.as_i64()) {
                        stats.constants_folded += 1;
                        return Expression::Literal(ScalarValue::Long(li | ri));
                    }
                }

                // x | 0 => x, 0 | x => x
                if let Expression::Literal(rv) = &opt_r {
                    if rv.is_zero() {
                        stats.constants_folded += 1;
                        return opt_l;
                    }
                }
                if let Expression::Literal(lv) = &opt_l {
                    if lv.is_zero() {
                        stats.constants_folded += 1;
                        return opt_r;
                    }
                }

                // x | x => x
                if opt_l == opt_r {
                    stats.constants_folded += 1;
                    return opt_l;
                }

                Expression::BitOr(Box::new(opt_l), Box::new(opt_r))
            }

            // Bitwise XOR
            Expression::BitXor(l, r) => {
                let opt_l = self.fold_constants(l, stats);
                let opt_r = self.fold_constants(r, stats);

                if let (Expression::Literal(lv), Expression::Literal(rv)) = (&opt_l, &opt_r) {
                    if let (Some(li), Some(ri)) = (lv.as_i64(), rv.as_i64()) {
                        stats.constants_folded += 1;
                        return Expression::Literal(ScalarValue::Long(li ^ ri));
                    }
                }

                // x ^ 0 => x, 0 ^ x => x
                if let Expression::Literal(rv) = &opt_r {
                    if rv.is_zero() {
                        stats.constants_folded += 1;
                        return opt_l;
                    }
                }
                if let Expression::Literal(lv) = &opt_l {
                    if lv.is_zero() {
                        stats.constants_folded += 1;
                        return opt_r;
                    }
                }

                // x ^ x => 0
                if opt_l == opt_r {
                    stats.constants_folded += 1;
                    return Expression::Literal(ScalarValue::Long(0));
                }

                Expression::BitXor(Box::new(opt_l), Box::new(opt_r))
            }

            // Operadores Lógicos Booleanos
            Expression::And(l, r) => {
                let opt_l = self.fold_constants(l, stats);
                let opt_r = self.fold_constants(r, stats);

                if let (Expression::Literal(lv), Expression::Literal(rv)) = (&opt_l, &opt_r) {
                    if let (Some(lb), Some(rb)) = (lv.as_bool(), rv.as_bool()) {
                        stats.constants_folded += 1;
                        return Expression::Literal(ScalarValue::Boolean(lb && rb));
                    }
                }

                // Identidades booleanas: x && false => false; x && true => x
                if let Expression::Literal(ScalarValue::Boolean(false)) = opt_r {
                    stats.constants_folded += 1;
                    return Expression::Literal(ScalarValue::Boolean(false));
                }
                if let Expression::Literal(ScalarValue::Boolean(false)) = opt_l {
                    stats.constants_folded += 1;
                    return Expression::Literal(ScalarValue::Boolean(false));
                }
                if let Expression::Literal(ScalarValue::Boolean(true)) = opt_r {
                    stats.constants_folded += 1;
                    return opt_l;
                }
                if let Expression::Literal(ScalarValue::Boolean(true)) = opt_l {
                    stats.constants_folded += 1;
                    return opt_r;
                }

                Expression::And(Box::new(opt_l), Box::new(opt_r))
            }

            Expression::Or(l, r) => {
                let opt_l = self.fold_constants(l, stats);
                let opt_r = self.fold_constants(r, stats);

                if let (Expression::Literal(lv), Expression::Literal(rv)) = (&opt_l, &opt_r) {
                    if let (Some(lb), Some(rb)) = (lv.as_bool(), rv.as_bool()) {
                        stats.constants_folded += 1;
                        return Expression::Literal(ScalarValue::Boolean(lb || rb));
                    }
                }

                // Identidades: x || true => true; x || false => x
                if let Expression::Literal(ScalarValue::Boolean(true)) = opt_r {
                    stats.constants_folded += 1;
                    return Expression::Literal(ScalarValue::Boolean(true));
                }
                if let Expression::Literal(ScalarValue::Boolean(true)) = opt_l {
                    stats.constants_folded += 1;
                    return Expression::Literal(ScalarValue::Boolean(true));
                }
                if let Expression::Literal(ScalarValue::Boolean(false)) = opt_r {
                    stats.constants_folded += 1;
                    return opt_l;
                }
                if let Expression::Literal(ScalarValue::Boolean(false)) = opt_l {
                    stats.constants_folded += 1;
                    return opt_r;
                }

                Expression::Or(Box::new(opt_l), Box::new(opt_r))
            }

            // Operadores Relacionais (Eq, Neq, Lt, Lte, Gt, Gte)
            Expression::Eq(l, r) => {
                let opt_l = self.fold_constants(l, stats);
                let opt_r = self.fold_constants(r, stats);

                if let (Expression::Literal(lv), Expression::Literal(rv)) = (&opt_l, &opt_r) {
                    stats.constants_folded += 1;
                    return Expression::Literal(ScalarValue::Boolean(lv == rv));
                }

                if opt_l == opt_r {
                    stats.constants_folded += 1;
                    return Expression::Literal(ScalarValue::Boolean(true));
                }

                Expression::Eq(Box::new(opt_l), Box::new(opt_r))
            }

            Expression::Neq(l, r) => {
                let opt_l = self.fold_constants(l, stats);
                let opt_r = self.fold_constants(r, stats);

                if let (Expression::Literal(lv), Expression::Literal(rv)) = (&opt_l, &opt_r) {
                    stats.constants_folded += 1;
                    return Expression::Literal(ScalarValue::Boolean(lv != rv));
                }

                if opt_l == opt_r {
                    stats.constants_folded += 1;
                    return Expression::Literal(ScalarValue::Boolean(false));
                }

                Expression::Neq(Box::new(opt_l), Box::new(opt_r))
            }

            Expression::Lt(l, r) => {
                let opt_l = self.fold_constants(l, stats);
                let opt_r = self.fold_constants(r, stats);

                if let (Expression::Literal(lv), Expression::Literal(rv)) = (&opt_l, &opt_r) {
                    if let (Some(la), Some(ra)) = (lv.as_f64(), rv.as_f64()) {
                        stats.constants_folded += 1;
                        return Expression::Literal(ScalarValue::Boolean(la < ra));
                    }
                }

                if opt_l == opt_r {
                    stats.constants_folded += 1;
                    return Expression::Literal(ScalarValue::Boolean(false));
                }

                Expression::Lt(Box::new(opt_l), Box::new(opt_r))
            }

            Expression::Lte(l, r) => {
                let opt_l = self.fold_constants(l, stats);
                let opt_r = self.fold_constants(r, stats);

                if let (Expression::Literal(lv), Expression::Literal(rv)) = (&opt_l, &opt_r) {
                    if let (Some(la), Some(ra)) = (lv.as_f64(), rv.as_f64()) {
                        stats.constants_folded += 1;
                        return Expression::Literal(ScalarValue::Boolean(la <= ra));
                    }
                }

                if opt_l == opt_r {
                    stats.constants_folded += 1;
                    return Expression::Literal(ScalarValue::Boolean(true));
                }

                Expression::Lte(Box::new(opt_l), Box::new(opt_r))
            }

            Expression::Gt(l, r) => {
                let opt_l = self.fold_constants(l, stats);
                let opt_r = self.fold_constants(r, stats);

                if let (Expression::Literal(lv), Expression::Literal(rv)) = (&opt_l, &opt_r) {
                    if let (Some(la), Some(ra)) = (lv.as_f64(), rv.as_f64()) {
                        stats.constants_folded += 1;
                        return Expression::Literal(ScalarValue::Boolean(la > ra));
                    }
                }

                if opt_l == opt_r {
                    stats.constants_folded += 1;
                    return Expression::Literal(ScalarValue::Boolean(false));
                }

                Expression::Gt(Box::new(opt_l), Box::new(opt_r))
            }

            Expression::Gte(l, r) => {
                let opt_l = self.fold_constants(l, stats);
                let opt_r = self.fold_constants(r, stats);

                if let (Expression::Literal(lv), Expression::Literal(rv)) = (&opt_l, &opt_r) {
                    if let (Some(la), Some(ra)) = (lv.as_f64(), rv.as_f64()) {
                        stats.constants_folded += 1;
                        return Expression::Literal(ScalarValue::Boolean(la >= ra));
                    }
                }

                if opt_l == opt_r {
                    stats.constants_folded += 1;
                    return Expression::Literal(ScalarValue::Boolean(true));
                }

                Expression::Gte(Box::new(opt_l), Box::new(opt_r))
            }

            Expression::BitShiftLeft(l, r) => Expression::BitShiftLeft(
                Box::new(self.fold_constants(l, stats)),
                Box::new(self.fold_constants(r, stats)),
            ),
            Expression::BitShiftRight(l, r) => Expression::BitShiftRight(
                Box::new(self.fold_constants(l, stats)),
                Box::new(self.fold_constants(r, stats)),
            ),

            Expression::QuimeraFactor {
                start_day,
                end_day,
                indexer_code,
                anniv_day,
                pct_indexer,
                spread_aa,
                broadcast_engine,
            } => Expression::QuimeraFactor {
                start_day: Box::new(self.fold_constants(start_day, stats)),
                end_day: Box::new(self.fold_constants(end_day, stats)),
                indexer_code: Box::new(self.fold_constants(indexer_code, stats)),
                anniv_day: Box::new(self.fold_constants(anniv_day, stats)),
                pct_indexer: Box::new(self.fold_constants(pct_indexer, stats)),
                spread_aa: Box::new(self.fold_constants(spread_aa, stats)),
                broadcast_engine: broadcast_engine.clone(),
            },
        }
    }

    /// Passagem 2: Redução de Força (Strength Reduction) de operações de silício.
    ///
    /// Substitui `div` e `mod` (25-40 ciclos x86) por `and` e `shr` (1 ciclo x86).
    pub fn reduce_strength(&self, expr: &Expression, stats: &mut OptimizationStats) -> Expression {
        match expr {
            // Módulo por potência de 2: x % 2^k => x & (2^k - 1)
            Expression::Mod(l, r) => {
                let opt_l = self.reduce_strength(l, stats);
                let opt_r = self.reduce_strength(r, stats);

                if let Expression::Literal(lit) = &opt_r {
                    if let Some(k) = lit.is_power_of_two() {
                        let mask = (1_i64 << k) - 1;
                        stats.strength_reductions += 1;
                        return Expression::BitAnd(
                            Box::new(opt_l),
                            Box::new(Expression::Literal(ScalarValue::Long(mask))),
                        );
                    }
                }
                Expression::Mod(Box::new(opt_l), Box::new(opt_r))
            }

            // Multiplicação por potência de 2: x * 2^k => x << k
            Expression::Mul(l, r) => {
                let opt_l = self.reduce_strength(l, stats);
                let opt_r = self.reduce_strength(r, stats);

                if let Expression::Literal(lit) = &opt_r {
                    if let Some(k) = lit.is_power_of_two() {
                        stats.strength_reductions += 1;
                        return Expression::BitShiftLeft(
                            Box::new(opt_l),
                            Box::new(Expression::Literal(ScalarValue::Long(k as i64))),
                        );
                    }
                }
                if let Expression::Literal(lit) = &opt_l {
                    if let Some(k) = lit.is_power_of_two() {
                        stats.strength_reductions += 1;
                        return Expression::BitShiftLeft(
                            Box::new(opt_r),
                            Box::new(Expression::Literal(ScalarValue::Long(k as i64))),
                        );
                    }
                }

                Expression::Mul(Box::new(opt_l), Box::new(opt_r))
            }

            // Divisão por potência de 2: x / 2^k => x >> k
            Expression::Div(l, r) => {
                let opt_l = self.reduce_strength(l, stats);
                let opt_r = self.reduce_strength(r, stats);

                if let Expression::Literal(lit) = &opt_r {
                    if let Some(k) = lit.is_power_of_two() {
                        stats.strength_reductions += 1;
                        return Expression::BitShiftRight(
                            Box::new(opt_l),
                            Box::new(Expression::Literal(ScalarValue::Long(k as i64))),
                        );
                    }
                }

                Expression::Div(Box::new(opt_l), Box::new(opt_r))
            }

            Expression::Add(l, r) => Expression::Add(
                Box::new(self.reduce_strength(l, stats)),
                Box::new(self.reduce_strength(r, stats)),
            ),
            Expression::Sub(l, r) => Expression::Sub(
                Box::new(self.reduce_strength(l, stats)),
                Box::new(self.reduce_strength(r, stats)),
            ),
            Expression::BitAnd(l, r) => Expression::BitAnd(
                Box::new(self.reduce_strength(l, stats)),
                Box::new(self.reduce_strength(r, stats)),
            ),
            Expression::BitOr(l, r) => Expression::BitOr(
                Box::new(self.reduce_strength(l, stats)),
                Box::new(self.reduce_strength(r, stats)),
            ),
            Expression::BitXor(l, r) => Expression::BitXor(
                Box::new(self.reduce_strength(l, stats)),
                Box::new(self.reduce_strength(r, stats)),
            ),
            Expression::BitShiftLeft(l, r) => Expression::BitShiftLeft(
                Box::new(self.reduce_strength(l, stats)),
                Box::new(self.reduce_strength(r, stats)),
            ),
            Expression::BitShiftRight(l, r) => Expression::BitShiftRight(
                Box::new(self.reduce_strength(l, stats)),
                Box::new(self.reduce_strength(r, stats)),
            ),
            Expression::And(l, r) => Expression::And(
                Box::new(self.reduce_strength(l, stats)),
                Box::new(self.reduce_strength(r, stats)),
            ),
            Expression::Or(l, r) => Expression::Or(
                Box::new(self.reduce_strength(l, stats)),
                Box::new(self.reduce_strength(r, stats)),
            ),
            Expression::Eq(l, r) => Expression::Eq(
                Box::new(self.reduce_strength(l, stats)),
                Box::new(self.reduce_strength(r, stats)),
            ),
            Expression::Neq(l, r) => Expression::Neq(
                Box::new(self.reduce_strength(l, stats)),
                Box::new(self.reduce_strength(r, stats)),
            ),
            Expression::Lt(l, r) => Expression::Lt(
                Box::new(self.reduce_strength(l, stats)),
                Box::new(self.reduce_strength(r, stats)),
            ),
            Expression::Lte(l, r) => Expression::Lte(
                Box::new(self.reduce_strength(l, stats)),
                Box::new(self.reduce_strength(r, stats)),
            ),
            Expression::Gt(l, r) => Expression::Gt(
                Box::new(self.reduce_strength(l, stats)),
                Box::new(self.reduce_strength(r, stats)),
            ),
            Expression::Gte(l, r) => Expression::Gte(
                Box::new(self.reduce_strength(l, stats)),
                Box::new(self.reduce_strength(r, stats)),
            ),
            Expression::Not(e) => Expression::Not(Box::new(self.reduce_strength(e, stats))),
            Expression::Round(e, d) => {
                Expression::Round(Box::new(self.reduce_strength(e, stats)), *d)
            }
            Expression::WhenOtherwise {
                condition,
                if_true,
                if_false,
            } => Expression::WhenOtherwise {
                condition: Box::new(self.reduce_strength(condition, stats)),
                if_true: Box::new(self.reduce_strength(if_true, stats)),
                if_false: Box::new(self.reduce_strength(if_false, stats)),
            },
            Expression::Cast { expr, target_type } => Expression::Cast {
                expr: Box::new(self.reduce_strength(expr, stats)),
                target_type: *target_type,
            },
            Expression::QuimeraFactor {
                start_day,
                end_day,
                indexer_code,
                anniv_day,
                pct_indexer,
                spread_aa,
                broadcast_engine,
            } => Expression::QuimeraFactor {
                start_day: Box::new(self.reduce_strength(start_day, stats)),
                end_day: Box::new(self.reduce_strength(end_day, stats)),
                indexer_code: Box::new(self.reduce_strength(indexer_code, stats)),
                anniv_day: Box::new(self.reduce_strength(anniv_day, stats)),
                pct_indexer: Box::new(self.reduce_strength(pct_indexer, stats)),
                spread_aa: Box::new(self.reduce_strength(spread_aa, stats)),
                broadcast_engine: broadcast_engine.clone(),
            },
            Expression::Literal(_) | Expression::Column(_) => expr.clone(),
        }
    }

    /// Passagem 3: Predicate Pushdown (Empurra nós de filtro para antes de projeções/WithColumn).
    pub fn push_down_predicates(
        &self,
        stage: LogicalStage,
        stats: &mut OptimizationStats,
    ) -> LogicalStage {
        match stage {
            LogicalStage::Filter { input, condition } => {
                let inner = *input;
                match inner {
                    // Filter em cima de WithColumn: se condition NÃO depende de column_name, empurra!
                    LogicalStage::WithColumn {
                        input: grand_input,
                        column_name,
                        expression,
                    } => {
                        let mut cond_cols = HashSet::new();
                        condition.collect_columns(&mut cond_cols);

                        if !cond_cols.contains(&column_name) {
                            stats.predicates_pushed += 1;
                            // Empurra Filter para baixo do WithColumn
                            let pushed_filter = LogicalStage::Filter {
                                input: grand_input,
                                condition,
                            };
                            let recursive_pushed =
                                self.push_down_predicates(pushed_filter, stats);
                            LogicalStage::WithColumn {
                                input: Box::new(recursive_pushed),
                                column_name,
                                expression,
                            }
                        } else {
                            LogicalStage::Filter {
                                input: Box::new(LogicalStage::WithColumn {
                                    input: grand_input,
                                    column_name,
                                    expression,
                                }),
                                condition,
                            }
                        }
                    }
                    LogicalStage::Union { left, right, distinct } => {
                        stats.predicates_pushed += 1;
                        let left_pushed = self.push_down_predicates(
                            LogicalStage::Filter {
                                input: left,
                                condition: condition.clone(),
                            },
                            stats,
                        );
                        let right_pushed = self.push_down_predicates(
                            LogicalStage::Filter {
                                input: right,
                                condition,
                            },
                            stats,
                        );
                        LogicalStage::Union {
                            left: Box::new(left_pushed),
                            right: Box::new(right_pushed),
                            distinct,
                        }
                    }
                    other => LogicalStage::Filter {
                        input: Box::new(self.push_down_predicates(other, stats)),
                        condition,
                    },
                }
            }
            LogicalStage::Join {
                left,
                right,
                left_key,
                right_key,
                join_type,
            } => LogicalStage::Join {
                left: Box::new(self.push_down_predicates(*left, stats)),
                right: Box::new(self.push_down_predicates(*right, stats)),
                left_key,
                right_key,
                join_type,
            },
            LogicalStage::Union {
                left,
                right,
                distinct,
            } => LogicalStage::Union {
                left: Box::new(self.push_down_predicates(*left, stats)),
                right: Box::new(self.push_down_predicates(*right, stats)),
                distinct,
            },
            LogicalStage::WithColumn {
                input,
                column_name,
                expression,
            } => LogicalStage::WithColumn {
                input: Box::new(self.push_down_predicates(*input, stats)),
                column_name,
                expression,
            },
            LogicalStage::Select { input, projections } => LogicalStage::Select {
                input: Box::new(self.push_down_predicates(*input, stats)),
                projections,
            },
            LogicalStage::Range { .. } => stage,
        }
    }

    /// Passagem 4: Common Subexpression Elimination (CSE)
    pub fn eliminate_common_subexpressions(
        &self,
        stage: LogicalStage,
        stats: &mut OptimizationStats,
    ) -> LogicalStage {
        match stage {
            LogicalStage::Select { input, projections } => {
                let mut freq_map: HashMap<&Expression, usize> = HashMap::new();
                for (_, expr) in &projections {
                    count_subexpressions(expr, &mut freq_map);
                }

                // Identifica subexpressões candidatas a CSE (profundidade >= 2 e contagem >= 2)
                let mut candidates: Vec<Expression> = freq_map
                    .into_iter()
                    .filter(|(expr, count)| {
                        *count >= 2
                            && expr.depth() >= 2
                            // Com a flag algébrica, só materializa se recalcular custar mais
                            // do que gravar e reler a coluna (ver `cse_is_profitable`).
                            && (!self.algebraic || cse_is_profitable(expr, *count))
                    })
                    .map(|(expr, _)| expr.clone())
                    .collect();

                // Ordena por tamanho decrescente para substituir sub-árvores maiores primeiro
                candidates.sort_by(|a, b| b.size().cmp(&a.size()));

                if candidates.is_empty() {
                    return LogicalStage::Select {
                        input: Box::new(self.eliminate_common_subexpressions(*input, stats)),
                        projections,
                    };
                }

                let mut current_input = self.eliminate_common_subexpressions(*input, stats);
                let mut updated_projections = projections;

                for (idx, candidate) in candidates.into_iter().enumerate() {
                    let synth_name = format!("_cse_reg_{}", idx);
                    let synth_col = Expression::Column(synth_name.clone());

                    // Insere um WithColumn com o valor computado uma única vez
                    current_input = LogicalStage::WithColumn {
                        input: Box::new(current_input),
                        column_name: synth_name,
                        expression: candidate.clone(),
                    };

                    // Substitui nas projeções subsequentes
                    for (_, expr) in &mut updated_projections {
                        *expr = expr.replace_expression(&candidate, &synth_col);
                    }
                    stats.common_subexpressions += 1;
                }

                LogicalStage::Select {
                    input: Box::new(current_input),
                    projections: updated_projections,
                }
            }
            LogicalStage::WithColumn {
                input,
                column_name,
                expression,
            } => LogicalStage::WithColumn {
                input: Box::new(self.eliminate_common_subexpressions(*input, stats)),
                column_name,
                expression,
            },
            LogicalStage::Filter { input, condition } => LogicalStage::Filter {
                input: Box::new(self.eliminate_common_subexpressions(*input, stats)),
                condition,
            },
            LogicalStage::Join {
                left,
                right,
                left_key,
                right_key,
                join_type,
            } => LogicalStage::Join {
                left: Box::new(self.eliminate_common_subexpressions(*left, stats)),
                right: Box::new(self.eliminate_common_subexpressions(*right, stats)),
                left_key,
                right_key,
                join_type,
            },
            LogicalStage::Union {
                left,
                right,
                distinct,
            } => LogicalStage::Union {
                left: Box::new(self.eliminate_common_subexpressions(*left, stats)),
                right: Box::new(self.eliminate_common_subexpressions(*right, stats)),
                distinct,
            },
            LogicalStage::Range { .. } => stage,
        }
    }

    /// Passagem 5: Dead Column Elimination (DCE)
    pub fn eliminate_dead_columns(
        &self,
        stage: &LogicalStage,
        required_cols: Option<&HashSet<String>>,
        stats: &mut OptimizationStats,
    ) -> LogicalStage {
        match stage {
            LogicalStage::Select { input, projections } => {
                let mut required = HashSet::new();
                for (_, expr) in projections {
                    expr.collect_columns(&mut required);
                }
                let opt_input = self.eliminate_dead_columns(input, Some(&required), stats);
                LogicalStage::Select {
                    input: Box::new(opt_input),
                    projections: projections.clone(),
                }
            }
            LogicalStage::WithColumn {
                input,
                column_name,
                expression,
            } => {
                if let Some(req) = required_cols {
                    if !req.contains(column_name) && !column_name.starts_with("_cse_") {
                        // Coluna gerada mas nunca usada downstream! Elimina nó!
                        stats.dead_columns_pruned += 1;
                        return self.eliminate_dead_columns(input, Some(req), stats);
                    }

                    let mut next_req = req.clone();
                    next_req.remove(column_name);
                    expression.collect_columns(&mut next_req);

                    let opt_input = self.eliminate_dead_columns(input, Some(&next_req), stats);
                    LogicalStage::WithColumn {
                        input: Box::new(opt_input),
                        column_name: column_name.clone(),
                        expression: expression.clone(),
                    }
                } else {
                    let opt_input = self.eliminate_dead_columns(input, None, stats);
                    LogicalStage::WithColumn {
                        input: Box::new(opt_input),
                        column_name: column_name.clone(),
                        expression: expression.clone(),
                    }
                }
            }
            LogicalStage::Filter { input, condition } => {
                if let Some(req) = required_cols {
                    let mut next_req = req.clone();
                    condition.collect_columns(&mut next_req);

                    let opt_input = self.eliminate_dead_columns(input, Some(&next_req), stats);
                    LogicalStage::Filter {
                        input: Box::new(opt_input),
                        condition: condition.clone(),
                    }
                } else {
                    let opt_input = self.eliminate_dead_columns(input, None, stats);
                    LogicalStage::Filter {
                        input: Box::new(opt_input),
                        condition: condition.clone(),
                    }
                }
            }
            LogicalStage::Join {
                left,
                right,
                left_key,
                right_key,
                join_type,
            } => {
                let (opt_left, opt_right) = if let Some(req) = required_cols {
                    let mut left_req = req.clone();
                    left_req.insert(left_key.clone());
                    let mut right_req = req.clone();
                    right_req.insert(right_key.clone());
                    (
                        self.eliminate_dead_columns(left.as_ref(), Some(&left_req), stats),
                        self.eliminate_dead_columns(right.as_ref(), Some(&right_req), stats),
                    )
                } else {
                    (
                        self.eliminate_dead_columns(left.as_ref(), None, stats),
                        self.eliminate_dead_columns(right.as_ref(), None, stats),
                    )
                };
                LogicalStage::Join {
                    left: Box::new(opt_left),
                    right: Box::new(opt_right),
                    left_key: left_key.clone(),
                    right_key: right_key.clone(),
                    join_type: *join_type,
                }
            }
            LogicalStage::Union {
                left,
                right,
                distinct,
            } => {
                let opt_left = self.eliminate_dead_columns(left.as_ref(), required_cols, stats);
                let opt_right = self.eliminate_dead_columns(right.as_ref(), required_cols, stats);
                LogicalStage::Union {
                    left: Box::new(opt_left),
                    right: Box::new(opt_right),
                    distinct: *distinct,
                }
            }
            LogicalStage::Range {
                start,
                end,
                step,
                num_partitions,
            } => LogicalStage::Range {
                start: *start,
                end: *end,
                step: *step,
                num_partitions: *num_partitions,
            },
        }
    }
}

/// Conta frequência de subexpressões de profundidade >= 2.
/// Custo relativo (em ciclos aproximados por linha) de materializar uma subexpressão como
/// coluna sintética: uma gravação e uma releitura de coluna inteira.
const CSE_MATERIALIZE_COST: u32 = 4;

/// Custo relativo por linha de avaliar uma expressão (folhas são gratuitas).
fn eval_cost(expr: &Expression) -> u32 {
    use Expression as E;
    match expr {
        E::Literal(_) | E::Column(_) => 0,
        E::Div(l, r) | E::Mod(l, r) => 25 + eval_cost(l) + eval_cost(r),
        E::Round(e, _) => 10 + eval_cost(e),
        E::QuimeraFactor { .. } => 40,
        E::Not(e) | E::Cast { expr: e, .. } => 1 + eval_cost(e),
        E::Add(l, r)
        | E::Sub(l, r)
        | E::Mul(l, r)
        | E::BitAnd(l, r)
        | E::BitOr(l, r)
        | E::BitXor(l, r)
        | E::BitShiftLeft(l, r)
        | E::BitShiftRight(l, r)
        | E::And(l, r)
        | E::Or(l, r)
        | E::Eq(l, r)
        | E::Neq(l, r)
        | E::Lt(l, r)
        | E::Lte(l, r)
        | E::Gt(l, r)
        | E::Gte(l, r) => 1 + eval_cost(l) + eval_cost(r),
        E::WhenOtherwise { condition, if_true, if_false } => {
            2 + eval_cost(condition) + eval_cost(if_true) + eval_cost(if_false)
        }
    }
}

/// O CSE só compensa se as `count - 1` reavaliações evitadas custarem mais que a materialização.
pub(crate) fn cse_is_profitable(expr: &Expression, count: usize) -> bool {
    (count as u32).saturating_sub(1) * eval_cost(expr) > CSE_MATERIALIZE_COST
}

pub(crate) fn count_subexpressions<'a>(expr: &'a Expression, freq: &mut HashMap<&'a Expression, usize>) {
    if expr.depth() >= 2 {
        *freq.entry(expr).or_insert(0) += 1;
    }
    match expr {
        Expression::Add(l, r)
        | Expression::Sub(l, r)
        | Expression::Mul(l, r)
        | Expression::Div(l, r)
        | Expression::Mod(l, r)
        | Expression::BitAnd(l, r)
        | Expression::BitOr(l, r)
        | Expression::BitXor(l, r)
        | Expression::BitShiftLeft(l, r)
        | Expression::BitShiftRight(l, r)
        | Expression::And(l, r)
        | Expression::Or(l, r)
        | Expression::Eq(l, r)
        | Expression::Neq(l, r)
        | Expression::Lt(l, r)
        | Expression::Lte(l, r)
        | Expression::Gt(l, r)
        | Expression::Gte(l, r) => {
            count_subexpressions(l, freq);
            count_subexpressions(r, freq);
        }
        Expression::Not(e) | Expression::Round(e, _) | Expression::Cast { expr: e, .. } => {
            count_subexpressions(e, freq);
        }
        Expression::WhenOtherwise {
            condition,
            if_true,
            if_false,
        } => {
            count_subexpressions(condition, freq);
            count_subexpressions(if_true, freq);
            count_subexpressions(if_false, freq);
        }
        _ => {}
    }
}

// Helpers para dobramento de literais
fn fold_binary_add(l: &ScalarValue, r: &ScalarValue) -> Option<ScalarValue> {
    match (l, r) {
        (ScalarValue::Long(la), ScalarValue::Long(ra)) => Some(ScalarValue::Long(la + ra)),
        (ScalarValue::Int(la), ScalarValue::Int(ra)) => Some(ScalarValue::Int(la + ra)),
        (ScalarValue::Double(la), ScalarValue::Double(ra)) => Some(ScalarValue::Double(la + ra)),
        (ScalarValue::Long(la), ScalarValue::Int(ra)) => {
            Some(ScalarValue::Long(la + (*ra as i64)))
        }
        (ScalarValue::Int(la), ScalarValue::Long(ra)) => {
            Some(ScalarValue::Long((*la as i64) + ra))
        }
        _ => None,
    }
}

fn fold_binary_sub(l: &ScalarValue, r: &ScalarValue) -> Option<ScalarValue> {
    match (l, r) {
        (ScalarValue::Long(la), ScalarValue::Long(ra)) => Some(ScalarValue::Long(la - ra)),
        (ScalarValue::Double(la), ScalarValue::Double(ra)) => Some(ScalarValue::Double(la - ra)),
        _ => None,
    }
}

fn fold_binary_mul(l: &ScalarValue, r: &ScalarValue) -> Option<ScalarValue> {
    match (l, r) {
        (ScalarValue::Long(la), ScalarValue::Long(ra)) => Some(ScalarValue::Long(la * ra)),
        (ScalarValue::Double(la), ScalarValue::Double(ra)) => Some(ScalarValue::Double(la * ra)),
        (ScalarValue::Long(la), ScalarValue::Int(ra)) => {
            Some(ScalarValue::Long(la * (*ra as i64)))
        }
        (ScalarValue::Int(la), ScalarValue::Long(ra)) => {
            Some(ScalarValue::Long((*la as i64) * ra))
        }
        _ => None,
    }
}

fn fold_binary_div(l: &ScalarValue, r: &ScalarValue) -> Option<ScalarValue> {
    match (l, r) {
        (ScalarValue::Double(la), ScalarValue::Double(ra)) => Some(ScalarValue::Double(la / ra)),
        (ScalarValue::Long(la), ScalarValue::Long(ra)) => {
            if *ra != 0 {
                Some(ScalarValue::Long(la / ra))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn fold_binary_mod(l: &ScalarValue, r: &ScalarValue) -> Option<ScalarValue> {
    match (l, r) {
        (ScalarValue::Long(la), ScalarValue::Long(ra)) => {
            if *ra != 0 {
                Some(ScalarValue::Long(la % ra))
            } else {
                None
            }
        }
        (ScalarValue::Long(la), ScalarValue::Int(ra)) => {
            if *ra != 0 {
                Some(ScalarValue::Long(la % (*ra as i64)))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn fold_cast_literal(lit: &ScalarValue, target: DataType) -> Option<ScalarValue> {
    match (lit, target) {
        (ScalarValue::Long(v), DataType::Short) => Some(ScalarValue::Short(*v as i16)),
        (ScalarValue::Long(v), DataType::Byte) => Some(ScalarValue::Byte(*v as i8)),
        (ScalarValue::Long(v), DataType::Int) => Some(ScalarValue::Int(*v as i32)),
        (ScalarValue::Long(v), DataType::Double) => Some(ScalarValue::Double(*v as f64)),
        (ScalarValue::Int(v), DataType::Short) => Some(ScalarValue::Short(*v as i16)),
        (ScalarValue::Int(v), DataType::Byte) => Some(ScalarValue::Byte(*v as i8)),
        (ScalarValue::Int(v), DataType::Long) => Some(ScalarValue::Long(*v as i64)),
        (ScalarValue::Double(v), DataType::Long) => Some(ScalarValue::Long(*v as i64)),
        _ => None,
    }
}
