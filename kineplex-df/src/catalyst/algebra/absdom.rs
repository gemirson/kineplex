//! # Domínio Abstrato de Intervalos e Conexões de Galois (`catalyst::algebra::absdom`)
//!
//! Formaliza a propagação de restrições e simplificação de predicados via **Interpretação Abstrata**
//! (Cousot & Cousot, 1977) sobre o reticulado completo de intervalos $(\mathcal{I}, \sqsubseteq, \sqcap, \sqcup, \bot, \top)$.
//!
//! ## Conexão de Galois $(\wp(\mathbb{Z}), \alpha, \gamma, \mathcal{I})$
//!
//! A relação de adjunção canônica entre o domínio concreto $\wp(\mathbb{Z})$ (partes dos inteiros)
//! e o domínio abstrato de intervalos $\mathcal{I}$ é estabelecida pelo par de funções monótonas:
//!
//! - **Abstração ($\alpha$):** $\alpha(S) = [\min(S), \max(S)]$ se $S \neq \emptyset$, senão $\bot$.
//! - **Concretização ($\gamma$):** $\gamma([lo, hi]) = \{ x \in \mathbb{Z} \mid lo \le x \le hi \}$, $\gamma(\bot) = \emptyset$, $\gamma(\top) = \mathbb{Z}$.
//! - **Propriedade de Adjunção:** $\alpha(S) \sqsubseteq I \iff S \subseteq \gamma(I)$.
//!
//! Todas as transformações aqui implementadas são **sombras superiores sadias** (sound over-approximations):
//! se o domínio abstrato prova que um predicado é sempre verdadeiro ou sempre falso sobre os intervalos
//! de entrada, essa conclusão é exata e invariante para qualquer tupla concreta.

use crate::catalyst::{DataType, Expression, ScalarValue};
use crate::sql::LogicalStage;
use std::collections::HashMap;

/// Elemento do Reticulado de Intervalos $\mathcal{I}$.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Interval {
    /// $\bot$: Estado impossível / conjunto vazio (ex: contradição $x > 10 \land x < 5$).
    Bottom,
    /// $[lo, hi]$: Intervalo fechado com garantia estrita de que $lo \le hi$.
    Bounded(i64, i64),
    /// $\top$: Sem limite conhecido ou overflow de saturação.
    Top,
}

impl Interval {
    pub const fn point(val: i64) -> Self {
        Interval::Bounded(val, val)
    }

    pub fn is_bottom(&self) -> bool {
        matches!(self, Interval::Bottom)
    }

    pub fn is_top(&self) -> bool {
        matches!(self, Interval::Top)
    }

    /// Operador Meet ($\sqcap$): Interseção de restrições (usado em conjunções lógicas e refinamentos).
    pub fn meet(&self, other: &Self) -> Self {
        match (self, other) {
            (Interval::Bottom, _) | (_, Interval::Bottom) => Interval::Bottom,
            (Interval::Top, x) | (x, Interval::Top) => *x,
            (Interval::Bounded(l1, h1), Interval::Bounded(l2, h2)) => {
                let lo = (*l1).max(*l2);
                let hi = (*h1).min(*h2);
                if lo <= hi {
                    Interval::Bounded(lo, hi)
                } else {
                    Interval::Bottom
                }
            }
        }
    }

    /// Operador Join ($\sqcup$): Envoltória convexa mínima (usado em disjunções lógicas e união de fluxos).
    pub fn join(&self, other: &Self) -> Self {
        match (self, other) {
            (Interval::Bottom, x) | (x, Interval::Bottom) => *x,
            (Interval::Top, _) | (_, Interval::Top) => Interval::Top,
            (Interval::Bounded(l1, h1), Interval::Bounded(l2, h2)) => {
                Interval::Bounded((*l1).min(*l2), (*h1).max(*h2))
            }
        }
    }

    /// Adição abstrata com proteção estrita contra overflow de inteiros.
    pub fn add(&self, other: &Self) -> Self {
        match (self, other) {
            (Interval::Bottom, _) | (_, Interval::Bottom) => Interval::Bottom,
            (Interval::Bounded(l1, h1), Interval::Bounded(l2, h2)) => {
                match (l1.checked_add(*l2), h1.checked_add(*h2)) {
                    (Some(lo), Some(hi)) => Interval::Bounded(lo, hi),
                    _ => Interval::Top,
                }
            }
            _ => Interval::Top,
        }
    }

    /// Subtração abstrata $[l_1, h_1] - [l_2, h_2] = [l_1 - h_2, h_1 - l_2]$.
    pub fn sub(&self, other: &Self) -> Self {
        match (self, other) {
            (Interval::Bottom, _) | (_, Interval::Bottom) => Interval::Bottom,
            (Interval::Bounded(l1, h1), Interval::Bounded(l2, h2)) => {
                match (l1.checked_sub(*h2), h1.checked_sub(*l2)) {
                    (Some(lo), Some(hi)) => Interval::Bounded(lo, hi),
                    _ => Interval::Top,
                }
            }
            _ => Interval::Top,
        }
    }

    /// Multiplicação abstrata considerando todos os quatro extremos de sinal.
    pub fn mul(&self, other: &Self) -> Self {
        match (self, other) {
            (Interval::Bottom, _) | (_, Interval::Bottom) => Interval::Bottom,
            (Interval::Bounded(l1, h1), Interval::Bounded(l2, h2)) => {
                let p1 = l1.checked_mul(*l2);
                let p2 = l1.checked_mul(*h2);
                let p3 = h1.checked_mul(*l2);
                let p4 = h1.checked_mul(*h2);
                if let (Some(v1), Some(v2), Some(v3), Some(v4)) = (p1, p2, p3, p4) {
                    let lo = v1.min(v2).min(v3).min(v4);
                    let hi = v1.max(v2).max(v3).max(v4);
                    Interval::Bounded(lo, hi)
                } else {
                    Interval::Top
                }
            }
            _ => Interval::Top,
        }
    }
}

/// Ambiente de análise estática associando nomes de colunas aos seus intervalos abstratos.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AbstractEnv {
    pub ranges: HashMap<String, Interval>,
}

impl AbstractEnv {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, col: &str) -> Interval {
        self.ranges.get(col).copied().unwrap_or(Interval::Top)
    }

    pub fn set(&mut self, col: impl Into<String>, interval: Interval) {
        self.ranges.insert(col.into(), interval);
    }

    /// Restringe o intervalo de uma coluna aplicando Meet ($\sqcap$).
    pub fn refine(&mut self, col: &str, constraint: Interval) {
        let current = self.get(col);
        self.set(col, current.meet(&constraint));
    }
}

/// Avalia o intervalo resultante de uma expressão aritmética inteira.
pub fn eval_interval(expr: &Expression, env: &AbstractEnv) -> Interval {
    use Expression as E;
    match expr {
        E::Literal(ScalarValue::Long(v)) => Interval::point(*v),
        E::Literal(ScalarValue::Int(v)) => Interval::point(*v as i64),
        E::Literal(ScalarValue::Short(v)) => Interval::point(*v as i64),
        E::Literal(ScalarValue::Byte(v)) => Interval::point(*v as i64),
        E::Column(name) => env.get(name),
        E::Add(l, r) => eval_interval(l, env).add(&eval_interval(r, env)),
        E::Sub(l, r) => eval_interval(l, env).sub(&eval_interval(r, env)),
        E::Mul(l, r) => eval_interval(l, env).mul(&eval_interval(r, env)),
        E::Cast { expr, target_type } => match target_type {
            DataType::Long | DataType::Int | DataType::Short | DataType::Byte => eval_interval(expr, env),
            _ => Interval::Top,
        },
        _ => Interval::Top,
    }
}

/// Tenta decidir estaticamente o valor booleano de uma expressão comparativa.
///
/// Retorna `Some(true)` se garantidamente verdadeiro para todos os pontos no ambiente abstrato,
/// `Some(false)` se garantidamente falso (contradição), ou `None` se indecidível.
pub fn try_eval_boolean(expr: &Expression, env: &AbstractEnv) -> Option<bool> {
    use Expression as E;
    match expr {
        E::Literal(ScalarValue::Boolean(b)) => Some(*b),
        E::Not(inner) => try_eval_boolean(inner, env).map(|b| !b),
        E::And(l, r) => match (try_eval_boolean(l, env), try_eval_boolean(r, env)) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        },
        E::Or(l, r) => match (try_eval_boolean(l, env), try_eval_boolean(r, env)) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        },
        E::Lt(l, r) => {
            let il = eval_interval(l, env);
            let ir = eval_interval(r, env);
            match (il, ir) {
                (Interval::Bounded(_, h1), Interval::Bounded(l2, _)) if h1 < l2 => Some(true),
                (Interval::Bounded(l1, _), Interval::Bounded(_, h2)) if l1 >= h2 => Some(false),
                _ => None,
            }
        }
        E::Lte(l, r) => {
            let il = eval_interval(l, env);
            let ir = eval_interval(r, env);
            match (il, ir) {
                (Interval::Bounded(_, h1), Interval::Bounded(l2, _)) if h1 <= l2 => Some(true),
                (Interval::Bounded(l1, _), Interval::Bounded(_, h2)) if l1 > h2 => Some(false),
                _ => None,
            }
        }
        E::Gt(l, r) => try_eval_boolean(&E::Lt(r.clone(), l.clone()), env),
        E::Gte(l, r) => try_eval_boolean(&E::Lte(r.clone(), l.clone()), env),
        E::Eq(l, r) => {
            let il = eval_interval(l, env);
            let ir = eval_interval(r, env);
            match (il, ir) {
                (Interval::Bounded(l1, h1), Interval::Bounded(l2, h2)) => {
                    if l1 == h1 && l2 == h2 && l1 == l2 {
                        Some(true)
                    } else if h1 < l2 || l1 > h2 {
                        Some(false)
                    } else {
                        None
                    }
                }
                _ => None,
            }
        }
        E::Neq(l, r) => try_eval_boolean(&E::Eq(l.clone(), r.clone()), env).map(|b| !b),
        _ => None,
    }
}

/// Simplifica recursivamente uma expressão substituindo subárvores decidíveis pelo domínio abstrato.
pub fn simplify_expr_with_absdom(expr: &Expression, env: &AbstractEnv) -> Expression {
    use Expression as E;
    if let Some(decided) = try_eval_boolean(expr, env) {
        return E::Literal(ScalarValue::Boolean(decided));
    }
    match expr {
        E::WhenOtherwise { condition, if_true, if_false } => {
            if let Some(b) = try_eval_boolean(condition, env) {
                if b {
                    simplify_expr_with_absdom(if_true, env)
                } else {
                    simplify_expr_with_absdom(if_false, env)
                }
            } else {
                E::WhenOtherwise {
                    condition: Box::new(simplify_expr_with_absdom(condition, env)),
                    if_true: Box::new(simplify_expr_with_absdom(if_true, env)),
                    if_false: Box::new(simplify_expr_with_absdom(if_false, env)),
                }
            }
        }
        E::And(l, r) => {
            let sl = simplify_expr_with_absdom(l, env);
            let sr = simplify_expr_with_absdom(r, env);
            match (&sl, &sr) {
                (E::Literal(ScalarValue::Boolean(true)), other)
                | (other, E::Literal(ScalarValue::Boolean(true))) => other.clone(),
                (E::Literal(ScalarValue::Boolean(false)), _)
                | (_, E::Literal(ScalarValue::Boolean(false))) => E::Literal(ScalarValue::Boolean(false)),
                _ => E::And(Box::new(sl), Box::new(sr)),
            }
        }
        E::Or(l, r) => {
            let sl = simplify_expr_with_absdom(l, env);
            let sr = simplify_expr_with_absdom(r, env);
            match (&sl, &sr) {
                (E::Literal(ScalarValue::Boolean(true)), _)
                | (_, E::Literal(ScalarValue::Boolean(true))) => E::Literal(ScalarValue::Boolean(true)),
                (E::Literal(ScalarValue::Boolean(false)), other)
                | (other, E::Literal(ScalarValue::Boolean(false))) => other.clone(),
                _ => E::Or(Box::new(sl), Box::new(sr)),
            }
        }
        E::Not(inner) => {
            let s = simplify_expr_with_absdom(inner, env);
            if let E::Literal(ScalarValue::Boolean(b)) = s {
                E::Literal(ScalarValue::Boolean(!b))
            } else {
                E::Not(Box::new(s))
            }
        }
        _ => expr.clone(),
    }
}

fn as_i64_scalar(lit: &ScalarValue) -> Option<i64> {
    match lit {
        ScalarValue::Long(v) => Some(*v),
        ScalarValue::Int(v) => Some(*v as i64),
        ScalarValue::Short(v) => Some(*v as i64),
        ScalarValue::Byte(v) => Some(*v as i64),
        _ => None,
    }
}

/// Extrai restrições de filtro na forma `coluna <op> constante` para refinar o ambiente abstrato.
fn extract_filter_constraint(condition: &Expression) -> Option<(String, Interval)> {
    use Expression as E;
    match condition {
        E::Lt(l, r) => match (l.as_ref(), r.as_ref()) {
            (E::Column(col), E::Literal(lit)) => {
                let c = as_i64_scalar(lit)?;
                Some((col.clone(), Interval::Bounded(i64::MIN, c.saturating_sub(1))))
            }
            _ => None,
        },
        E::Lte(l, r) => match (l.as_ref(), r.as_ref()) {
            (E::Column(col), E::Literal(lit)) => {
                let c = as_i64_scalar(lit)?;
                Some((col.clone(), Interval::Bounded(i64::MIN, c)))
            }
            _ => None,
        },
        E::Gt(l, r) => match (l.as_ref(), r.as_ref()) {
            (E::Column(col), E::Literal(lit)) => {
                let c = as_i64_scalar(lit)?;
                Some((col.clone(), Interval::Bounded(c.saturating_add(1), i64::MAX)))
            }
            _ => None,
        },
        E::Gte(l, r) => match (l.as_ref(), r.as_ref()) {
            (E::Column(col), E::Literal(lit)) => {
                let c = as_i64_scalar(lit)?;
                Some((col.clone(), Interval::Bounded(c, i64::MAX)))
            }
            _ => None,
        },
        E::Eq(l, r) => match (l.as_ref(), r.as_ref()) {
            (E::Column(col), E::Literal(lit)) => {
                let c = as_i64_scalar(lit)?;
                Some((col.clone(), Interval::point(c)))
            }
            _ => None,
        },
        _ => None,
    }
}

/// Otimização de Plano Lógico baseada no Domínio Abstrato (Galois Pruning & Predicate Simplification).
///
/// Percorre a árvore inferindo o ambiente abstrato bottom-up e simplificando top-down:
/// 1. Elimina nós `Filter` que sejam tautologias comprovadas (`condition == true`).
/// 2. Poda subárvores que resultem em conjunto vazio (`condition == false`), convertendo em `Range(0..0)`.
/// 3. Colapsa `WhenOtherwise` cujas condições sejam provadas pelo intervalo das colunas.
fn stage_may_benefit_from_absdom(stage: &LogicalStage) -> bool {
    match stage {
        LogicalStage::Filter { .. } => true,
        LogicalStage::WithColumn { input, expression, .. } => {
            matches!(expression, Expression::WhenOtherwise { .. }) || stage_may_benefit_from_absdom(input)
        }
        LogicalStage::Select { input, projections } => {
            projections.iter().any(|(_, e)| matches!(e, Expression::WhenOtherwise { .. }))
                || stage_may_benefit_from_absdom(input)
        }
        LogicalStage::Join { left, right, .. } | LogicalStage::Union { left, right, .. } => {
            stage_may_benefit_from_absdom(left) || stage_may_benefit_from_absdom(right)
        }
        LogicalStage::Range { .. } => false,
    }
}

pub fn simplify_stage_with_absdom(stage: &LogicalStage, env: &mut AbstractEnv) -> (LogicalStage, bool) {
    if !stage_may_benefit_from_absdom(stage) {
        return (stage.clone(), false);
    }
    simplify_stage_with_absdom_inner(stage, env)
}

fn simplify_stage_with_absdom_inner(stage: &LogicalStage, env: &mut AbstractEnv) -> (LogicalStage, bool) {
    match stage {
        LogicalStage::Range { start, end, step, num_partitions: _ } => {
            if *step > 0 && *start < *end {
                env.set("id", Interval::Bounded(*start, end - 1));
            } else if *step < 0 && *start > *end {
                env.set("id", Interval::Bounded(end + 1, *start));
            } else {
                env.set("id", Interval::Bottom);
            }
            (stage.clone(), false)
        }
        LogicalStage::WithColumn { input, column_name, expression } => {
            let (opt_input, mut changed) = simplify_stage_with_absdom_inner(input, env);
            let col_interval = eval_interval(expression, env);
            env.set(column_name.clone(), col_interval);
            let opt_expr = simplify_expr_with_absdom(expression, env);
            if &opt_expr != expression {
                changed = true;
            }
            (
                LogicalStage::WithColumn {
                    input: Box::new(opt_input),
                    column_name: column_name.clone(),
                    expression: opt_expr,
                },
                changed,
            )
        }
        LogicalStage::Filter { input, condition } => {
            let (opt_input, mut changed) = simplify_stage_with_absdom_inner(input, env);
            let simplified_cond = simplify_expr_with_absdom(condition, env);
            if &simplified_cond != condition {
                changed = true;
            }

            // Verifica se a condição foi resolvida estaticamente
            if let Some(decision) = try_eval_boolean(&simplified_cond, env) {
                if decision {
                    // Tautologia: o filtro aceita todas as tuplas. Elimina o nó Filter!
                    return (opt_input, true);
                } else {
                    // Contradição: nenhuma tupla passa pelo filtro. Poda total para Range vazio!
                    return (
                        LogicalStage::Range {
                            start: 0,
                            end: 0,
                            step: 1,
                            num_partitions: 1,
                        },
                        true,
                    );
                }
            }

            // Se o predicado for um refinador simples de coluna (ex: id < 50_000), estreita o ambiente
            if let Some((col, constraint)) = extract_filter_constraint(&simplified_cond) {
                env.refine(&col, constraint);
            }

            (
                LogicalStage::Filter {
                    input: Box::new(opt_input),
                    condition: simplified_cond,
                },
                changed,
            )
        }
        LogicalStage::Select { input, projections } => {
            let (opt_input, mut changed) = simplify_stage_with_absdom_inner(input, env);
            let mut opt_projections = Vec::with_capacity(projections.len());
            let mut next_env = AbstractEnv::new();

            for (name, expr) in projections {
                let opt_expr = simplify_expr_with_absdom(expr, env);
                if &opt_expr != expr {
                    changed = true;
                }
                let intv = eval_interval(&opt_expr, env);
                next_env.set(name.clone(), intv);
                opt_projections.push((name.clone(), opt_expr));
            }

            *env = next_env;
            (
                LogicalStage::Select {
                    input: Box::new(opt_input),
                    projections: opt_projections,
                },
                changed,
            )
        }
        LogicalStage::Join { left, right, left_key, right_key, join_type } => {
            let mut env_r = env.clone();
            let (opt_l, ch_l) = simplify_stage_with_absdom_inner(left, env);
            let (opt_r, ch_r) = simplify_stage_with_absdom_inner(right, &mut env_r);
            (
                LogicalStage::Join {
                    left: Box::new(opt_l),
                    right: Box::new(opt_r),
                    left_key: left_key.clone(),
                    right_key: right_key.clone(),
                    join_type: *join_type,
                },
                ch_l || ch_r,
            )
        }
        LogicalStage::Union { left, right, distinct } => {
            let mut env_r = env.clone();
            let (opt_l, ch_l) = simplify_stage_with_absdom_inner(left, env);
            let (opt_r, ch_r) = simplify_stage_with_absdom_inner(right, &mut env_r);
            // Na união, as propriedades são a envoltória convexa (join de reticulado) dos dois fluxos
            let mut merged = AbstractEnv::new();
            for (k, intv_l) in &env.ranges {
                if let Some(intv_r) = env_r.ranges.get(k) {
                    merged.set(k.clone(), intv_l.join(intv_r));
                } else {
                    merged.set(k.clone(), Interval::Top);
                }
            }
            *env = merged;
            (
                LogicalStage::Union {
                    left: Box::new(opt_l),
                    right: Box::new(opt_r),
                    distinct: *distinct,
                },
                ch_l || ch_r,
            )
        }
    }
}
