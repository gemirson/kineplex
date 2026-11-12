//! # Canonicalização por simetria (`algebra::canon`)
//!
//! Leva cada `Expression` à forma normal da ação do grupo de permutações dos operandos
//! sobre os operadores comutativos: `a + b` e `b + a` passam a ser a mesma árvore, o que
//! permite ao CSE existente enxergá-las como a mesma subexpressão.
//!
//! ## Escopo (v1) e invariantes de segurança
//!
//! - **Comutativos canonicalizados:** `Add`, `Mul`, `And`, `Or`, `Eq`, `Neq`.
//! - **Comparações:** `Gt(a, b) -> Lt(b, a)` e `Gte(a, b) -> Lte(b, a)`.
//! - **Bitwise NÃO são tocados:** o avaliador aceita `(Long, Int)` mas não `(Int, Long)`
//!   em `BitAnd/BitOr/BitXor`, então trocar operandos poderia transformar um plano válido
//!   em `panic!`. Sem informação de tipo no IR, a troca não é segura.
//! - **Sem associatividade:** em ponto flutuante `(a+b)+c != a+(b+c)`. Só a comutatividade
//!   é usada, que vale exatamente em IEEE-754.
//! - **Literais sempre à direita:** preserva os padrões de `fold_constants` e de strength
//!   reduction (`x * 2^k`, `x + 0`), evitando perda de qualidade de plano.
//! - **Idempotente e determinística:** troca apenas quando a ordem total diz que é preciso.

use crate::catalyst::optimizer::{cse_is_profitable, count_subexpressions};
use crate::catalyst::{Expression, ScalarValue};
use crate::sql::LogicalStage;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

/// Canonicaliza recursivamente uma expressão.
pub fn canonicalize_expr(expr: &Expression) -> Expression {
    use Expression as E;
    match expr {
        E::Literal(_) | E::Column(_) => expr.clone(),

        // Operadores comutativos: ordena os operandos.
        E::Add(l, r) => commute(l, r, E::Add),
        E::Mul(l, r) => commute(l, r, E::Mul),
        E::And(l, r) => commute(l, r, E::And),
        E::Or(l, r) => commute(l, r, E::Or),
        E::Eq(l, r) => commute(l, r, E::Eq),
        E::Neq(l, r) => commute(l, r, E::Neq),

        // Comparações: Gt/Gte viram Lt/Lte com operandos trocados (nunca reordenadas depois,
        // pois Lt/Lte não são comutativas).
        E::Gt(l, r) => E::Lt(Box::new(canonicalize_expr(r)), Box::new(canonicalize_expr(l))),
        E::Gte(l, r) => E::Lte(Box::new(canonicalize_expr(r)), Box::new(canonicalize_expr(l))),
        E::Lt(l, r) => E::Lt(Box::new(canonicalize_expr(l)), Box::new(canonicalize_expr(r))),
        E::Lte(l, r) => E::Lte(Box::new(canonicalize_expr(l)), Box::new(canonicalize_expr(r))),

        // Operadores sem simetria: apenas descem nos filhos.
        E::Sub(l, r) => E::Sub(bx(l), bx(r)),
        E::Div(l, r) => E::Div(bx(l), bx(r)),
        E::Mod(l, r) => E::Mod(bx(l), bx(r)),
        E::BitAnd(l, r) => E::BitAnd(bx(l), bx(r)),
        E::BitOr(l, r) => E::BitOr(bx(l), bx(r)),
        E::BitXor(l, r) => E::BitXor(bx(l), bx(r)),
        E::BitShiftLeft(l, r) => E::BitShiftLeft(bx(l), bx(r)),
        E::BitShiftRight(l, r) => E::BitShiftRight(bx(l), bx(r)),
        E::Not(e) => E::Not(bx(e)),
        E::Round(e, d) => E::Round(bx(e), *d),
        E::Cast { expr, target_type } => E::Cast {
            expr: bx(expr),
            target_type: *target_type,
        },
        E::WhenOtherwise {
            condition,
            if_true,
            if_false,
        } => E::WhenOtherwise {
            condition: bx(condition),
            if_true: bx(if_true),
            if_false: bx(if_false),
        },
        E::QuimeraFactor {
            start_day,
            end_day,
            indexer_code,
            anniv_day,
            pct_indexer,
            spread_aa,
            broadcast_engine,
        } => E::QuimeraFactor {
            start_day: bx(start_day),
            end_day: bx(end_day),
            indexer_code: bx(indexer_code),
            anniv_day: bx(anniv_day),
            pct_indexer: bx(pct_indexer),
            spread_aa: bx(spread_aa),
            broadcast_engine: Arc::clone(broadcast_engine),
        },
    }
}

/// Canonicaliza todas as expressões de uma árvore de `LogicalStage`.
pub fn canonicalize_stage(stage: &LogicalStage) -> LogicalStage {
    match stage {
        LogicalStage::Range { .. } => stage.clone(),
        LogicalStage::Select { input, projections } => LogicalStage::Select {
            input: Box::new(canonicalize_stage(input)),
            projections: projections
                .iter()
                .map(|(n, e)| (n.clone(), canonicalize_expr(e)))
                .collect(),
        },
        LogicalStage::WithColumn {
            input,
            column_name,
            expression,
        } => LogicalStage::WithColumn {
            input: Box::new(canonicalize_stage(input)),
            column_name: column_name.clone(),
            expression: canonicalize_expr(expression),
        },
        LogicalStage::Filter { input, condition } => LogicalStage::Filter {
            input: Box::new(canonicalize_stage(input)),
            condition: canonicalize_expr(condition),
        },
        LogicalStage::Join {
            left,
            right,
            left_key,
            right_key,
            join_type,
        } => LogicalStage::Join {
            left: Box::new(canonicalize_stage(left)),
            right: Box::new(canonicalize_stage(right)),
            left_key: left_key.clone(),
            right_key: right_key.clone(),
            join_type: *join_type,
        },
        LogicalStage::Union {
            left,
            right,
            distinct,
        } => LogicalStage::Union {
            left: Box::new(canonicalize_stage(left)),
            right: Box::new(canonicalize_stage(right)),
            distinct: *distinct,
        },
    }
}

/// Número de subexpressões distintas compartilhadas que o CSE materializaria (lucrativas).
fn profitable_shared(projections: &[(String, Expression)]) -> usize {
    let mut freq: HashMap<&Expression, usize> = HashMap::new();
    for (_, e) in projections {
        count_subexpressions(e, &mut freq);
    }
    freq.iter()
        .filter(|(e, n)| **n >= 2 && e.depth() >= 2 && cse_is_profitable(e, **n))
        .count()
}

/// Canonicalização **direcionada**: só adota a forma canônica de um `Select` quando ela aumenta
/// o número de subexpressões compartilhadas lucrativas (o único ganho medido da canonicalização).
/// Nos demais casos o plano permanece idêntico ao original, o que garante por construção
/// ausência de regressão em planos sem ganho. `Filter`, `WithColumn`, `Join` e `Union` não são
/// reescritos, pois o CSE só atua em projeções.
pub fn canonicalize_stage_targeted(stage: &LogicalStage) -> LogicalStage {
    match stage {
        LogicalStage::Range { .. } => stage.clone(),
        LogicalStage::Select { input, projections } => {
            let canon: Vec<(String, Expression)> = projections
                .iter()
                .map(|(n, e)| (n.clone(), canonicalize_expr(e)))
                .collect();
            let chosen = if profitable_shared(&canon) > profitable_shared(projections) {
                canon
            } else {
                projections.clone()
            };
            LogicalStage::Select {
                input: Box::new(canonicalize_stage_targeted(input)),
                projections: chosen,
            }
        }
        LogicalStage::WithColumn {
            input,
            column_name,
            expression,
        } => LogicalStage::WithColumn {
            input: Box::new(canonicalize_stage_targeted(input)),
            column_name: column_name.clone(),
            expression: expression.clone(),
        },
        LogicalStage::Filter { input, condition } => LogicalStage::Filter {
            input: Box::new(canonicalize_stage_targeted(input)),
            condition: condition.clone(),
        },
        LogicalStage::Join {
            left,
            right,
            left_key,
            right_key,
            join_type,
        } => LogicalStage::Join {
            left: Box::new(canonicalize_stage_targeted(left)),
            right: Box::new(canonicalize_stage_targeted(right)),
            left_key: left_key.clone(),
            right_key: right_key.clone(),
            join_type: *join_type,
        },
        LogicalStage::Union {
            left,
            right,
            distinct,
        } => LogicalStage::Union {
            left: Box::new(canonicalize_stage_targeted(left)),
            right: Box::new(canonicalize_stage_targeted(right)),
            distinct: *distinct,
        },
    }
}

fn bx(e: &Expression) -> Box<Expression> {
    Box::new(canonicalize_expr(e))
}

/// Canonicaliza os filhos e ordena o par segundo a ordem total `expr_cmp`.
fn commute(
    l: &Expression,
    r: &Expression,
    build: fn(Box<Expression>, Box<Expression>) -> Expression,
) -> Expression {
    let cl = canonicalize_expr(l);
    let cr = canonicalize_expr(r);
    if expr_cmp(&cl, &cr) == Ordering::Greater {
        build(Box::new(cr), Box::new(cl))
    } else {
        build(Box::new(cl), Box::new(cr))
    }
}

/// Rank de variante: `Literal` é o maior, para ficar sempre à direita.
fn rank(e: &Expression) -> u8 {
    use Expression as E;
    match e {
        E::Column(_) => 0,
        E::Not(_) => 1,
        E::Cast { .. } => 2,
        E::Round(..) => 3,
        E::Add(..) => 4,
        E::Sub(..) => 5,
        E::Mul(..) => 6,
        E::Div(..) => 7,
        E::Mod(..) => 8,
        E::BitAnd(..) => 9,
        E::BitOr(..) => 10,
        E::BitXor(..) => 11,
        E::BitShiftLeft(..) => 12,
        E::BitShiftRight(..) => 13,
        E::And(..) => 14,
        E::Or(..) => 15,
        E::Eq(..) => 16,
        E::Neq(..) => 17,
        E::Lt(..) => 18,
        E::Lte(..) => 19,
        E::Gt(..) => 20,
        E::Gte(..) => 21,
        E::WhenOtherwise { .. } => 22,
        E::QuimeraFactor { .. } => 23,
        E::Literal(_) => 255,
    }
}

fn children(e: &Expression) -> Vec<&Expression> {
    use Expression as E;
    match e {
        E::Literal(_) | E::Column(_) => vec![],
        E::Not(a) | E::Round(a, _) => vec![a],
        E::Cast { expr, .. } => vec![expr],
        E::Add(a, b)
        | E::Sub(a, b)
        | E::Mul(a, b)
        | E::Div(a, b)
        | E::Mod(a, b)
        | E::BitAnd(a, b)
        | E::BitOr(a, b)
        | E::BitXor(a, b)
        | E::BitShiftLeft(a, b)
        | E::BitShiftRight(a, b)
        | E::And(a, b)
        | E::Or(a, b)
        | E::Eq(a, b)
        | E::Neq(a, b)
        | E::Lt(a, b)
        | E::Lte(a, b)
        | E::Gt(a, b)
        | E::Gte(a, b) => vec![a, b],
        E::WhenOtherwise {
            condition,
            if_true,
            if_false,
        } => vec![condition, if_true, if_false],
        E::QuimeraFactor {
            start_day,
            end_day,
            indexer_code,
            anniv_day,
            pct_indexer,
            spread_aa,
            ..
        } => vec![start_day, end_day, indexer_code, anniv_day, pct_indexer, spread_aa],
    }
}

fn scalar_tag(v: &ScalarValue) -> u8 {
    match v {
        ScalarValue::Boolean(_) => 0,
        ScalarValue::Byte(_) => 1,
        ScalarValue::Short(_) => 2,
        ScalarValue::Int(_) => 3,
        ScalarValue::Long(_) => 4,
        ScalarValue::Double(_) => 5,
        ScalarValue::Decimal128(_) => 6,
    }
}

fn scalar_cmp(a: &ScalarValue, b: &ScalarValue) -> Ordering {
    use ScalarValue as S;
    scalar_tag(a)
        .cmp(&scalar_tag(b))
        .then_with(|| match (a, b) {
            (S::Boolean(x), S::Boolean(y)) => x.cmp(y),
            (S::Byte(x), S::Byte(y)) => x.cmp(y),
            (S::Short(x), S::Short(y)) => x.cmp(y),
            (S::Int(x), S::Int(y)) => x.cmp(y),
            (S::Long(x), S::Long(y)) => x.cmp(y),
            (S::Double(x), S::Double(y)) => x.total_cmp(y),
            // Mesma tag garantida; Debug é determinístico para o decimal.
            _ => format!("{:?}", a).cmp(&format!("{:?}", b)),
        })
}

/// Ordem total determinística sobre expressões (rank, depois conteúdo, depois filhos).
fn expr_cmp(a: &Expression, b: &Expression) -> Ordering {
    use Expression as E;
    let by_rank = rank(a).cmp(&rank(b));
    if by_rank != Ordering::Equal {
        return by_rank;
    }
    match (a, b) {
        (E::Literal(x), E::Literal(y)) => scalar_cmp(x, y),
        (E::Column(x), E::Column(y)) => x.cmp(y),
        (E::Round(_, dx), E::Round(_, dy)) => dx.cmp(dy).then_with(|| cmp_children(a, b)),
        (E::Cast { target_type: tx, .. }, E::Cast { target_type: ty, .. }) => {
            (*tx as u8).cmp(&(*ty as u8)).then_with(|| cmp_children(a, b))
        }
        (E::QuimeraFactor { broadcast_engine: bx_, .. }, E::QuimeraFactor { broadcast_engine: by, .. }) => {
            cmp_children(a, b)
                .then_with(|| (Arc::as_ptr(bx_) as usize).cmp(&(Arc::as_ptr(by) as usize)))
        }
        _ => cmp_children(a, b),
    }
}

fn cmp_children(a: &Expression, b: &Expression) -> Ordering {
    let ca = children(a);
    let cb = children(b);
    for (x, y) in ca.iter().zip(cb.iter()) {
        let o = expr_cmp(x, y);
        if o != Ordering::Equal {
            return o;
        }
    }
    ca.len().cmp(&cb.len())
}
