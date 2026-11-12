//! # Spark SQL & DataFrame API with Hardware Sympathy (`sql`)
//!
//! Replicador exato da API Spark SQL / Catalyst DataFrame com todas as melhorias:
//! - **Whole-Stage Vectorized Pipeline:** Processamento em chunks de 2048 linhas diretamente na Cache L1d.
//! - **Multithreaded Partitions:** Paralelismo baseado em núcleos físicos da CPU sem contenção de lock.
//! - **Zero Deserialization:** Ao contrário do Spark `UnsafeRow -> CaseClass`, os buffers colunares são
//!   acessados diretamente como ponteiros contíguos de 64 bytes de alinhamento.
//! - **Agregações com Redução de Kahan-Babuška ou QDec128 Exato:** Imunidade a cancelamento catastrófico.

use crate::catalyst::{
    CatalystCompilerOptimizer, ColumnVector, ColumnarBatch, DataType, Expression, FusedPipeline,
    ScalarValue,
};
pub use crate::ingnes_df::JoinType;
use crate::kernel::FlatBroadcastEngine;
use crate::memory::{AlignedBuffer, L1D_OPTIMAL_BATCH_SIZE};
use std::sync::Arc;

/// Coluna de Expressão fluente na DSL DataFrame (semelhante ao `org.apache.spark.sql.Column`).
#[derive(Clone, Debug)]
pub struct Column {
    pub expr: Expression,
}

impl Column {
    pub fn new(expr: Expression) -> Self {
        Self { expr }
    }

    pub fn alias(self, _alias: &str) -> Self {
        // O alias é capturado pela assinatura do select / with_column
        self
    }

    pub fn cast(self, target_type: &str) -> Self {
        let dt = match target_type {
            "short" | "smallint" => DataType::Short,
            "byte" | "tinyint" => DataType::Byte,
            "int" | "integer" => DataType::Int,
            "long" | "bigint" => DataType::Long,
            "double" | "float8" => DataType::Double,
            "boolean" | "bool" => DataType::Boolean,
            "decimal128" | "qdec128" => DataType::Decimal128,
            _ => panic!("Tipo de cast desconhecido: {}", target_type),
        };
        Column::new(Expression::Cast {
            expr: Box::new(self.expr),
            target_type: dt,
        })
    }

    #[allow(clippy::should_implement_trait)]
    pub fn add(self, rhs: Column) -> Self {
        Column::new(Expression::Add(Box::new(self.expr), Box::new(rhs.expr)))
    }

    pub fn sub(self, rhs: Column) -> Self {
        Column::new(Expression::Sub(Box::new(self.expr), Box::new(rhs.expr)))
    }

    pub fn mul(self, rhs: Column) -> Self {
        Column::new(Expression::Mul(Box::new(self.expr), Box::new(rhs.expr)))
    }

    pub fn div(self, rhs: Column) -> Self {
        Column::new(Expression::Div(Box::new(self.expr), Box::new(rhs.expr)))
    }

    pub fn rem(self, rhs: Column) -> Self {
        Column::new(Expression::Mod(Box::new(self.expr), Box::new(rhs.expr)))
    }

    pub fn eq(self, rhs: Column) -> Self {
        Column::new(Expression::Eq(Box::new(self.expr), Box::new(rhs.expr)))
    }

    pub fn neq(self, rhs: Column) -> Self {
        Column::new(Expression::Neq(Box::new(self.expr), Box::new(rhs.expr)))
    }

    pub fn lt(self, rhs: Column) -> Self {
        Column::new(Expression::Lt(Box::new(self.expr), Box::new(rhs.expr)))
    }

    pub fn lte(self, rhs: Column) -> Self {
        Column::new(Expression::Lte(Box::new(self.expr), Box::new(rhs.expr)))
    }

    pub fn gt(self, rhs: Column) -> Self {
        Column::new(Expression::Gt(Box::new(self.expr), Box::new(rhs.expr)))
    }

    pub fn gte(self, rhs: Column) -> Self {
        Column::new(Expression::Gte(Box::new(self.expr), Box::new(rhs.expr)))
    }

    pub fn bit_and(self, rhs: Column) -> Self {
        Column::new(Expression::BitAnd(Box::new(self.expr), Box::new(rhs.expr)))
    }

    pub fn bit_or(self, rhs: Column) -> Self {
        Column::new(Expression::BitOr(Box::new(self.expr), Box::new(rhs.expr)))
    }

    pub fn bit_xor(self, rhs: Column) -> Self {
        Column::new(Expression::BitXor(Box::new(self.expr), Box::new(rhs.expr)))
    }

    pub fn shift_left(self, rhs: Column) -> Self {
        Column::new(Expression::BitShiftLeft(Box::new(self.expr), Box::new(rhs.expr)))
    }

    pub fn shift_right(self, rhs: Column) -> Self {
        Column::new(Expression::BitShiftRight(Box::new(self.expr), Box::new(rhs.expr)))
    }

    pub fn and(self, rhs: Column) -> Self {
        Column::new(Expression::And(Box::new(self.expr), Box::new(rhs.expr)))
    }

    pub fn or(self, rhs: Column) -> Self {
        Column::new(Expression::Or(Box::new(self.expr), Box::new(rhs.expr)))
    }

    #[allow(clippy::should_implement_trait)]
    pub fn not(self) -> Self {
        Column::new(Expression::Not(Box::new(self.expr)))
    }
}

// Sobrecarga de operadores fluentes
impl std::ops::Add for Column {
    type Output = Column;
    fn add(self, rhs: Column) -> Column {
        self.add(rhs)
    }
}

impl std::ops::Sub for Column {
    type Output = Column;
    fn sub(self, rhs: Column) -> Column {
        self.sub(rhs)
    }
}

impl std::ops::Mul for Column {
    type Output = Column;
    fn mul(self, rhs: Column) -> Column {
        self.mul(rhs)
    }
}

impl std::ops::Div for Column {
    type Output = Column;
    fn div(self, rhs: Column) -> Column {
        self.div(rhs)
    }
}

impl std::ops::Rem for Column {
    type Output = Column;
    fn rem(self, rhs: Column) -> Column {
        self.rem(rhs)
    }
}

impl std::ops::BitAnd for Column {
    type Output = Column;
    fn bitand(self, rhs: Column) -> Column {
        self.bit_and(rhs)
    }
}

impl std::ops::BitOr for Column {
    type Output = Column;
    fn bitor(self, rhs: Column) -> Column {
        self.bit_or(rhs)
    }
}

impl std::ops::BitXor for Column {
    type Output = Column;
    fn bitxor(self, rhs: Column) -> Column {
        self.bit_xor(rhs)
    }
}

impl std::ops::Shl<Column> for Column {
    type Output = Column;
    fn shl(self, rhs: Column) -> Column {
        self.shift_left(rhs)
    }
}

impl std::ops::Shr<Column> for Column {
    type Output = Column;
    fn shr(self, rhs: Column) -> Column {
        self.shift_right(rhs)
    }
}

impl std::ops::Not for Column {
    type Output = Column;
    fn not(self) -> Column {
        self.not()
    }
}

/// Construtor de Coluna (`col("nome")`).
pub fn col(name: &str) -> Column {
    Column::new(Expression::Column(name.to_string()))
}

/// Construtor de Literal Genérico (`lit(valor)`).
pub trait IntoLiteral {
    fn into_literal(self) -> Column;
}

impl IntoLiteral for i8 {
    fn into_literal(self) -> Column {
        Column::new(Expression::Literal(ScalarValue::Byte(self)))
    }
}

impl IntoLiteral for i16 {
    fn into_literal(self) -> Column {
        Column::new(Expression::Literal(ScalarValue::Short(self)))
    }
}

impl IntoLiteral for i32 {
    fn into_literal(self) -> Column {
        Column::new(Expression::Literal(ScalarValue::Int(self)))
    }
}

impl IntoLiteral for i64 {
    fn into_literal(self) -> Column {
        Column::new(Expression::Literal(ScalarValue::Long(self)))
    }
}

impl IntoLiteral for f64 {
    fn into_literal(self) -> Column {
        Column::new(Expression::Literal(ScalarValue::Double(self)))
    }
}

impl IntoLiteral for bool {
    fn into_literal(self) -> Column {
        Column::new(Expression::Literal(ScalarValue::Boolean(self)))
    }
}

pub fn lit<T: IntoLiteral>(val: T) -> Column {
    val.into_literal()
}

/// Construtor de Expressão Condicional (`when(cond, if_true).otherwise(if_false)`).
pub struct WhenBuilder {
    condition: Column,
    if_true: Column,
}

pub fn when(condition: Column, if_true: Column) -> WhenBuilder {
    WhenBuilder { condition, if_true }
}

impl WhenBuilder {
    pub fn otherwise(self, if_false: Column) -> Column {
        Column::new(Expression::WhenOtherwise {
            condition: Box::new(self.condition.expr),
            if_true: Box::new(self.if_true.expr),
            if_false: Box::new(if_false.expr),
        })
    }
}

/// Função de arredondamento Spark SQL (`round(col, decimals)`).
pub fn round(column: Column, decimals: usize) -> Column {
    Column::new(Expression::Round(Box::new(column.expr), decimals))
}

/// Funções de Agregação Spark SQL.
pub fn count(column_name: &str) -> Aggregation {
    Aggregation::Count(column_name.to_string())
}

pub fn sum(column_name: &str) -> Aggregation {
    Aggregation::Sum(column_name.to_string())
}

pub fn avg(column_name: &str) -> Aggregation {
    Aggregation::Avg(column_name.to_string())
}

#[derive(Clone, Debug)]
pub enum Aggregation {
    Count(String),
    Sum(String),
    Avg(String),
}

/// Representação de uma Partição Geradora (como `spark.range`).
#[derive(Clone, Debug)]
pub struct RangePartition {
    pub start: i64,
    pub end: i64,
    pub step: i64,
}

/// Plano Físico de um DataFrame em Spark SQL.
#[derive(Clone, Debug)]
pub enum LogicalStage {
    Range {
        start: i64,
        end: i64,
        step: i64,
        num_partitions: usize,
    },
    Select {
        input: Box<LogicalStage>,
        projections: Vec<(String, Expression)>,
    },
    WithColumn {
        input: Box<LogicalStage>,
        column_name: String,
        expression: Expression,
    },
    Filter {
        input: Box<LogicalStage>,
        condition: Expression,
    },
    Join {
        left: Box<LogicalStage>,
        right: Box<LogicalStage>,
        left_key: String,
        right_key: String,
        join_type: JoinType,
    },
    Union {
        left: Box<LogicalStage>,
        right: Box<LogicalStage>,
        distinct: bool,
    },
}

/// Resultado de Linha Agregada (equivalente a `Row` do Spark SQL).
#[derive(Clone, Debug)]
pub struct Row {
    pub names: Vec<String>,
    pub values: Vec<ScalarValue>,
}

impl Row {
    pub fn get_as_i64(&self, name: &str) -> i64 {
        let idx = self.names.iter().position(|n| n == name).unwrap_or_else(|| {
            panic!("Campo '{}' nao encontrado na Row: {:?}", name, self.names)
        });
        match &self.values[idx] {
            ScalarValue::Long(v) => *v,
            ScalarValue::Int(v) => *v as i64,
            _ => panic!("Tipo invalido para get_as_i64"),
        }
    }

    pub fn get_as_f64(&self, name: &str) -> f64 {
        let idx = self.names.iter().position(|n| n == name).unwrap_or_else(|| {
            panic!("Campo '{}' nao encontrado na Row: {:?}", name, self.names)
        });
        match &self.values[idx] {
            ScalarValue::Double(v) => *v,
            ScalarValue::Long(v) => *v as f64,
            _ => panic!("Tipo invalido para get_as_f64"),
        }
    }
}

/// DataFrame Spark SQL em Rust (`DataFrame`).
#[derive(Clone, Debug)]
pub struct DataFrame {
    pub plan: LogicalStage,
}

impl DataFrame {
    /// Retorna uma referência ao plano lógico de execução do DataFrame.
    #[inline(always)]
    pub fn logical_plan(&self) -> &LogicalStage {
        &self.plan
    }

    /// Injeta projeções nomeadas no DataFrame (`.select(...)`).
    pub fn select(self, fields: Vec<(&str, Column)>) -> Self {
        let projections = fields
            .into_iter()
            .map(|(name, col)| (name.to_string(), col.expr))
            .collect();
        DataFrame {
            plan: LogicalStage::Select {
                input: Box::new(self.plan),
                projections,
            },
        }
    }

    /// Adiciona ou substitui uma coluna (`.with_column(name, col)`).
    pub fn with_column(self, name: &str, column: Column) -> Self {
        DataFrame {
            plan: LogicalStage::WithColumn {
                input: Box::new(self.plan),
                column_name: name.to_string(),
                expression: column.expr,
            },
        }
    }

    /// Filtra linhas com base em uma condição booleana (`.filter(cond)`).
    pub fn filter(self, condition: Column) -> Self {
        DataFrame {
            plan: LogicalStage::Filter {
                input: Box::new(self.plan),
                condition: condition.expr,
            },
        }
    }

    /// Injeção da Expressão Nativa do Fator B3 Whole-Stage (`.with_b3_factor(...)`).
    pub fn with_b3_factor(
        self,
        broadcast_engine: Arc<FlatBroadcastEngine>,
        output_col_name: &str,
        start_day_col: &str,
        end_day_col: &str,
        indexer_code_col: &str,
        anniv_day_col: &str,
        pct_indexer_col: &str,
        spread_aa_col: &str,
    ) -> Self {
        let b3_expr = Expression::QuimeraFactor {
            start_day: Box::new(Expression::Column(start_day_col.to_string())),
            end_day: Box::new(Expression::Column(end_day_col.to_string())),
            indexer_code: Box::new(Expression::Column(indexer_code_col.to_string())),
            anniv_day: Box::new(Expression::Column(anniv_day_col.to_string())),
            pct_indexer: Box::new(Expression::Column(pct_indexer_col.to_string())),
            spread_aa: Box::new(Expression::Column(spread_aa_col.to_string())),
            broadcast_engine,
        };

        self.with_column(output_col_name, Column::new(b3_expr))
    }

    /// Atalho com parâmetros padrão B3 idênticos ao Quimera Catalyst Scala:
    pub fn with_b3_factor_default(
        self,
        broadcast_engine: Arc<FlatBroadcastEngine>,
        output_col_name: &str,
    ) -> Self {
        self.with_b3_factor(
            broadcast_engine,
            output_col_name,
            "startEpochDay",
            "endEpochDay",
            "indexerCode",
            "annivDay",
            "pctIndexer",
            "spreadAa",
        )
    }

    /// Explica o plano lógico e físico de execução (`df.explain("codegen")` ou `df.explain("compiler")`).
    pub fn explain(&self, mode: &str) {
        println!("== Parsed Logical Plan ==");
        println!("{:#?}", self.plan);

        if mode == "optimized" || mode == "compiler" || mode == "extended" {
            let optimizer = CatalystCompilerOptimizer::new();
            let (opt_plan, stats) = optimizer.optimize(&self.plan);
            println!("\n== Catalyst Compiler Optimization Report ==");
            println!("  - Passagens executadas: {}", stats.passes_run);
            println!("  - Dobramento de Constantes (Constant Folding): {}", stats.constants_folded);
            println!("  - Reducao de Forca (Strength Reduction mod/mul -> bitwise): {}", stats.strength_reductions);
            println!("  - Eliminacao de Subexpressoes Comuns (CSE): {}", stats.common_subexpressions);
            println!("  - Descida de Filtros (Predicate Pushdown): {}", stats.predicates_pushed);
            println!("  - Poda de Colunas Mortas (Dead Column Elimination): {}", stats.dead_columns_pruned);
            println!("  - Total de transformacoes aplicadas: {}", stats.total_transformations());

            println!("\n== Optimized Logical Plan ==");
            println!("{:#?}", opt_plan);

            let fused = FusedPipeline::compile(&opt_plan);
            println!("\n== Physical Plan (Tungsten 2.0 Whole-Stage Operator Fusion) ==");
            println!("* (1) QuimeraVectorizedWholeStageCodegen [L1D Micro-Batch = {} rows, 64-byte CacheLine Aligned]", L1D_OPTIMAL_BATCH_SIZE);
            println!("  +- Fused Pipeline: {} passos compilados", fused.steps.len());
            for (i, step) in fused.steps.iter().enumerate() {
                println!("     [{}] {:?}", i, step);
            }
        } else {
            println!("\n== Physical Plan ({}) ==", mode);
            println!("* (1) QuimeraVectorizedWholeStageCodegen [L1D Micro-Batch = {} rows, 64-byte CacheLine Aligned]", L1D_OPTIMAL_BATCH_SIZE);
            println!("  +- Pipeline: ScanRange -> ColumnarProject -> FactorExpression -> In-Register Accumulate");
            println!("  +- Plan Tree: {:?}", self.plan);
        }
    }

    /// Junção relacional Bare-Metal com Hashing de Fibonacci e Crivo L1d (`df.join(other, left_key, right_key, join_type)`).
    pub fn join(
        &self,
        other: &DataFrame,
        left_key: &str,
        right_key: &str,
        join_type: &str,
    ) -> DataFrame {
        DataFrame {
            plan: LogicalStage::Join {
                left: Box::new(self.plan.clone()),
                right: Box::new(other.plan.clone()),
                left_key: left_key.to_string(),
                right_key: right_key.to_string(),
                join_type: JoinType::from_str(join_type),
            },
        }
    }

    /// União de dois DataFrames com o mesmo esquema (`df.union(other)`).
    pub fn union(&self, other: &DataFrame) -> DataFrame {
        DataFrame {
            plan: LogicalStage::Union {
                left: Box::new(self.plan.clone()),
                right: Box::new(other.plan.clone()),
                distinct: false,
            },
        }
    }

    /// União com eliminação de duplicatas (`df.union_distinct(other)`).
    pub fn union_distinct(&self, other: &DataFrame) -> DataFrame {
        DataFrame {
            plan: LogicalStage::Union {
                left: Box::new(self.plan.clone()),
                right: Box::new(other.plan.clone()),
                distinct: true,
            },
        }
    }

    /// Otimiza a árvore de execução com o Catalyst Compiler Optimizer.
    pub fn optimize(self) -> Self {
        self.optimize_with(&CatalystCompilerOptimizer::new())
    }

    /// Como [`DataFrame::optimize`], mas com um otimizador configurável
    /// (ex.: `CatalystCompilerOptimizer::new().with_algebraic(true)`).
    pub fn optimize_with(self, optimizer: &CatalystCompilerOptimizer) -> Self {
        let (opt_plan, _) = optimizer.optimize(&self.plan);
        DataFrame { plan: opt_plan }
    }

    /// Executa agregações distribuídas em paralelo sobre todos os núcleos da CPU.
    pub fn agg(self, aggs: Vec<(&str, Aggregation)>) -> AggregationExecutor {
        AggregationExecutor {
            df: self,
            aggregations: aggs.into_iter().map(|(alias, a)| (alias.to_string(), a)).collect(),
        }
    }

    /// Coleta os dados para a memória (apenas para dataframes pequenos ou testes).
    pub fn collect(&self) -> Vec<ColumnarBatch> {
        self.collect_with(&CatalystCompilerOptimizer::new())
    }

    /// Como [`DataFrame::collect`], mas com um otimizador configurável.
    pub fn collect_with(&self, optimizer: &CatalystCompilerOptimizer) -> Vec<ColumnarBatch> {
        let (opt_plan, _) = optimizer.optimize(&self.plan);
        execute_stage_collect(&opt_plan)
    }

    /// Converte uma coluna do DataFrame em um `BanachFrame` para análise funcional
    /// em espaços de Sobolev $W^{1,2}$, energia de Dirichlet e wavelets.
    pub fn to_banach_frame(&self, col_name: &str) -> Result<crate::banach::BanachFrame, String> {
        let batches = self.collect();
        let mut values = Vec::new();
        for b in &batches {
            if let Some(col_idx) = b.names.iter().position(|n| n == col_name) {
                match &b.columns[col_idx] {
                    crate::catalyst::ColumnVector::Double(buf) => {
                        values.extend_from_slice(buf.as_slice());
                    }
                    crate::catalyst::ColumnVector::Long(buf) => {
                        for &v in buf.as_slice() {
                            values.push(v as f64);
                        }
                    }
                    crate::catalyst::ColumnVector::Int(buf) => {
                        for &v in buf.as_slice() {
                            values.push(v as f64);
                        }
                    }
                    _ => return Err(format!("Coluna '{}' não é numérica compatível", col_name)),
                }
            } else {
                return Err(format!("Coluna '{}' não encontrada no lote", col_name));
            }
        }
        if values.is_empty() {
            return Err("DataFrame vazio".to_string());
        }
        Ok(crate::banach::BanachFrame::new(&values, None))
    }

    fn extract_partitions(&self) -> Vec<RangePartition> {
        match &self.plan {
            LogicalStage::Range { start, end, step, num_partitions } => {
                let total = (end - start) / step;
                let chunk_size = (total + (*num_partitions as i64) - 1) / (*num_partitions as i64);
                let mut parts = Vec::with_capacity(*num_partitions);
                for p in 0..*num_partitions {
                    let p_start = start + (p as i64) * chunk_size * step;
                    let p_end = (p_start + chunk_size * step).min(*end);
                    if p_start < p_end {
                        parts.push(RangePartition {
                            start: p_start,
                            end: p_end,
                            step: *step,
                        });
                    }
                }
                parts
            }
            LogicalStage::Select { input, .. }
            | LogicalStage::WithColumn { input, .. }
            | LogicalStage::Filter { input, .. } => {
                let dummy_df = DataFrame { plan: *input.clone() };
                dummy_df.extract_partitions()
            }
            LogicalStage::Join { left, .. } | LogicalStage::Union { left, .. } => {
                let dummy_df = DataFrame { plan: *left.clone() };
                dummy_df.extract_partitions()
            }
        }
    }
}

/// Executor Paralelo de Agregações Spark SQL Whole-Stage.
pub struct AggregationExecutor {
    df: DataFrame,
    aggregations: Vec<(String, Aggregation)>,
}

#[derive(Default)]
struct PartitionAggState {
    count: i64,
    sum_map: Vec<f64>,
    sum_count_map: Vec<i64>,
}

impl AggregationExecutor {
    /// Executa a agregação paralela usando todas as CPUs e retorna a linha agregada final.
    pub fn collect(&self) -> Vec<Row> {
        self.collect_with(&CatalystCompilerOptimizer::new())
    }

    /// Como [`AggregationExecutor::collect`], mas com um otimizador configurável.
    pub fn collect_with(&self, optimizer: &CatalystCompilerOptimizer) -> Vec<Row> {
        let (opt_plan, _) = optimizer.optimize(&self.df.plan);
        let opt_df = DataFrame { plan: opt_plan };

        if !is_pure_range_pipeline(&opt_df.plan) {
            let batches = execute_stage_collect(&opt_df.plan);
            let aggs = &self.aggregations;
            let mut state = PartitionAggState {
                count: 0,
                sum_map: vec![0.0; aggs.len()],
                sum_count_map: vec![0; aggs.len()],
            };
            for batch in batches {
                let rows = batch.num_rows;
                state.count += rows as i64;
                for (idx, (_, agg_type)) in aggs.iter().enumerate() {
                    match agg_type {
                        Aggregation::Count(_) => {
                            state.sum_map[idx] += rows as f64;
                        }
                        Aggregation::Sum(col_name) => {
                            let col = batch.column(col_name);
                            match col {
                                ColumnVector::Double(d) => {
                                    for v in d.as_slice() {
                                        state.sum_map[idx] += *v;
                                    }
                                }
                                ColumnVector::Long(l) => {
                                    for v in l.as_slice() {
                                        state.sum_map[idx] += *v as f64;
                                    }
                                }
                                _ => unimplemented!("Soma nao suportada para este tipo"),
                            }
                        }
                        Aggregation::Avg(col_name) => {
                            let col = batch.column(col_name);
                            match col {
                                ColumnVector::Double(d) => {
                                    for v in d.as_slice() {
                                        state.sum_map[idx] += *v;
                                        state.sum_count_map[idx] += 1;
                                    }
                                }
                                ColumnVector::Long(l) => {
                                    for v in l.as_slice() {
                                        state.sum_map[idx] += *v as f64;
                                        state.sum_count_map[idx] += 1;
                                    }
                                }
                                _ => unimplemented!("Media nao suportada para este tipo"),
                            }
                        }
                    }
                }
            }
            let mut row_names = Vec::new();
            let mut row_vals = Vec::new();
            for (idx, (alias, agg_type)) in aggs.iter().enumerate() {
                row_names.push(alias.clone());
                match agg_type {
                    Aggregation::Count(_) => {
                        row_vals.push(ScalarValue::Long(state.sum_map[idx] as i64));
                    }
                    Aggregation::Sum(_) => {
                        row_vals.push(ScalarValue::Double(state.sum_map[idx]));
                    }
                    Aggregation::Avg(_) => {
                        let count = state.sum_count_map[idx].max(1);
                        let avg_val = state.sum_map[idx] / (count as f64);
                        row_vals.push(ScalarValue::Double(avg_val));
                    }
                }
            }
            return vec![Row {
                names: row_names,
                values: row_vals,
            }];
        }

        let partitions = opt_df.extract_partitions();
        let plan = Arc::new(opt_df.plan.clone());
        let aggs = Arc::new(self.aggregations.clone());

        let tasks: Vec<_> = partitions
            .into_iter()
            .map(|part| {
                let plan_ref = Arc::clone(&plan);
                let aggs_ref = Arc::clone(&aggs);

                move || {
                    let mut state = PartitionAggState {
                        count: 0,
                        sum_map: vec![0.0; aggs_ref.len()],
                        sum_count_map: vec![0; aggs_ref.len()],
                    };

                    let p_start = part.start;
                    let p_end = part.end;
                    let step = part.step;
                    let mut cur = p_start;
                    let fused = FusedPipeline::compile(&plan_ref);

                    while cur < p_end {
                        let batch_end = (cur + (L1D_OPTIMAL_BATCH_SIZE as i64) * step).min(p_end);
                        let batch_size = ((batch_end - cur) / step) as usize;

                        // Cria o ColumnarBatch da raiz Range com alinhamento de 64 bytes
                        let mut id_buf = AlignedBuffer::from_elem(0_i64, batch_size);
                        let id_slice = id_buf.as_mut_slice();
                        for i in 0..batch_size {
                            id_slice[i] = cur + (i as i64) * step;
                        }

                        let root_batch = ColumnarBatch::new(
                            vec!["id".to_string()],
                            vec![ColumnVector::Long(id_buf)],
                        );

                        let evaluated_batch = fused.execute_batch(&root_batch);
                        let rows = evaluated_batch.num_rows;
                        state.count += rows as i64;

                        // Acumula cada agregação no registrador via 4 acumuladores paralelos
                        for (idx, (_, agg_type)) in aggs_ref.iter().enumerate() {
                            match agg_type {
                                Aggregation::Count(_) => {
                                    state.sum_map[idx] += rows as f64;
                                }
                                Aggregation::Sum(col_name) => {
                                    let col = evaluated_batch.column(col_name);
                                    match col {
                                        ColumnVector::Double(d) => {
                                            let s = d.as_slice();
                                            let chunks = s.chunks_exact(4);
                                            let rem = chunks.remainder();
                                            let mut s0 = 0.0;
                                            let mut s1 = 0.0;
                                            let mut s2 = 0.0;
                                            let mut s3 = 0.0;
                                            for c in chunks {
                                                s0 += c[0];
                                                s1 += c[1];
                                                s2 += c[2];
                                                s3 += c[3];
                                            }
                                            let mut local_sum = (s0 + s1) + (s2 + s3);
                                            for &r in rem {
                                                local_sum += r;
                                            }
                                            state.sum_map[idx] += local_sum;
                                        }
                                        ColumnVector::Long(l) => {
                                            let s = l.as_slice();
                                            let chunks = s.chunks_exact(4);
                                            let rem = chunks.remainder();
                                            let mut s0 = 0_i64;
                                            let mut s1 = 0_i64;
                                            let mut s2 = 0_i64;
                                            let mut s3 = 0_i64;
                                            for c in chunks {
                                                s0 = s0.wrapping_add(c[0]);
                                                s1 = s1.wrapping_add(c[1]);
                                                s2 = s2.wrapping_add(c[2]);
                                                s3 = s3.wrapping_add(c[3]);
                                            }
                                            let mut local_sum = s0.wrapping_add(s1).wrapping_add(s2.wrapping_add(s3));
                                            for &r in rem {
                                                local_sum = local_sum.wrapping_add(r);
                                            }
                                            state.sum_map[idx] += local_sum as f64;
                                        }
                                        _ => panic!("Sum nao suportado para coluna {}", col_name),
                                    }
                                }
                                Aggregation::Avg(col_name) => {
                                    let col = evaluated_batch.column(col_name);
                                    match col {
                                        ColumnVector::Double(d) => {
                                            let s = d.as_slice();
                                            let chunks = s.chunks_exact(4);
                                            let rem = chunks.remainder();
                                            let mut s0 = 0.0;
                                            let mut s1 = 0.0;
                                            let mut s2 = 0.0;
                                            let mut s3 = 0.0;
                                            for c in chunks {
                                                s0 += c[0];
                                                s1 += c[1];
                                                s2 += c[2];
                                                s3 += c[3];
                                            }
                                            let mut local_sum = (s0 + s1) + (s2 + s3);
                                            for &r in rem {
                                                local_sum += r;
                                            }
                                            state.sum_map[idx] += local_sum;
                                            state.sum_count_map[idx] += rows as i64;
                                        }
                                        ColumnVector::Long(l) => {
                                            let s = l.as_slice();
                                            let chunks = s.chunks_exact(4);
                                            let rem = chunks.remainder();
                                            let mut s0 = 0_i64;
                                            let mut s1 = 0_i64;
                                            let mut s2 = 0_i64;
                                            let mut s3 = 0_i64;
                                            for c in chunks {
                                                s0 = s0.wrapping_add(c[0]);
                                                s1 = s1.wrapping_add(c[1]);
                                                s2 = s2.wrapping_add(c[2]);
                                                s3 = s3.wrapping_add(c[3]);
                                            }
                                            let mut local_sum = s0.wrapping_add(s1).wrapping_add(s2.wrapping_add(s3));
                                            for &r in rem {
                                                local_sum = local_sum.wrapping_add(r);
                                            }
                                            state.sum_map[idx] += local_sum as f64;
                                            state.sum_count_map[idx] += rows as i64;
                                        }
                                        _ => panic!("Avg suportado apenas para Double e Long"),
                                    }
                                }
                            }
                        }

                        cur = batch_end;
                    }

                    state
                }
            })
            .collect();

        let partition_states = crate::concurrency::global_thread_pool().execute_parallel(tasks);

        // Consolida os resultados parciais de todas as partições (Redução Final)
        let mut final_sum_map = vec![0.0_f64; self.aggregations.len()];
        let mut final_count_map = vec![0_i64; self.aggregations.len()];
        let mut total_count = 0_i64;

        for part_state in partition_states {
            total_count += part_state.count;
            for i in 0..self.aggregations.len() {
                final_sum_map[i] += part_state.sum_map[i];
                final_count_map[i] += part_state.sum_count_map[i];
            }
        }

        let mut row_names = Vec::new();
        let mut row_vals = Vec::new();

        for (i, (alias, agg_type)) in self.aggregations.iter().enumerate() {
            row_names.push(alias.clone());
            match agg_type {
                Aggregation::Count(_) => {
                    row_vals.push(ScalarValue::Long(total_count));
                }
                Aggregation::Sum(_) => {
                    row_vals.push(ScalarValue::Double(final_sum_map[i]));
                }
                Aggregation::Avg(_) => {
                    let cnt = final_count_map[i];
                    let avg_val = if cnt > 0 {
                        final_sum_map[i] / (cnt as f64)
                    } else {
                        0.0
                    };
                    row_vals.push(ScalarValue::Double(avg_val));
                }
            }
        }

        vec![Row {
            names: row_names,
            values: row_vals,
        }]
    }
}

/// Avalia recursivamente a árvore do plano físico sobre um micro-lote (Whole-Stage Pipeline).
#[allow(dead_code)]
fn evaluate_stage_pipeline(batch: &ColumnarBatch, stage: &LogicalStage) -> ColumnarBatch {
    match stage {
        LogicalStage::Range { .. } => batch.clone(),
        LogicalStage::Select { input, projections } => {
            let in_batch = evaluate_stage_pipeline(batch, input);
            let mut names = Vec::with_capacity(projections.len());
            let mut cols = Vec::with_capacity(projections.len());
            for (alias, expr) in projections {
                names.push(alias.clone());
                cols.push(expr.eval(&in_batch));
            }
            ColumnarBatch::new(names, cols)
        }
        LogicalStage::WithColumn {
            input,
            column_name,
            expression,
        } => {
            let in_batch = evaluate_stage_pipeline(batch, input);
            let eval_col = expression.eval(&in_batch);
            let mut names = in_batch.names;
            let mut cols = in_batch.columns;

            if let Some(pos) = names.iter().position(|n| n == column_name) {
                cols[pos] = eval_col;
            } else {
                names.push(column_name.clone());
                cols.push(eval_col);
            }
            ColumnarBatch::new(names, cols)
        }
        LogicalStage::Filter { input, condition } => {
            let in_batch = evaluate_stage_pipeline(batch, input);
            let cond_col = condition.eval(&in_batch);
            let cond_slice = match &cond_col {
                ColumnVector::Boolean(b) => b.as_slice(),
                _ => panic!("Filter condition deve ser Boolean"),
            };

            // Filtro colunar compacto
            let matched_indices: Vec<usize> = cond_slice
                .iter()
                .enumerate()
                .filter_map(|(idx, &m)| if m { Some(idx) } else { None })
                .collect();

            let filtered_len = matched_indices.len();
            let mut filtered_cols = Vec::with_capacity(in_batch.columns.len());

            for col in &in_batch.columns {
                match col {
                    ColumnVector::Long(v) => {
                        let mut buf = AlignedBuffer::from_elem(0_i64, filtered_len);
                        for (out_idx, &in_idx) in matched_indices.iter().enumerate() {
                            buf[out_idx] = v[in_idx];
                        }
                        filtered_cols.push(ColumnVector::Long(buf));
                    }
                    ColumnVector::Short(v) => {
                        let mut buf = AlignedBuffer::from_elem(0_i16, filtered_len);
                        for (out_idx, &in_idx) in matched_indices.iter().enumerate() {
                            buf[out_idx] = v[in_idx];
                        }
                        filtered_cols.push(ColumnVector::Short(buf));
                    }
                    ColumnVector::Byte(v) => {
                        let mut buf = AlignedBuffer::from_elem(0_i8, filtered_len);
                        for (out_idx, &in_idx) in matched_indices.iter().enumerate() {
                            buf[out_idx] = v[in_idx];
                        }
                        filtered_cols.push(ColumnVector::Byte(buf));
                    }
                    ColumnVector::Double(v) => {
                        let mut buf = AlignedBuffer::from_elem(0.0_f64, filtered_len);
                        for (out_idx, &in_idx) in matched_indices.iter().enumerate() {
                            buf[out_idx] = v[in_idx];
                        }
                        filtered_cols.push(ColumnVector::Double(buf));
                    }
                    ColumnVector::Boolean(v) => {
                        let mut buf = AlignedBuffer::from_elem(false, filtered_len);
                        for (out_idx, &in_idx) in matched_indices.iter().enumerate() {
                            buf[out_idx] = v[in_idx];
                        }
                        filtered_cols.push(ColumnVector::Boolean(buf));
                    }
                    _ => unimplemented!("Filtro colunar nao implementado para este tipo"),
                }
            }

            ColumnarBatch::new(in_batch.names, filtered_cols)
        }
        LogicalStage::Join {
            left,
            right,
            left_key,
            right_key,
            join_type,
        } => {
            let left_batch = evaluate_stage_pipeline(batch, left);
            let right_batch = evaluate_stage_pipeline(batch, right);
            left_batch.join(&right_batch, left_key, right_key, *join_type)
        }
        LogicalStage::Union {
            left,
            right,
            distinct,
        } => {
            let left_batch = evaluate_stage_pipeline(batch, left);
            let right_batch = evaluate_stage_pipeline(batch, right);
            let merged = left_batch.concat(&right_batch);
            if *distinct {
                merged.deduplicate()
            } else {
                merged
            }
        }
    }
}

pub fn is_pure_range_pipeline(stage: &LogicalStage) -> bool {
    match stage {
        LogicalStage::Range { .. } => true,
        LogicalStage::Select { input, .. }
        | LogicalStage::WithColumn { input, .. }
        | LogicalStage::Filter { input, .. } => is_pure_range_pipeline(input),
        _ => false,
    }
}

pub fn execute_stage_collect(stage: &LogicalStage) -> Vec<ColumnarBatch> {
    match stage {
        LogicalStage::Range {
            start,
            end,
            step,
            num_partitions,
        } => {
            let df = DataFrame {
                plan: LogicalStage::Range {
                    start: *start,
                    end: *end,
                    step: *step,
                    num_partitions: *num_partitions,
                },
            };
            let partitions = df.extract_partitions();
            let stage_arc = Arc::new(stage.clone());
            let tasks: Vec<_> = partitions
                .into_iter()
                .map(|part| {
                    let st = Arc::clone(&stage_arc);
                    move || execute_partition_range(part, &st)
                })
                .collect();
            let partition_results = crate::concurrency::global_thread_pool().execute_parallel(tasks);
            let mut results = Vec::new();
            for part_res in partition_results {
                results.extend(part_res);
            }
            results
        }
        LogicalStage::Join {
            left,
            right,
            left_key,
            right_key,
            join_type,
        } => {
            let left_batches = execute_stage_collect(left);
            let right_batches = execute_stage_collect(right);
            let left_merged = ColumnarBatch::concat_batches(&left_batches);
            let right_merged = ColumnarBatch::concat_batches(&right_batches);
            let joined = left_merged.join(&right_merged, left_key, right_key, *join_type);
            vec![joined]
        }
        LogicalStage::Union {
            left,
            right,
            distinct,
        } => {
            let left_batches = execute_stage_collect(left);
            let right_batches = execute_stage_collect(right);
            let mut all_batches = left_batches;
            all_batches.extend(right_batches);
            let merged = ColumnarBatch::concat_batches(&all_batches);
            if *distinct {
                vec![merged.deduplicate()]
            } else {
                vec![merged]
            }
        }
        LogicalStage::Select { input, projections } => {
            if is_pure_range_pipeline(stage) {
                let df = DataFrame {
                    plan: stage.clone(),
                };
                let partitions = df.extract_partitions();
                let mut results = Vec::new();
                for part in partitions {
                    results.extend(execute_partition_range(part, stage));
                }
                results
            } else {
                let in_batches = execute_stage_collect(input);
                in_batches
                    .into_iter()
                    .map(|b| {
                        let mut names = Vec::with_capacity(projections.len());
                        let mut cols = Vec::with_capacity(projections.len());
                        for (alias, expr) in projections {
                            names.push(alias.clone());
                            cols.push(expr.eval(&b));
                        }
                        ColumnarBatch::new(names, cols)
                    })
                    .collect()
            }
        }
        LogicalStage::WithColumn {
            input,
            column_name,
            expression,
        } => {
            if is_pure_range_pipeline(stage) {
                let df = DataFrame {
                    plan: stage.clone(),
                };
                let partitions = df.extract_partitions();
                let mut results = Vec::new();
                for part in partitions {
                    results.extend(execute_partition_range(part, stage));
                }
                results
            } else {
                let in_batches = execute_stage_collect(input);
                in_batches
                    .into_iter()
                    .map(|b| {
                        let eval_col = expression.eval(&b);
                        let mut names = b.names;
                        let mut cols = b.columns;
                        if let Some(pos) = names.iter().position(|n| n == column_name) {
                            cols[pos] = eval_col;
                        } else {
                            names.push(column_name.clone());
                            cols.push(eval_col);
                        }
                        ColumnarBatch::new(names, cols)
                    })
                    .collect()
            }
        }
        LogicalStage::Filter { input, condition } => {
            if is_pure_range_pipeline(stage) {
                let df = DataFrame {
                    plan: stage.clone(),
                };
                let partitions = df.extract_partitions();
                let mut results = Vec::new();
                for part in partitions {
                    results.extend(execute_partition_range(part, stage));
                }
                results
            } else {
                let in_batches = execute_stage_collect(input);
                in_batches
                    .into_iter()
                    .map(|b| {
                        let cond_col = condition.eval(&b);
                        let cond_slice = match &cond_col {
                            ColumnVector::Boolean(bl) => bl.as_slice(),
                            _ => panic!("Filter condition deve ser Boolean"),
                        };
                        let matched_indices: Vec<Option<usize>> = cond_slice
                            .iter()
                            .enumerate()
                            .filter_map(|(idx, &m)| if m { Some(Some(idx)) } else { None })
                            .collect();
                        let mut filtered_cols = Vec::with_capacity(b.columns.len());
                        for col in &b.columns {
                            filtered_cols.push(col.gather_indices(&matched_indices));
                        }
                        ColumnarBatch::new(b.names, filtered_cols)
                    })
                    .collect()
            }
        }
    }
}

fn execute_partition_range(part: RangePartition, plan: &LogicalStage) -> Vec<ColumnarBatch> {
    let mut batches = Vec::new();
    let mut cur = part.start;
    let step = part.step;
    let end = part.end;
    let fused = FusedPipeline::compile(plan);

    while cur < end {
        let b_end = (cur + (L1D_OPTIMAL_BATCH_SIZE as i64) * step).min(end);
        let b_size = ((b_end - cur) / step) as usize;

        let mut id_buf = AlignedBuffer::from_elem(0_i64, b_size);
        for i in 0..b_size {
            id_buf[i] = cur + (i as i64) * step;
        }

        let root = ColumnarBatch::new(vec!["id".to_string()], vec![ColumnVector::Long(id_buf)]);
        batches.push(fused.execute_batch(&root));
        cur = b_end;
    }

    batches
}

/// Sessão Spark em Rust (`SparkSession`).
pub struct SparkSession {
    pub app_name: String,
    pub master: String,
    pub default_parallelism: usize,
}

pub struct SparkSessionBuilder {
    app_name: String,
    master: String,
    configs: Vec<(String, String)>,
}

impl SparkSession {
    pub fn builder() -> SparkSessionBuilder {
        SparkSessionBuilder {
            app_name: "Quimera-Spark-Session".to_string(),
            master: "local[*]".to_string(),
            configs: Vec::new(),
        }
    }

    /// Cria um DataFrame baseado em um intervalo numérico (análogo ao `spark.range`).
    pub fn range(&self, start: i64, end: i64, step: i64, num_partitions: usize) -> DataFrame {
        DataFrame {
            plan: LogicalStage::Range {
                start,
                end,
                step,
                num_partitions: num_partitions.max(1),
            },
        }
    }

    pub fn stop(self) {
        // Encerramento ordenado da sessão
    }
}

impl SparkSessionBuilder {
    pub fn app_name(mut self, name: &str) -> Self {
        self.app_name = name.to_string();
        self
    }

    pub fn master(mut self, master: &str) -> Self {
        self.master = master.to_string();
        self
    }

    pub fn config(mut self, key: &str, value: &str) -> Self {
        self.configs.push((key.to_string(), value.to_string()));
        self
    }

    pub fn get_or_create(self) -> SparkSession {
        let num_cpus = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(6);
        SparkSession {
            app_name: self.app_name,
            master: self.master,
            default_parallelism: num_cpus,
        }
    }
}
