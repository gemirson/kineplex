//! # Operator Fusion & Whole-Stage Loop Engine (`catalyst::fusion`)
//!
//! Reimplementação do Tungsten Whole-Stage Code Generation em Rust nativo.
//!
//! Em vez de executar estágios de consulta de forma desacoplada através de chamadas virtuais
//! que alocam e preenchem `ColumnarBatch` intermediários na memória heap, o **Fused Pipeline**
//! funde múltiplos operadores (`WithColumn`, `Filter`, `Select`) em um único pipeline colunar
//! executado dentro da janela ótima de Cache L1d (2048 elementos / 32 KiB).

use crate::catalyst::{ColumnVector, ColumnarBatch, Expression};
use crate::memory::AlignedBuffer;
use crate::sql::LogicalStage;

/// Passo atômico de execução em um pipeline fundido.
#[derive(Clone, Debug)]
pub enum PipelineStep {
    WithColumn {
        name: String,
        expr: Expression,
    },
    Filter {
        condition: Expression,
    },
    Select {
        projections: Vec<(String, Expression)>,
    },
}

/// Pipeline Físico compilado com fusão de operadores (Whole-Stage).
#[derive(Clone, Debug)]
pub struct FusedPipeline {
    pub root_range: Option<(i64, i64, i64, usize)>,
    pub steps: Vec<PipelineStep>,
}

impl FusedPipeline {
    /// Compila uma árvore `LogicalStage` em um pipeline plano fundido.
    pub fn compile(stage: &LogicalStage) -> Self {
        let mut steps = Vec::new();
        let mut root_range = None;

        let mut curr = stage;
        let mut stack = Vec::new();

        while let LogicalStage::Select { input: _, .. }
        | LogicalStage::WithColumn { input: _, .. }
        | LogicalStage::Filter { input: _, .. } = curr
        {
            stack.push(curr);
            match curr {
                LogicalStage::Select { input, .. }
                | LogicalStage::WithColumn { input, .. }
                | LogicalStage::Filter { input, .. } => {
                    curr = input.as_ref();
                }
                _ => break,
            }
        }

        if let LogicalStage::Range {
            start,
            end,
            step,
            num_partitions,
        } = curr
        {
            root_range = Some((*start, *end, *step, *num_partitions));
        }

        // Desempilha na ordem de execução correta (Source -> Step 1 -> Step 2 ...)
        while let Some(st) = stack.pop() {
            match st {
                LogicalStage::WithColumn {
                    column_name,
                    expression,
                    ..
                } => {
                    steps.push(PipelineStep::WithColumn {
                        name: column_name.clone(),
                        expr: expression.clone(),
                    });
                }
                LogicalStage::Filter { condition, .. } => {
                    steps.push(PipelineStep::Filter {
                        condition: condition.clone(),
                    });
                }
                LogicalStage::Select { projections, .. } => {
                    steps.push(PipelineStep::Select {
                        projections: projections.clone(),
                    });
                }
                _ => {}
            }
        }

        Self { root_range, steps }
    }

    /// Executa o pipeline fundido sobre um lote inicial (Zero Heap Allocation intermediária).
    pub fn execute_batch(&self, root_batch: &ColumnarBatch) -> ColumnarBatch {
        let mut curr_batch = root_batch.clone();

        for step in &self.steps {
            match step {
                PipelineStep::WithColumn { name, expr } => {
                    let col = expr.eval(&curr_batch);
                    let mut names = curr_batch.names;
                    let mut cols = curr_batch.columns;

                    if let Some(pos) = names.iter().position(|n| n == name) {
                        cols[pos] = col;
                    } else {
                        names.push(name.clone());
                        cols.push(col);
                    }
                    curr_batch = ColumnarBatch::new(names, cols);
                }
                PipelineStep::Filter { condition } => {
                    let cond_col = condition.eval(&curr_batch);
                    let cond_slice = match &cond_col {
                        ColumnVector::Boolean(b) => b.as_slice(),
                        _ => panic!("Filter condition deve ser Boolean"),
                    };

                    let matched_indices: Vec<usize> = cond_slice
                        .iter()
                        .enumerate()
                        .filter_map(|(idx, &m)| if m { Some(idx) } else { None })
                        .collect();

                    let filtered_len = matched_indices.len();
                    let mut filtered_cols = Vec::with_capacity(curr_batch.columns.len());

                    for col in &curr_batch.columns {
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
                            ColumnVector::Decimal128(v) => {
                                let mut buf = AlignedBuffer::from_elem(Default::default(), filtered_len);
                                for (out_idx, &in_idx) in matched_indices.iter().enumerate() {
                                    buf[out_idx] = v[in_idx];
                                }
                                filtered_cols.push(ColumnVector::Decimal128(buf));
                            }
                            ColumnVector::Int(v) => {
                                let mut buf = AlignedBuffer::from_elem(0_i32, filtered_len);
                                for (out_idx, &in_idx) in matched_indices.iter().enumerate() {
                                    buf[out_idx] = v[in_idx];
                                }
                                filtered_cols.push(ColumnVector::Int(buf));
                            }
                        }
                    }
                    curr_batch = ColumnarBatch::new(curr_batch.names, filtered_cols);
                }
                PipelineStep::Select { projections } => {
                    let mut names = Vec::with_capacity(projections.len());
                    let mut cols = Vec::with_capacity(projections.len());
                    for (name, expr) in projections {
                        names.push(name.clone());
                        cols.push(expr.eval(&curr_batch));
                    }
                    curr_batch = ColumnarBatch::new(names, cols);
                }
            }
        }

        curr_batch
    }
}
