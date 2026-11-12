/// Ação atômica unificada gerada em memória ao concluir a escrita de uma partição Parquet de 128 MB
#[derive(Clone, Debug, PartialEq)]
pub struct UnifiedDataFileAction {
    pub relative_path: String,
    pub file_size_bytes: u64,
    pub record_count: u64,
    pub min_values: Vec<f64>, // \vec{\ell}_k para detecção de conflitos SSI e Data Skipping
    pub max_values: Vec<f64>, // \vec{u}_k
    pub null_counts: Vec<u64>,
    pub modification_time_ms: i64,
}

impl UnifiedDataFileAction {
    pub fn new(
        relative_path: String,
        file_size_bytes: u64,
        record_count: u64,
        min_values: Vec<f64>,
        max_values: Vec<f64>,
        null_counts: Vec<u64>,
        modification_time_ms: i64,
    ) -> Self {
        Self {
            relative_path,
            file_size_bytes,
            record_count,
            min_values,
            max_values,
            null_counts,
            modification_time_ms,
        }
    }

    /// Projeta a ação do monoide diretamente para o formato JSON do _delta_log (Delta Lake Protocol)
    pub fn to_delta_log_json_line(&self) -> String {
        format!(
            concat!(
                r#"{{"add":{{"path":"{}","size":{},"modificationTime":{},"dataChange":true,"#,
                r#""stats":"{{\"numRecords\":{},\"minValues\":{:?},\"maxValues\":{:?},\"nullCount\":{:?}}}"}}}}"#
            ),
            self.relative_path,
            self.file_size_bytes,
            self.modification_time_ms,
            self.record_count,
            self.min_values,
            self.max_values,
            self.null_counts
        )
    }

    /// Verifica a Propriedade de Diamante (Comutatividade SSI) entre dois hiper-retângulos em R^C
    /// Se retornar `false`, os arquivos são disjuntos e o commit concorrente comuta sem re-execução!
    #[inline(always)]
    pub fn intersects_hyper_rectangle(
        &self,
        query_min: &[f64],
        query_max: &[f64],
    ) -> bool {
        self.min_values
            .iter()
            .zip(self.max_values.iter())
            .zip(query_min.iter().zip(query_max.iter()))
            .all(|((&f_min, &f_max), (&q_min, &q_max))| f_min <= q_max && f_max >= q_min)
    }
}
