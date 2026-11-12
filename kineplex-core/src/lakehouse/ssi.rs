use crate::lakehouse::monoid::UnifiedDataFileAction;

/// Valida em O(K) se uma transação concorrente T_ext invalida o predicado de leitura de T_own
/// utilizando o Teorema do Diamante de Isolamento Serializável (SSI).
/// Se retornar `true`, as transações comutam: R(T_own) \cap W(T_ext) = \emptyset.
pub fn can_commute_concurrent_commit(
    concurrent_added_files: &[UnifiedDataFileAction],
    read_predicate_min: &[f64],
    read_predicate_max: &[f64],
) -> bool {
    !concurrent_added_files
        .iter()
        .any(|file| file.intersects_hyper_rectangle(read_predicate_min, read_predicate_max))
}
