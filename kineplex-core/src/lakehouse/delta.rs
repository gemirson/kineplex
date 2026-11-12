use crate::lakehouse::monoid::UnifiedDataFileAction;

/// Serializador de ações do monoide para o log linear de transações do Delta Lake
pub struct DeltaLogSerializer;

impl DeltaLogSerializer {
    /// Converte um conjunto de ações em linhas formatadas para o arquivo _delta_log/{version}.json
    pub fn serialize_actions(actions: &[UnifiedDataFileAction]) -> Vec<String> {
        actions.iter().map(|a| a.to_delta_log_json_line()).collect()
    }
}
