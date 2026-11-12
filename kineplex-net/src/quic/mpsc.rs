use tokio::sync::mpsc::{channel, Receiver, Sender};

/// Canal MPSC para transmissão de atualizações de setpoint ótimo r* em tempo real
pub struct SetpointChannel {
    tx: Sender<f64>,
    rx: Receiver<f64>,
}

impl SetpointChannel {
    pub fn new(buffer: usize) -> Self {
        let (tx, rx) = channel(buffer);
        Self { tx, rx }
    }

    pub fn split(self) -> (SetpointSender, SetpointReceiver) {
        (
            SetpointSender { tx: self.tx },
            SetpointReceiver { rx: self.rx },
        )
    }
}

#[derive(Clone)]
pub struct SetpointSender {
    tx: Sender<f64>,
}

impl SetpointSender {
    /// Envia um novo setpoint r* sem bloquear a thread analítica
    pub fn send_target_r(&self, target_r: f64) -> Result<(), String> {
        self.tx
            .try_send(target_r)
            .map_err(|e| format!("Falha ao enviar setpoint r*: {}", e))
    }
}

pub struct SetpointReceiver {
    rx: Receiver<f64>,
}

impl SetpointReceiver {
    /// Drena o setpoint mais recente pendente no canal (se houver)
    pub fn try_recv_latest(&mut self) -> Option<f64> {
        let mut latest = None;
        while let Ok(val) = self.rx.try_recv() {
            latest = Some(val);
        }
        latest
    }
}
