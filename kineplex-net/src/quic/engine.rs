use crate::quic::arena::{RegisteredMemoryArena, DEFAULT_ARENA_SIZE, DEFAULT_SLOT_SIZE};
use crate::quic::error::QuicTransportError;
use crate::quic::metric::HybridPressureMetric;
use crate::quic::pid::{ClosedLoopPidController, PidCycleOutput};
use crate::quic::pid_params::PidControlParameters;
use io_uring::{opcode, types, IoUring};
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub const DEFAULT_RING_ENTRIES: u32 = 1024;
pub const DEFAULT_GSO_SEGMENT_SIZE: u16 = 1350;

/// Configuração do Motor io_uring + QUIC
#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub ring_entries: u32,
    pub enable_sqpoll: bool,
    pub sqpoll_idle_ms: u32,
    pub gso_segment_size: u16,
    pub pid_params: PidControlParameters,
    pub target_setpoint_r: f64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            ring_entries: DEFAULT_RING_ENTRIES,
            enable_sqpoll: true,
            sqpoll_idle_ms: 2000,
            gso_segment_size: DEFAULT_GSO_SEGMENT_SIZE,
            pid_params: PidControlParameters::default(),
            target_setpoint_r: 0.25,
        }
    }
}

/// Motor de submissão e conclusão de I/O de alta velocidade
pub struct IoUringQuicEngine {
    ring: IoUring,
    arena: Arc<RegisteredMemoryArena>,
    socket_fd: RawFd,
    is_sqpoll_active: bool,
    config: EngineConfig,
    pid_controller: ClosedLoopPidController,
    inflight_count: u32,
    total_submissions: AtomicU64,
    total_completions: AtomicU64,
    min_rtt_micros: u64,
}

impl IoUringQuicEngine {
    /// Inicializa o motor com tentativa de SQPOLL, fallback gracioso e registro de descritores e buffers
    pub fn new(
        socket_fd: RawFd,
        config: EngineConfig,
    ) -> Result<Self, QuicTransportError> {
        let arena = Arc::new(RegisteredMemoryArena::new(DEFAULT_ARENA_SIZE, DEFAULT_SLOT_SIZE)?);

        // 1. Tentativa de inicialização com IORING_SETUP_SQPOLL
        let (ring, is_sqpoll_active) = if config.enable_sqpoll {
            match IoUring::builder()
                .setup_sqpoll(config.sqpoll_idle_ms)
                .build(config.ring_entries)
            {
                Ok(r) => (r, true),
                Err(e) => {
                    // Fallback gracioso para modo sem SQPOLL (ex: contêiner ou sem CAP_SYS_ADMIN)
                    eprintln!(
                        "Aviso: IORING_SETUP_SQPOLL indisponível ({}), utilizando modo padrão de anel",
                        e
                    );
                    let fallback_ring = IoUring::new(config.ring_entries)
                        .map_err(|err| QuicTransportError::IoUringInitFailed(err.to_string()))?;
                    (fallback_ring, false)
                }
            }
        } else {
            let r = IoUring::new(config.ring_entries)
                .map_err(|e| QuicTransportError::IoUringInitFailed(e.to_string()))?;
            (r, false)
        };

        let submitter = ring.submitter();

        // 2. Registro de Descritores de Arquivo: Fixed(0)
        let fds = [socket_fd];
        let _ = submitter.register_files(&fds);

        // 3. Registro de Buffers de Memória da Arena
        let iov = libc::iovec {
            iov_base: arena.raw_ptr() as *mut libc::c_void,
            iov_len: arena.total_bytes(),
        };
        unsafe {
            let _ = submitter.register_buffers(&[iov]);
        }

        let pid_controller = ClosedLoopPidController::new(config.pid_params, config.target_setpoint_r);

        Ok(Self {
            ring,
            arena,
            socket_fd,
            is_sqpoll_active,
            config,
            pid_controller,
            inflight_count: 0,
            total_submissions: AtomicU64::new(0),
            total_completions: AtomicU64::new(0),
            min_rtt_micros: 500, // 500 us VPC default
        })
    }

    #[inline(always)]
    pub fn is_sqpoll(&self) -> bool {
        self.is_sqpoll_active
    }

    #[inline(always)]
    pub fn socket_fd(&self) -> RawFd {
        self.socket_fd
    }

    #[inline(always)]
    pub fn arena(&self) -> &Arc<RegisteredMemoryArena> {
        &self.arena
    }

    #[inline(always)]
    pub fn inflight(&self) -> u32 {
        self.inflight_count
    }

    #[inline(always)]
    pub fn pid(&self) -> &ClosedLoopPidController {
        &self.pid_controller
    }

    #[inline(always)]
    pub fn pid_mut(&mut self) -> &mut ClosedLoopPidController {
        &mut self.pid_controller
    }

    /// Atualiza o setpoint r* do controlador PID sem derivative kick
    #[inline(always)]
    pub fn set_target_setpoint(&mut self, new_r: f64) {
        self.pid_controller.update_setpoint(new_r);
    }

    /// Executa a colheita de CQEs e atualiza o estado de inflight e PID
    pub fn reap_completions(&mut self, smoothed_rtt_micros: u64) -> (u32, PidCycleOutput) {
        let mut completed = 0u32;
        {
            let mut cq = self.ring.completion();
            while let Some(cqe) = cq.next() {
                completed += 1;
                // user_data carrega o slot_idx
                let user_data = cqe.user_data();
                if user_data < self.arena.total_slots() as u64 {
                    self.arena.release_slot(user_data as usize);
                }
            }
        }

        self.inflight_count = self.inflight_count.saturating_sub(completed);
        self.total_completions.fetch_add(completed as u64, Ordering::Relaxed);

        if smoothed_rtt_micros > 0 && smoothed_rtt_micros < self.min_rtt_micros {
            self.min_rtt_micros = smoothed_rtt_micros;
        }

        // Amostragem e cálculo do PID
        let metric = HybridPressureMetric::compute(
            self.config.pid_params.alpha_rtt,
            smoothed_rtt_micros,
            self.min_rtt_micros,
            self.inflight_count,
            self.config.ring_entries,
        );

        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let cycle = self.pid_controller.step(now_nanos, &metric);
        (completed, cycle)
    }

    /// Submete um super-buffer GSO de até 64 KB utilizando WriteFixed no socket registrado
    pub fn submit_gso_super_buffer(
        &mut self,
        slot_idx: usize,
        data_len: usize,
    ) -> Result<u32, QuicTransportError> {
        if slot_idx >= self.arena.total_slots() {
            return Err(QuicTransportError::BufferRegistrationFailed(
                "Índice de slot fora dos limites da arena".to_string(),
            ));
        }

        let slot_offset = (slot_idx * self.arena.slot_size()) as u64;
        let buf_ptr = unsafe { self.arena.raw_ptr().add(slot_offset as usize) };

        // Prepara SQE com opcode::WriteFixed no Fixed(0)
        let write_e = opcode::WriteFixed::new(
            types::Fixed(0),
            buf_ptr,
            data_len as u32,
            0, // buffer_index 0 da tabela registrada
        )
        .build()
        .user_data(slot_idx as u64);

        unsafe {
            if self.ring.submission().push(&write_e).is_err() {
                return Err(QuicTransportError::QueueFull);
            }
        }

        self.inflight_count += 1;
        self.total_submissions.fetch_add(1, Ordering::Relaxed);

        // Se SQPOLL estiver ativo e o kernel não estiver dormindo, não precisamos de io_uring_enter!
        if !self.is_sqpoll_active {
            let submitted = self.ring.submit()
                .map_err(|e| QuicTransportError::IoUringInitFailed(e.to_string()))?;
            Ok(submitted as u32)
        } else {
            Ok(1)
        }
    }
}
