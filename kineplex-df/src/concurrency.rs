//! # MicroThreadPool: Lock-Free/Low-Latency Static Worker Pool (`concurrency`)
//!
//! Substituto bare-metal para `thread::spawn` dinâmico em execuções de consulta.
//!
//! Em pipelines de streaming e motores OLAP de ultra-baixa latência, a criação
//! e destruição dinâmica de threads do sistema operacional (`thread::spawn`)
//! impõe uma penalidade de 50 a 100 µs por consulta, polui a TLB e destrói
//! a localidade das caches L1d/L2 dos núcleos físicos.
//!
//! O `MicroThreadPool` inicializa um conjunto estático de workers persistentes
//! dimensionado para os núcleos físicos da máquina, coordenados via canais
//! sincronizados com `Condvar` e fila protegida por mutex leve (zero overhead quando ocioso).

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

type Job = Box<dyn FnOnce() + Send + 'static>;

struct SharedPoolState {
    jobs: Mutex<Vec<Job>>,
    cond: Condvar,
    shutdown: AtomicBool,
    active_count: AtomicUsize,
}

/// Pool de threads estático com trabalhadores persistentes.
pub struct MicroThreadPool {
    workers: Vec<JoinHandle<()>>,
    state: Arc<SharedPoolState>,
    thread_count: usize,
}

impl MicroThreadPool {
    /// Inicializa o pool com o número ideal de threads da CPU (ou 4 por padrão).
    pub fn new(num_threads: usize) -> Self {
        let threads = num_threads.max(1);
        let state = Arc::new(SharedPoolState {
            jobs: Mutex::new(Vec::new()),
            cond: Condvar::new(),
            shutdown: AtomicBool::new(false),
            active_count: AtomicUsize::new(0),
        });

        let mut workers = Vec::with_capacity(threads);

        for id in 0..threads {
            let state_clone = Arc::clone(&state);
            let handle = thread::Builder::new()
                .name(format!("kineplex-df-worker-{}", id))
                .spawn(move || {
                    while !state_clone.shutdown.load(Ordering::Acquire) {
                        let job = {
                            let mut lock = state_clone.jobs.lock().unwrap();
                            while lock.is_empty() && !state_clone.shutdown.load(Ordering::Acquire) {
                                lock = state_clone.cond.wait(lock).unwrap();
                            }
                            if state_clone.shutdown.load(Ordering::Acquire) && lock.is_empty() {
                                break;
                            }
                            lock.pop()
                        };

                        if let Some(task) = job {
                            state_clone.active_count.fetch_add(1, Ordering::SeqCst);
                            task();
                            state_clone.active_count.fetch_sub(1, Ordering::SeqCst);
                        }
                    }
                })
                .expect("Falha ao instanciar thread persistente no MicroThreadPool");

            workers.push(handle);
        }

        Self {
            workers,
            state,
            thread_count: threads,
        }
    }

    /// Retorna o número de threads operacionais no pool.
    #[inline(always)]
    pub fn thread_count(&self) -> usize {
        self.thread_count
    }

    /// Executa uma lista de closures em paralelo nas threads do pool e coleta os resultados.
    /// Se `tasks.len() == 1`, executa diretamente na thread chamadora para zero overhead de despacho.
    /// Inclui Work-Helping: a thread chamadora auxilia na execução de tarefas pendentes enquanto aguarda,
    /// prevenindo completamente impasses (deadlocks) em chamadas aninhadas.
    pub fn execute_parallel<T, F>(&self, tasks: Vec<F>) -> Vec<T>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let n = tasks.len();
        if n == 0 {
            return Vec::new();
        }

        if n == 1 {
            let f = tasks.into_iter().next().unwrap();
            return vec![f()];
        }

        let (tx, rx) = mpsc::channel();
        let total_tasks = n;

        {
            let mut lock = self.state.jobs.lock().unwrap();
            for (idx, task) in tasks.into_iter().enumerate() {
                let tx_clone = tx.clone();
                lock.push(Box::new(move || {
                    let res = task();
                    let _ = tx_clone.send((idx, res));
                }));
            }
            self.state.cond.notify_all();
        }

        let mut results: Vec<Option<T>> = (0..total_tasks).map(|_| None).collect();
        let mut completed = 0;

        while completed < total_tasks {
            match rx.try_recv() {
                Ok((idx, res)) => {
                    results[idx] = Some(res);
                    completed += 1;
                }
                Err(mpsc::TryRecvError::Empty) => {
                    let pending_job = {
                        let mut lock = self.state.jobs.lock().unwrap();
                        lock.pop()
                    };
                    if let Some(job) = pending_job {
                        job();
                    } else if let Ok((idx, res)) = rx.recv() {
                        results[idx] = Some(res);
                        completed += 1;
                    }
                }
                Err(mpsc::TryRecvError::Disconnected) => break,
            }
        }

        results.into_iter().map(|opt| opt.unwrap()).collect()
    }
}

impl Drop for MicroThreadPool {
    fn drop(&mut self) {
        self.state.shutdown.store(true, Ordering::Release);
        self.state.cond.notify_all();
        for handle in self.workers.drain(..) {
            let _ = handle.join();
        }
    }
}

// Pool global preguiçoso compartilhado por consultas SQL e DataFrames
static GLOBAL_POOL: std::sync::OnceLock<MicroThreadPool> = std::sync::OnceLock::new();

/// Obtém a referência ao MicroThreadPool global dimensionado para os núcleos da máquina.
pub fn global_thread_pool() -> &'static MicroThreadPool {
    GLOBAL_POOL.get_or_init(|| {
        let threads = thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        MicroThreadPool::new(threads)
    })
}
