//! Asynchronous UDP transport for Foca's SWIM membership protocol.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::pending;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use bytes::Buf;
use foca::{
    BroadcastHandler, Config, Foca, Identity, Invalidates, Notification, PostcardCodec, Runtime,
    Timer,
};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};

use crate::telemetry::{
    NodeTelemetry, TelemetryCollector, TelemetryDecodeError, NODE_TELEMETRY_ENCODED_LEN,
};

/// Port offset applied to a node endpoint to obtain its Gossip endpoint.
pub const GOSSIP_PORT_OFFSET: u16 = 1;

const EXPECTED_CLUSTER_SIZE: u32 = 64;
const DEFAULT_TOPOLOGY_LOG_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_TELEMETRY_INTERVAL: Duration = Duration::from_secs(2);
const PAYLOAD_VERSION: u8 = 2;
const PAYLOAD_HEADER_LENGTH: usize = 36;
const PAYLOAD_LENGTH: usize = PAYLOAD_HEADER_LENGTH + NODE_TELEMETRY_ENCODED_LEN;
const EVENT_CHANNEL_CAPACITY: usize = 128;
const TELEMETRY_CHANNEL_CAPACITY: usize = 1;

/// Converts a node endpoint into its dedicated Gossip endpoint.
///
/// # Errors
///
/// Returns [`GossipPortError`] when adding [`GOSSIP_PORT_OFFSET`] would overflow
/// the `u16` port range.
pub fn gossip_addr(mut node_addr: SocketAddr) -> Result<SocketAddr, GossipPortError> {
    let gossip_port = node_addr
        .port()
        .checked_add(GOSSIP_PORT_OFFSET)
        .ok_or(GossipPortError { node_addr })?;
    node_addr.set_port(gossip_port);
    Ok(node_addr)
}

/// Error returned when a node port cannot reserve the adjacent Gossip port.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GossipPortError {
    node_addr: SocketAddr,
}

impl Display for GossipPortError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "node endpoint {} cannot reserve Gossip port +{}",
            self.node_addr, GOSSIP_PORT_OFFSET
        )
    }
}

impl Error for GossipPortError {}

/// Renewable cluster identity carried by every Foca protocol message.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GossipIdentity {
    addr: SocketAddr,
    generation: u64,
}

impl GossipIdentity {
    fn new(addr: SocketAddr, generation: u64) -> Self {
        Self { addr, generation }
    }

    fn seed(addr: SocketAddr) -> Self {
        Self::new(addr, 0)
    }

    /// Returns the UDP Gossip endpoint represented by this identity.
    #[must_use]
    pub const fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Returns the identity generation used to distinguish process restarts.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }
}

impl Identity for GossipIdentity {
    fn renew(&self) -> Option<Self> {
        Some(Self::new(self.addr, self.generation.wrapping_add(1)))
    }

    fn has_same_prefix(&self, other: &Self) -> bool {
        self.addr == other.addr
    }
}

/// Versioned custom payload disseminated alongside SWIM membership updates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TopologyPayload {
    node_addr: SocketAddr,
    generation: u64,
    sequence: u64,
    telemetry: NodeTelemetry,
}

impl TopologyPayload {
    /// Creates topology metadata for one running node generation.
    #[must_use]
    pub const fn new(
        node_addr: SocketAddr,
        generation: u64,
        sequence: u64,
        telemetry: NodeTelemetry,
    ) -> Self {
        Self {
            node_addr,
            generation,
            sequence,
            telemetry,
        }
    }

    /// Returns the node's Gossip endpoint.
    #[must_use]
    pub const fn node_addr(&self) -> SocketAddr {
        self.node_addr
    }

    /// Returns the monotonic generation associated with the endpoint.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the revision of this generation's telemetry sample.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the compact resource telemetry carried by this payload.
    #[must_use]
    pub const fn telemetry(&self) -> NodeTelemetry {
        self.telemetry
    }

    fn encode(self) -> [u8; PAYLOAD_LENGTH] {
        let mut encoded = [0_u8; PAYLOAD_LENGTH];
        encoded[0] = PAYLOAD_VERSION;
        match self.node_addr.ip() {
            IpAddr::V4(ip) => {
                encoded[1] = 4;
                encoded[2..6].copy_from_slice(&ip.octets());
            }
            IpAddr::V6(ip) => {
                encoded[1] = 6;
                encoded[2..18].copy_from_slice(&ip.octets());
            }
        }
        encoded[18..20].copy_from_slice(&self.node_addr.port().to_be_bytes());
        encoded[20..28].copy_from_slice(&self.generation.to_be_bytes());
        encoded[28..36].copy_from_slice(&self.sequence.to_be_bytes());
        encoded[36..PAYLOAD_LENGTH].copy_from_slice(&self.telemetry.encode());
        encoded
    }

    fn decode(mut data: impl Buf) -> Result<Self, PayloadDecodeError> {
        if data.remaining() < PAYLOAD_LENGTH {
            return Err(PayloadDecodeError::Truncated);
        }

        let mut encoded = [0_u8; PAYLOAD_LENGTH];
        data.copy_to_slice(&mut encoded);
        if encoded[0] != PAYLOAD_VERSION {
            return Err(PayloadDecodeError::UnsupportedVersion(encoded[0]));
        }

        let ip = match encoded[1] {
            4 => IpAddr::V4(Ipv4Addr::new(
                encoded[2], encoded[3], encoded[4], encoded[5],
            )),
            6 => {
                let mut octets = [0_u8; 16];
                octets.copy_from_slice(&encoded[2..18]);
                IpAddr::V6(Ipv6Addr::from(octets))
            }
            family => return Err(PayloadDecodeError::UnsupportedAddressFamily(family)),
        };
        let port = u16::from_be_bytes([encoded[18], encoded[19]]);
        let generation = u64::from_be_bytes([
            encoded[20],
            encoded[21],
            encoded[22],
            encoded[23],
            encoded[24],
            encoded[25],
            encoded[26],
            encoded[27],
        ]);
        let sequence = u64::from_be_bytes([
            encoded[28],
            encoded[29],
            encoded[30],
            encoded[31],
            encoded[32],
            encoded[33],
            encoded[34],
            encoded[35],
        ]);
        let telemetry = NodeTelemetry::decode(&encoded[36..PAYLOAD_LENGTH])
            .map_err(PayloadDecodeError::InvalidTelemetry)?;

        Ok(Self::new(
            SocketAddr::new(ip, port),
            generation,
            sequence,
            telemetry,
        ))
    }
}

#[derive(Debug)]
enum PayloadDecodeError {
    Truncated,
    UnsupportedVersion(u8),
    UnsupportedAddressFamily(u8),
    InvalidTelemetry(TelemetryDecodeError),
}

impl Display for PayloadDecodeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => formatter.write_str("custom Gossip payload is truncated"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported Gossip payload version {version}")
            }
            Self::UnsupportedAddressFamily(family) => {
                write!(formatter, "unsupported Gossip address family {family}")
            }
            Self::InvalidTelemetry(error) => write!(formatter, "invalid node telemetry: {error}"),
        }
    }
}

#[derive(Debug)]
struct PayloadBroadcast {
    payload: TopologyPayload,
    encoded: [u8; PAYLOAD_LENGTH],
}

impl PayloadBroadcast {
    fn new(payload: TopologyPayload) -> Self {
        Self {
            payload,
            encoded: payload.encode(),
        }
    }
}

impl AsRef<[u8]> for PayloadBroadcast {
    fn as_ref(&self) -> &[u8] {
        &self.encoded
    }
}

impl Invalidates for PayloadBroadcast {
    fn invalidates(&self, other: &Self) -> bool {
        self.payload.node_addr == other.payload.node_addr
            && self.payload.generation == other.payload.generation
            && self.payload.sequence >= other.payload.sequence
    }
}

struct PayloadHandler {
    revisions: BTreeMap<(SocketAddr, u64), u64>,
    decoded_payloads: mpsc::UnboundedSender<TopologyPayload>,
}

impl PayloadHandler {
    fn new(decoded_payloads: mpsc::UnboundedSender<TopologyPayload>) -> Self {
        Self {
            revisions: BTreeMap::new(),
            decoded_payloads,
        }
    }
}

impl BroadcastHandler<GossipIdentity> for PayloadHandler {
    type Broadcast = PayloadBroadcast;
    type Error = PayloadDecodeError;

    fn receive_item(&mut self, data: impl Buf) -> Result<Option<Self::Broadcast>, Self::Error> {
        let payload = TopologyPayload::decode(data)?;
        let revision_key = (payload.node_addr, payload.generation);
        let is_new = self
            .revisions
            .get(&revision_key)
            .map_or(true, |sequence| payload.sequence > *sequence);

        if is_new {
            self.revisions.insert(revision_key, payload.sequence);
            let _send_result = self.decoded_payloads.send(payload);
            Ok(Some(PayloadBroadcast::new(payload)))
        } else {
            Ok(None)
        }
    }
}

/// Runtime settings for one asynchronous Gossip participant.
#[derive(Clone, Debug)]
pub struct GossipConfig {
    /// Local UDP endpoint on which Gossip packets are received.
    pub bind_addr: SocketAddr,
    /// Reachable UDP endpoint advertised to other cluster members.
    pub advertise_addr: SocketAddr,
    /// Reachable Gossip endpoints used for initial cluster discovery.
    pub seeds: Vec<SocketAddr>,
    /// Interval between structured topology snapshots.
    pub topology_log_interval: Duration,
    /// Interval between local CPU and memory telemetry samples.
    pub telemetry_interval: Duration,
}

impl GossipConfig {
    /// Creates Gossip settings with a 30-second topology logging interval.
    #[must_use]
    pub fn new(bind_addr: SocketAddr, advertise_addr: SocketAddr, seeds: Vec<SocketAddr>) -> Self {
        Self {
            bind_addr,
            advertise_addr,
            seeds,
            topology_log_interval: DEFAULT_TOPOLOGY_LOG_INTERVAL,
            telemetry_interval: DEFAULT_TELEMETRY_INTERVAL,
        }
    }

    /// Overrides how often known and active topology state is logged.
    #[must_use]
    pub const fn with_topology_log_interval(mut self, interval: Duration) -> Self {
        self.topology_log_interval = interval;
        self
    }

    /// Overrides how often local CPU and memory telemetry is sampled.
    #[must_use]
    pub const fn with_telemetry_interval(mut self, interval: Duration) -> Self {
        self.telemetry_interval = interval;
        self
    }
}

/// Membership transition emitted by the asynchronous Gossip service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MembershipEvent {
    /// A node became active and reachable in the SWIM topology.
    MemberUp(SocketAddr),
    /// A previously active node was declared down after suspicion elapsed.
    MemberDown(SocketAddr),
}

/// Error returned while starting, running, or stopping Gossip.
#[derive(Debug)]
pub enum GossipError {
    /// An operating-system UDP operation failed.
    Io(io::Error),
    /// The background supervisor task failed or was cancelled unexpectedly.
    Supervisor(tokio::task::JoinError),
    /// A protocol constant could not be represented as a non-zero value.
    InvalidProtocolConfiguration,
    /// The topology logging interval was zero and would make Tokio panic.
    InvalidTopologyLogInterval,
    /// The telemetry collection interval was zero.
    InvalidTelemetryInterval,
    /// The operating system telemetry collector thread could not start.
    TelemetryCollector(io::Error),
    /// The Gossip supervisor is no longer accepting telemetry updates.
    SupervisorUnavailable,
    /// The advertised endpoint used an unspecified, unreachable interface.
    UnspecifiedAdvertiseAddress,
}

impl Display for GossipError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "Gossip UDP I/O failed: {error}"),
            Self::Supervisor(error) => write!(formatter, "Gossip supervisor failed: {error}"),
            Self::InvalidProtocolConfiguration => {
                formatter.write_str("Gossip protocol configuration is invalid")
            }
            Self::InvalidTopologyLogInterval => {
                formatter.write_str("Gossip topology log interval must be greater than zero")
            }
            Self::InvalidTelemetryInterval => {
                formatter.write_str("Gossip telemetry interval must be greater than zero")
            }
            Self::TelemetryCollector(error) => {
                write!(formatter, "telemetry collector failed to start: {error}")
            }
            Self::SupervisorUnavailable => formatter.write_str("Gossip supervisor is unavailable"),
            Self::UnspecifiedAdvertiseAddress => {
                formatter.write_str("Gossip advertise address must be reachable")
            }
        }
    }
}

impl Error for GossipError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) | Self::TelemetryCollector(error) => Some(error),
            Self::Supervisor(error) => Some(error),
            Self::InvalidProtocolConfiguration
            | Self::InvalidTopologyLogInterval
            | Self::InvalidTelemetryInterval
            | Self::SupervisorUnavailable
            | Self::UnspecifiedAdvertiseAddress => None,
        }
    }
}

impl From<io::Error> for GossipError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Running asynchronous Gossip service.
pub struct GossipHandle {
    local_addr: SocketAddr,
    events: broadcast::Sender<MembershipEvent>,
    members: watch::Receiver<Arc<[SocketAddr]>>,
    telemetry: watch::Receiver<Arc<BTreeMap<SocketAddr, NodeTelemetry>>>,
    telemetry_updates: mpsc::Sender<NodeTelemetry>,
    telemetry_collector: TelemetryCollector,
    shutdown: Option<oneshot::Sender<()>>,
    supervisor: JoinHandle<Result<(), GossipError>>,
}

impl GossipHandle {
    /// Returns the UDP endpoint bound by this Gossip service.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Subscribes to future membership transitions.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<MembershipEvent> {
        self.events.subscribe()
    }

    /// Returns a sorted snapshot of all active endpoints, including this node.
    #[must_use]
    pub fn members(&self) -> Arc<[SocketAddr]> {
        self.members.borrow().clone()
    }

    /// Returns the latest decoded telemetry sample for each known endpoint.
    #[must_use]
    pub fn telemetry(&self) -> Arc<BTreeMap<SocketAddr, NodeTelemetry>> {
        self.telemetry.borrow().clone()
    }

    /// Enqueues an explicit local sample for Gossip dissemination.
    ///
    /// This supports future Arrow pool accounting while the built-in collector supplies
    /// the operating-system baseline.
    ///
    /// # Errors
    ///
    /// Returns [`GossipError::SupervisorUnavailable`] after the driver has stopped.
    pub async fn update_telemetry(&self, telemetry: NodeTelemetry) -> Result<(), GossipError> {
        self.telemetry_updates
            .send(telemetry)
            .await
            .map_err(|_| GossipError::SupervisorUnavailable)
    }

    /// Gracefully leaves the cluster and waits for all Gossip resources to stop.
    ///
    /// # Errors
    ///
    /// Returns [`GossipError`] if the supervisor or UDP transport failed.
    pub async fn shutdown(mut self) -> Result<(), GossipError> {
        self.telemetry_collector.stop();
        if let Some(shutdown) = self.shutdown.take() {
            let _shutdown_result = shutdown.send(());
        }
        self.await_supervisor().await
    }

    /// Abruptly stops this participant without broadcasting a leave update.
    ///
    /// This models a process crash and is primarily useful for failure-detector tests.
    pub async fn abort(mut self) {
        self.telemetry_collector.stop();
        self.shutdown.take();
        self.supervisor.abort();
        let _join_result = self.supervisor.await;
    }

    async fn await_supervisor(self) -> Result<(), GossipError> {
        self.supervisor.await.map_err(GossipError::Supervisor)?
    }
}

/// Binds UDP and starts one Foca SWIM participant on a background Tokio task.
///
/// The future returns only after the socket is bound, so the returned handle is ready
/// to receive seed announcements immediately.
///
/// # Errors
///
/// Returns [`GossipError`] when the UDP endpoint cannot be bound or the built-in LAN
/// protocol configuration cannot be constructed.
pub async fn start(mut config: GossipConfig) -> Result<GossipHandle, GossipError> {
    if config.topology_log_interval.is_zero() {
        return Err(GossipError::InvalidTopologyLogInterval);
    }
    if config.telemetry_interval.is_zero() {
        return Err(GossipError::InvalidTelemetryInterval);
    }
    if config.advertise_addr.ip().is_unspecified() {
        return Err(GossipError::UnspecifiedAdvertiseAddress);
    }

    let socket = UdpSocket::bind(config.bind_addr).await?;
    let local_addr = socket.local_addr()?;
    if config.advertise_addr.port() == 0 {
        config.advertise_addr.set_port(local_addr.port());
    }

    let cluster_size =
        NonZeroU32::new(EXPECTED_CLUSTER_SIZE).ok_or(GossipError::InvalidProtocolConfiguration)?;
    let protocol_config = Config::new_lan(cluster_size);
    let receive_buffer_size = protocol_config.max_packet_size.get();

    let mut rng = StdRng::from_entropy();
    let identity = GossipIdentity::new(config.advertise_addr, rng.next_u64());
    let initial_telemetry = NodeTelemetry::default();
    let payload = TopologyPayload::new(identity.addr, identity.generation, 0, initial_telemetry);
    let (decoded_payload_tx, decoded_payload_rx) = mpsc::unbounded_channel();
    let mut foca = Foca::with_custom_broadcast(
        identity.clone(),
        protocol_config,
        rng,
        PostcardCodec,
        PayloadHandler::new(decoded_payload_tx),
    );
    if let Err(error) = foca.add_broadcast(&payload.encode()) {
        tracing::warn!(error = %error, "failed to register local Gossip payload");
    }

    let seeds = config
        .seeds
        .into_iter()
        .filter(|seed| *seed != identity.addr)
        .collect::<BTreeSet<_>>();
    let initial_members: Arc<[SocketAddr]> = Arc::from([identity.addr]);
    let initial_telemetry_by_node = Arc::new(BTreeMap::from([(identity.addr, initial_telemetry)]));
    let (events, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
    let (members_tx, members_rx) = watch::channel(initial_members);
    let (telemetry_state_tx, telemetry_state_rx) = watch::channel(initial_telemetry_by_node);
    let (telemetry_updates_tx, telemetry_updates_rx) = mpsc::channel(TELEMETRY_CHANNEL_CAPACITY);
    let telemetry_collector =
        TelemetryCollector::spawn(config.telemetry_interval, telemetry_updates_tx.clone())
            .map_err(GossipError::TelemetryCollector)?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let supervisor_events = events.clone();
    let supervisor = tokio::spawn(run_supervisor(
        socket,
        foca,
        seeds,
        receive_buffer_size,
        config.topology_log_interval,
        supervisor_events,
        members_tx,
        telemetry_updates_rx,
        decoded_payload_rx,
        telemetry_state_tx,
        shutdown_rx,
    ));

    Ok(GossipHandle {
        local_addr,
        events,
        members: members_rx,
        telemetry: telemetry_state_rx,
        telemetry_updates: telemetry_updates_tx,
        telemetry_collector,
        shutdown: Some(shutdown_tx),
        supervisor,
    })
}

type Swim = Foca<GossipIdentity, PostcardCodec, StdRng, PayloadHandler>;

struct AccumulatingRuntime {
    outgoing: VecDeque<(GossipIdentity, Vec<u8>)>,
    timers: Vec<(Duration, Timer<GossipIdentity>)>,
    notifications: VecDeque<Notification<GossipIdentity>>,
}

impl AccumulatingRuntime {
    fn new() -> Self {
        Self {
            outgoing: VecDeque::new(),
            timers: Vec::new(),
            notifications: VecDeque::new(),
        }
    }
}

impl Runtime<GossipIdentity> for AccumulatingRuntime {
    fn notify(&mut self, notification: Notification<GossipIdentity>) {
        self.notifications.push_back(notification);
    }

    fn send_to(&mut self, destination: GossipIdentity, data: &[u8]) {
        self.outgoing.push_back((destination, data.to_vec()));
    }

    fn submit_after(&mut self, timer: Timer<GossipIdentity>, delay: Duration) {
        self.timers.push((delay, timer));
    }
}

struct ScheduledTimer {
    deadline: Instant,
    event: Timer<GossipIdentity>,
}

struct Membership {
    active_generations: BTreeMap<SocketAddr, usize>,
    known: BTreeSet<SocketAddr>,
}

impl Membership {
    fn new(local_addr: SocketAddr) -> Self {
        Self {
            active_generations: BTreeMap::from([(local_addr, 1)]),
            known: BTreeSet::from([local_addr]),
        }
    }

    fn member_up(&mut self, addr: SocketAddr) -> bool {
        self.known.insert(addr);
        let count = self.active_generations.entry(addr).or_insert(0);
        *count += 1;
        *count == 1
    }

    fn member_down(&mut self, addr: SocketAddr) -> bool {
        let became_inactive = self.active_generations.get_mut(&addr).is_some_and(|count| {
            *count = count.saturating_sub(1);
            *count == 0
        });
        if became_inactive {
            self.active_generations.remove(&addr);
        }
        became_inactive
    }

    fn snapshot(&self) -> Arc<[SocketAddr]> {
        self.active_generations
            .keys()
            .copied()
            .collect::<Vec<_>>()
            .into()
    }

    fn known_count(&self) -> usize {
        self.known.len()
    }

    fn is_active(&self, addr: SocketAddr) -> bool {
        self.active_generations.contains_key(&addr)
    }
}

#[derive(Default)]
struct TelemetryState {
    revisions: BTreeMap<(SocketAddr, u64), u64>,
    current: BTreeMap<SocketAddr, (u64, u64, NodeTelemetry)>,
}

impl TelemetryState {
    fn apply(&mut self, payload: TopologyPayload) -> bool {
        let revision_key = (payload.node_addr, payload.generation);
        if self
            .revisions
            .get(&revision_key)
            .is_some_and(|sequence| *sequence >= payload.sequence)
        {
            return false;
        }
        self.revisions.insert(revision_key, payload.sequence);
        self.current.insert(
            payload.node_addr,
            (payload.generation, payload.sequence, payload.telemetry),
        );
        true
    }

    fn remove(&mut self, addr: SocketAddr) -> bool {
        self.revisions
            .retain(|(node_addr, _), _| *node_addr != addr);
        self.current.remove(&addr).is_some()
    }

    fn snapshot(&self) -> Arc<BTreeMap<SocketAddr, NodeTelemetry>> {
        Arc::new(
            self.current
                .iter()
                .map(|(addr, (_, _, telemetry))| (*addr, *telemetry))
                .collect(),
        )
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_supervisor(
    socket: UdpSocket,
    mut foca: Swim,
    seeds: BTreeSet<SocketAddr>,
    receive_buffer_size: usize,
    topology_log_interval: Duration,
    events: broadcast::Sender<MembershipEvent>,
    members_tx: watch::Sender<Arc<[SocketAddr]>>,
    mut telemetry_updates: mpsc::Receiver<NodeTelemetry>,
    mut decoded_payloads: mpsc::UnboundedReceiver<TopologyPayload>,
    telemetry_tx: watch::Sender<Arc<BTreeMap<SocketAddr, NodeTelemetry>>>,
    mut shutdown: oneshot::Receiver<()>,
) -> Result<(), GossipError> {
    let mut runtime = AccumulatingRuntime::new();
    let mut scheduled_timers = Vec::new();
    let mut membership = Membership::new(foca.identity().addr);
    let mut telemetry_state = TelemetryState::default();
    let mut local_telemetry_sequence = 0_u64;
    let mut receive_buffer = vec![0_u8; receive_buffer_size];
    let mut topology_tick = tokio::time::interval(topology_log_interval);
    topology_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    for seed in seeds {
        handle_protocol_result(
            foca.announce(GossipIdentity::seed(seed), &mut runtime),
            "announce",
        );
    }
    drain_runtime(
        &socket,
        &mut runtime,
        &mut scheduled_timers,
        &mut membership,
        &events,
        &members_tx,
        &mut decoded_payloads,
        &mut telemetry_state,
        &telemetry_tx,
    )
    .await;

    loop {
        let next_timer = next_timer(&scheduled_timers);
        tokio::select! {
            receive_result = socket.recv_from(&mut receive_buffer) => {
                let (received, source) = receive_result?;
                tracing::trace!(bytes = received, %source, "received Gossip datagram");
                handle_protocol_result(
                    foca.handle_data(&receive_buffer[..received], &mut runtime),
                    "handle_data",
                );
            }
            _ = wait_for_timer(next_timer.map(|(_, deadline)| deadline)) => {
                if let Some((index, _)) = next_timer {
                    let timer = scheduled_timers.swap_remove(index).event;
                    handle_protocol_result(foca.handle_timer(timer, &mut runtime), "handle_timer");
                }
            }
            Some(telemetry) = telemetry_updates.recv() => {
                local_telemetry_sequence = local_telemetry_sequence.wrapping_add(1);
                let identity = foca.identity().clone();
                let payload = TopologyPayload::new(
                    identity.addr,
                    identity.generation,
                    local_telemetry_sequence,
                    telemetry,
                );
                handle_protocol_result(
                    foca.add_broadcast(&payload.encode()),
                    "add_telemetry_broadcast",
                );
                if foca.num_members() > 0 {
                    handle_protocol_result(foca.gossip(&mut runtime), "gossip_telemetry");
                }
            }
            _ = topology_tick.tick() => {
                let active = membership.snapshot();
                tracing::info!(
                    known_count = membership.known_count(),
                    active_count = active.len(),
                    telemetry_count = telemetry_state.current.len(),
                    members = ?active,
                    "Gossip topology snapshot"
                );
            }
            _ = &mut shutdown => {
                break;
            }
        }

        drain_runtime(
            &socket,
            &mut runtime,
            &mut scheduled_timers,
            &mut membership,
            &events,
            &members_tx,
            &mut decoded_payloads,
            &mut telemetry_state,
            &telemetry_tx,
        )
        .await;
    }

    handle_protocol_result(foca.leave_cluster(&mut runtime), "leave_cluster");
    drain_runtime(
        &socket,
        &mut runtime,
        &mut scheduled_timers,
        &mut membership,
        &events,
        &members_tx,
        &mut decoded_payloads,
        &mut telemetry_state,
        &telemetry_tx,
    )
    .await;
    Ok(())
}

fn handle_protocol_result(result: Result<(), foca::Error>, operation: &'static str) {
    if let Err(error) = result {
        tracing::warn!(%error, operation, "ignored recoverable Foca protocol error");
    }
}

#[allow(clippy::too_many_arguments)]
async fn drain_runtime(
    socket: &UdpSocket,
    runtime: &mut AccumulatingRuntime,
    scheduled_timers: &mut Vec<ScheduledTimer>,
    membership: &mut Membership,
    events: &broadcast::Sender<MembershipEvent>,
    members_tx: &watch::Sender<Arc<[SocketAddr]>>,
    decoded_payloads: &mut mpsc::UnboundedReceiver<TopologyPayload>,
    telemetry_state: &mut TelemetryState,
    telemetry_tx: &watch::Sender<Arc<BTreeMap<SocketAddr, NodeTelemetry>>>,
) {
    while let Some((destination, packet)) = runtime.outgoing.pop_front() {
        match socket.send_to(&packet, destination.addr).await {
            Ok(sent) => tracing::trace!(
                bytes = sent,
                member = %destination.addr,
                "sent Gossip datagram"
            ),
            Err(error) => tracing::warn!(
                %error,
                member = %destination.addr,
                "failed to send Gossip datagram"
            ),
        }
    }

    scheduled_timers.extend(
        runtime
            .timers
            .drain(..)
            .map(|(delay, event)| ScheduledTimer {
                deadline: Instant::now() + delay,
                event,
            }),
    );

    let mut membership_changed = false;
    let mut telemetry_changed = false;
    while let Some(notification) = runtime.notifications.pop_front() {
        match notification {
            Notification::MemberUp(identity) => {
                if membership.member_up(identity.addr) {
                    membership_changed = true;
                    let _send_result = events.send(MembershipEvent::MemberUp(identity.addr));
                    tracing::info!(member = %identity.addr, "MemberUp");
                }
            }
            Notification::MemberDown(identity) => {
                if membership.member_down(identity.addr) {
                    membership_changed = true;
                    telemetry_changed |= telemetry_state.remove(identity.addr);
                    let _send_result = events.send(MembershipEvent::MemberDown(identity.addr));
                    tracing::info!(member = %identity.addr, "MemberDown");
                }
            }
            Notification::Active => tracing::debug!("Gossip participant joined the cluster"),
            Notification::Idle => tracing::info!("Gossip participant has no active peers"),
            Notification::Defunct => tracing::warn!("Gossip participant was declared down"),
            Notification::Rejoin(identity) => tracing::info!(
                member = %identity.addr,
                generation = identity.generation,
                "Gossip participant renewed its identity"
            ),
        }
    }

    if membership_changed {
        let _previous_snapshot = members_tx.send_replace(membership.snapshot());
    }

    while let Ok(payload) = decoded_payloads.try_recv() {
        if membership.is_active(payload.node_addr) && telemetry_state.apply(payload) {
            telemetry_changed = true;
            tracing::debug!(
                member = %payload.node_addr,
                generation = payload.generation,
                sequence = payload.sequence,
                cpu_utilization_pct = payload.telemetry.cpu_utilization_pct,
                arrow_mem_avail_mb = payload.telemetry.arrow_mem_avail_mb,
                "node telemetry updated"
            );
        }
    }

    if telemetry_changed {
        let _previous_snapshot = telemetry_tx.send_replace(telemetry_state.snapshot());
    }
}

fn next_timer(timers: &[ScheduledTimer]) -> Option<(usize, Instant)> {
    timers
        .iter()
        .enumerate()
        .min_by_key(|(_, timer)| timer.deadline)
        .map(|(index, timer)| (index, timer.deadline))
}

async fn wait_for_timer(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => pending::<()>().await,
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};

    use super::{gossip_addr, PayloadHandler, TopologyPayload};
    use crate::telemetry::NodeTelemetry;
    use foca::BroadcastHandler;
    use tokio::sync::mpsc;

    #[test]
    fn derives_gossip_port_for_ipv4_and_ipv6() {
        let ipv4 = SocketAddr::from(([127, 0, 0, 1], 8000));
        assert_eq!(
            gossip_addr(ipv4),
            Ok(SocketAddr::from(([127, 0, 0, 1], 8001)))
        );

        let ipv6 = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 65534, 7, 3));
        let derived = gossip_addr(ipv6).expect("port 65534 has one adjacent port");
        assert_eq!(derived.port(), 65535);
        let ipv6_metadata = match derived {
            SocketAddr::V6(addr) => Some((addr.flowinfo(), addr.scope_id())),
            SocketAddr::V4(_) => None,
        };
        assert_eq!(ipv6_metadata, Some((7, 3)));
    }

    #[test]
    fn rejects_gossip_port_overflow() {
        let endpoint = SocketAddr::from(([127, 0, 0, 1], u16::MAX));
        assert!(gossip_addr(endpoint).is_err());
    }

    #[test]
    fn custom_payload_round_trips_and_deduplicates() {
        let endpoint = SocketAddr::from(([192, 168, 1, 10], 8001));
        let telemetry = NodeTelemetry::new(42, 16_384);
        let payload = TopologyPayload::new(endpoint, 42, 7, telemetry);
        let encoded = payload.encode();
        assert_eq!(
            TopologyPayload::decode(encoded.as_slice()).expect("valid payload must decode"),
            payload
        );

        let (decoded_tx, mut decoded_rx) = mpsc::unbounded_channel();
        let mut handler = PayloadHandler::new(decoded_tx);
        assert!(handler
            .receive_item(encoded.as_slice())
            .expect("new payload must parse")
            .is_some());
        assert_eq!(
            decoded_rx
                .try_recv()
                .expect("new telemetry must be emitted"),
            payload
        );
        assert!(handler
            .receive_item(encoded.as_slice())
            .expect("duplicate payload must parse")
            .is_none());
    }

    #[test]
    fn custom_payload_validates_wire_format_and_ipv6() {
        assert!(matches!(
            TopologyPayload::decode([].as_slice()),
            Err(super::PayloadDecodeError::Truncated)
        ));

        let payload = TopologyPayload::new(
            "[2001:db8::1]:8001".parse().expect("valid fixture"),
            7,
            11,
            NodeTelemetry::new(55, 8_192),
        );
        let mut encoded = payload.encode();
        assert_eq!(
            TopologyPayload::decode(encoded.as_slice()).expect("IPv6 payload must decode"),
            payload
        );

        encoded[0] = 99;
        assert!(matches!(
            TopologyPayload::decode(encoded.as_slice()),
            Err(super::PayloadDecodeError::UnsupportedVersion(99))
        ));
        encoded[0] = super::PAYLOAD_VERSION;
        encoded[1] = 99;
        assert!(matches!(
            TopologyPayload::decode(encoded.as_slice()),
            Err(super::PayloadDecodeError::UnsupportedAddressFamily(99))
        ));

        encoded[1] = 6;
        encoded[36] = 101;
        assert!(matches!(
            TopologyPayload::decode(encoded.as_slice()),
            Err(super::PayloadDecodeError::InvalidTelemetry(_))
        ));
    }
}
