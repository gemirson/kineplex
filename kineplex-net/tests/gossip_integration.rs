use std::net::SocketAddr;
use std::time::Duration;

use kineplex_net::gossip::{start, GossipConfig, GossipHandle, MembershipEvent};
use tokio::sync::broadcast;
use tokio::time::{sleep, timeout, Instant};

const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(12);
const FAILURE_TIMEOUT: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

fn loopback_config(seeds: Vec<SocketAddr>) -> GossipConfig {
    let ephemeral = SocketAddr::from(([127, 0, 0, 1], 0));
    GossipConfig::new(ephemeral, ephemeral, seeds)
        .with_topology_log_interval(Duration::from_secs(60))
}

async fn start_node(seeds: Vec<SocketAddr>) -> GossipHandle {
    start(loopback_config(seeds))
        .await
        .expect("a loopback Gossip participant must start")
}

async fn wait_for_convergence(nodes: &[&GossipHandle], expected: &[SocketAddr]) {
    let mut sorted_expected = expected.to_vec();
    sorted_expected.sort_unstable();
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;

    loop {
        let converged = nodes.iter().all(|node| {
            let mut members = node.members().to_vec();
            members.sort_unstable();
            members == sorted_expected
        });
        if converged {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "Gossip participants did not converge before the deadline"
        );
        sleep(POLL_INTERVAL).await;
    }
}

async fn wait_for_member_down(
    events: &mut broadcast::Receiver<MembershipEvent>,
    expected: SocketAddr,
) {
    let observed = timeout(FAILURE_TIMEOUT, async {
        loop {
            match events.recv().await {
                Ok(MembershipEvent::MemberDown(member)) if member == expected => return true,
                Ok(MembershipEvent::MemberUp(_) | MembershipEvent::MemberDown(_)) => {}
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => return false,
            }
        }
    })
    .await
    .expect("MemberDown must be emitted within the SWIM failure deadline");
    assert!(observed, "membership event stream closed before MemberDown");
}

#[tokio::test]
async fn three_nodes_converge_through_one_seed() {
    let node_b = start_node(Vec::new()).await;
    let node_a = start_node(vec![node_b.local_addr()]).await;
    let node_c = start_node(vec![node_b.local_addr()]).await;
    let expected = [
        node_a.local_addr(),
        node_b.local_addr(),
        node_c.local_addr(),
    ];

    wait_for_convergence(&[&node_a, &node_b, &node_c], &expected).await;

    let (a_result, b_result, c_result) =
        tokio::join!(node_a.shutdown(), node_b.shutdown(), node_c.shutdown());
    assert!(a_result.is_ok());
    assert!(b_result.is_ok());
    assert!(c_result.is_ok());
}

#[tokio::test]
async fn survivors_mark_a_crashed_node_down() {
    let node_b = start_node(Vec::new()).await;
    let node_a = start_node(vec![node_b.local_addr()]).await;
    let node_c = start_node(vec![node_b.local_addr()]).await;
    let node_c_addr = node_c.local_addr();
    let expected_before_failure = [node_a.local_addr(), node_b.local_addr(), node_c_addr];

    wait_for_convergence(&[&node_a, &node_b, &node_c], &expected_before_failure).await;

    let mut node_a_events = node_a.subscribe();
    let mut node_b_events = node_b.subscribe();
    node_c.abort().await;

    tokio::join!(
        wait_for_member_down(&mut node_a_events, node_c_addr),
        wait_for_member_down(&mut node_b_events, node_c_addr)
    );
    let expected_after_failure = [node_a.local_addr(), node_b.local_addr()];
    wait_for_convergence(&[&node_a, &node_b], &expected_after_failure).await;

    let (a_result, b_result) = tokio::join!(node_a.shutdown(), node_b.shutdown());
    assert!(a_result.is_ok());
    assert!(b_result.is_ok());
}

#[tokio::test]
async fn rejects_unreachable_or_zero_interval_configuration() {
    let ephemeral = SocketAddr::from(([127, 0, 0, 1], 0));
    let unspecified = SocketAddr::from(([0, 0, 0, 0], 8001));

    let unreachable = start(GossipConfig::new(ephemeral, unspecified, Vec::new())).await;
    assert!(unreachable.is_err());

    let zero_interval = start(
        GossipConfig::new(ephemeral, ephemeral, Vec::new())
            .with_topology_log_interval(Duration::ZERO),
    )
    .await;
    assert!(zero_interval.is_err());
}
