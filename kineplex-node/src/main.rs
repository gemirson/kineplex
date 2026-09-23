use std::fmt::Display;
use std::io::Write;
use std::process::ExitCode;

use kineplex_core::initialize_node_config;
use kineplex_core::observability::init_tracing;
use kineplex_net::gossip::{gossip_addr, start, GossipConfig};
use kineplex_node::cli;
use kineplex_node::control::ControlServer;

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let tracing_guard = match init_tracing() {
        Ok(guard) => guard,
        Err(error) => {
            write_boot_error(&error);
            return ExitCode::FAILURE;
        }
    };

    let exit_code = run().await;
    drop(tracing_guard);
    exit_code
}

async fn run() -> ExitCode {
    let parsed_config = match cli::parse() {
        Ok(config) => config,
        Err(error) => return print_clap_error(&error),
    };

    let config = match initialize_node_config(parsed_config) {
        Ok(config) => config,
        Err(error) => {
            tracing::error!(error = %error, "failed to initialize KinePlex node");
            return ExitCode::FAILURE;
        }
    };

    let advertise_addr = match config.advertise_addr() {
        Some(addr) => addr,
        None => {
            tracing::error!(
                bind_ip = %config.bind_ip,
                "--advertise-ip is required when --bind-ip is unspecified"
            );
            return ExitCode::FAILURE;
        }
    };
    let gossip_bind_addr = match gossip_addr(config.bind_addr()) {
        Ok(addr) => addr,
        Err(error) => {
            tracing::error!(%error, "invalid Gossip bind endpoint");
            return ExitCode::FAILURE;
        }
    };
    let gossip_advertise_addr = match gossip_addr(advertise_addr) {
        Ok(addr) => addr,
        Err(error) => {
            tracing::error!(%error, "invalid Gossip advertise endpoint");
            return ExitCode::FAILURE;
        }
    };
    let gossip_seeds = match config
        .seeds
        .iter()
        .copied()
        .map(gossip_addr)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(seeds) => seeds,
        Err(error) => {
            tracing::error!(%error, "invalid Gossip seed endpoint");
            return ExitCode::FAILURE;
        }
    };

    let gossip = match start(GossipConfig::new(
        gossip_bind_addr,
        gossip_advertise_addr,
        gossip_seeds,
    ))
    .await
    {
        Ok(handle) => handle,
        Err(error) => {
            tracing::error!(%error, "failed to start Gossip service");
            return ExitCode::FAILURE;
        }
    };

    let control =
        match ControlServer::bind_with_routing(config.bind_addr(), gossip.routing_table()).await {
            Ok(server) => server,
            Err(error) => {
                let _shutdown_result = gossip.shutdown().await;
                tracing::error!(%error, "failed to start Control Plane TCP server");
                return ExitCode::FAILURE;
            }
        };

    tracing::info!(
        bind_addr = %config.bind_addr(),
        control_addr = %control.local_addr(),
        gossip_bind_addr = %gossip.local_addr(),
        gossip_advertise_addr = %gossip_advertise_addr,
        seeds = ?config.seeds,
        seed_count = config.seeds.len(),
        "Starting KinePlex Node"
    );

    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(%error, "failed to listen for process shutdown signal");
        let _control_result = control.shutdown().await;
        let _gossip_result = gossip.shutdown().await;
        return ExitCode::FAILURE;
    }

    tracing::info!("shutting down KinePlex Node");
    let control_result = control.shutdown().await;
    let gossip_result = gossip.shutdown().await;
    if let Err(error) = control_result {
        tracing::error!(%error, "failed to stop Control Plane server");
    }
    match gossip_result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "failed to stop Gossip service");
            ExitCode::FAILURE
        }
    }
}

fn print_clap_error(error: &clap::Error) -> ExitCode {
    let exit_code = match error.exit_code() {
        0 => ExitCode::SUCCESS,
        2 => ExitCode::from(2),
        unexpected => {
            tracing::error!(
                exit_code = unexpected,
                "clap returned an unexpected exit code"
            );
            ExitCode::FAILURE
        }
    };

    if let Err(print_error) = error.print() {
        tracing::error!(error = %print_error, "failed to print command-line diagnostic");
        return ExitCode::FAILURE;
    }

    exit_code
}

fn write_boot_error(error: &impl Display) {
    let mut stderr = std::io::stderr().lock();
    let _write_result = writeln!(stderr, "error: failed to initialize tracing: {error}");
}
