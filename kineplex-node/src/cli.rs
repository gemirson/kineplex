//! Command-line parsing for `kineplex-node`.

use std::ffi::OsString;
use std::net::{IpAddr, SocketAddr};

use clap::Parser;
use kineplex_core::{NodeConfig, DEFAULT_NODE_PORT};
use kineplex_net::gossip::gossip_addr;
use kineplex_net::parse_socket_addr;

#[derive(Debug, Parser)]
#[command(
    name = "kineplex-node",
    version,
    about = "Starts a KinePlex distributed database node",
    long_about = "Starts a KinePlex node and bootstraps P2P membership through optional seed peers.\n\
                  Both IPv4 and IPv6 literals are supported. IPv6 seed addresses must be bracketed."
)]
struct Cli {
    /// UDP port on which this node accepts traffic.
    #[arg(
        long,
        default_value_t = DEFAULT_NODE_PORT,
        value_name = "PORT",
        value_parser = parse_node_port
    )]
    port: u16,

    /// Local IPv4 or IPv6 interface address on which this node binds.
    #[arg(long, default_value = "0.0.0.0", value_name = "IP")]
    bind_ip: IpAddr,

    /// Reachable IPv4 or IPv6 address advertised to Gossip peers.
    ///
    /// Required when `--bind-ip` is unspecified (`0.0.0.0` or `::`).
    #[arg(long, value_name = "IP")]
    advertise_ip: Option<IpAddr>,

    /// Comma-separated IPv4/IPv6 node addresses used to join the P2P mesh.
    #[arg(
        long,
        value_delimiter = ',',
        value_name = "IP:PORT",
        value_parser = parse_seed
    )]
    seed: Vec<SocketAddr>,
}

fn parse_node_port(value: &str) -> Result<u16, String> {
    let port = value.parse::<u16>().map_err(|error| error.to_string())?;
    validate_adjacent_gossip_port(SocketAddr::from(([0, 0, 0, 0], port)))?;
    Ok(port)
}

fn parse_seed(value: &str) -> Result<SocketAddr, String> {
    let seed = parse_socket_addr(value).map_err(|error| error.to_string())?;
    validate_adjacent_gossip_port(seed)?;
    Ok(seed)
}

fn validate_adjacent_gossip_port(node_addr: SocketAddr) -> Result<(), String> {
    gossip_addr(node_addr)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

impl From<Cli> for NodeConfig {
    fn from(cli: Cli) -> Self {
        Self::new(cli.bind_ip, cli.port, cli.seed).with_advertise_ip(cli.advertise_ip)
    }
}

/// Parses the current process arguments into an immutable node configuration.
///
/// # Errors
///
/// Returns a formatted [`clap::Error`] for invalid values or unsupported arguments.
/// The caller can use [`clap::Error::exit`] to print the diagnostic, include the
/// `--help` hint, and terminate with clap's conventional exit code.
pub fn parse() -> Result<NodeConfig, clap::Error> {
    parse_from(std::env::args_os())
}

/// Parses an explicit argument sequence into an immutable node configuration.
///
/// This variant enables deterministic parsing in tests and embedding scenarios. The
/// first value must be the executable name, as with [`std::env::args_os`].
///
/// # Errors
///
/// Returns [`clap::Error`] when an address, port, or command-line option is invalid.
pub fn parse_from<I, T>(arguments: I) -> Result<NodeConfig, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    Cli::try_parse_from(arguments).map(|cli| {
        tracing::trace!("command-line configuration parsed");
        cli.into()
    })
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

    use clap::error::ErrorKind;
    use kineplex_core::DEFAULT_NODE_PORT;

    use super::parse_from;

    #[test]
    fn uses_defaults_without_arguments() {
        let config = parse_from(["kineplex-node"]).expect("default arguments must be valid");

        assert_eq!(config.port, DEFAULT_NODE_PORT);
        assert_eq!(config.bind_ip, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        assert_eq!(config.advertise_ip, None);
        assert!(config.seeds.is_empty());
    }

    #[test]
    fn parses_one_seed() {
        let config = parse_from(["kineplex-node", "--seed", "127.0.0.1:8000"])
            .expect("a valid IPv4 seed must parse");

        assert_eq!(
            config.seeds,
            [SocketAddr::from((Ipv4Addr::LOCALHOST, 8000))]
        );
    }

    #[test]
    fn parses_comma_separated_seeds() {
        let config = parse_from([
            "kineplex-node",
            "--seed",
            "192.168.1.10:8000,192.168.1.11:8000",
        ])
        .expect("comma-separated IPv4 seeds must parse");

        assert_eq!(
            config.seeds,
            [
                SocketAddr::from(([192, 168, 1, 10], 8000)),
                SocketAddr::from(([192, 168, 1, 11], 8000)),
            ]
        );
    }

    #[test]
    fn parses_ipv6_bind_address_and_seed() {
        let config = parse_from([
            "kineplex-node",
            "--bind-ip",
            "::1",
            "--port",
            "9000",
            "--seed",
            "[2001:db8::1]:8000",
        ])
        .expect("valid IPv6 literals must parse");

        assert_eq!(config.bind_ip, IpAddr::V6(Ipv6Addr::LOCALHOST));
        assert_eq!(config.port, 9000);
        assert_eq!(
            config.seeds,
            ["[2001:db8::1]:8000"
                .parse::<SocketAddr>()
                .expect("the fixture is a valid IPv6 socket address")]
        );
    }

    #[test]
    fn rejects_malformed_bind_ip() {
        let error = parse_from(["kineplex-node", "--bind-ip", "not-an-ip"])
            .expect_err("a malformed bind IP must fail");

        assert_eq!(error.kind(), ErrorKind::ValueValidation);
        assert!(error.to_string().contains("--bind-ip"));
    }

    #[test]
    fn rejects_malformed_seed() {
        let error = parse_from(["kineplex-node", "--seed", "127.0.0.1"])
            .expect_err("a seed without a port must fail");

        assert_eq!(error.kind(), ErrorKind::ValueValidation);
        assert!(error.to_string().contains("--seed"));
    }

    #[test]
    fn rejects_port_larger_than_u16() {
        let error = parse_from(["kineplex-node", "--port", "999999"])
            .expect_err("a port larger than u16 must fail");

        assert_eq!(error.kind(), ErrorKind::ValueValidation);
        assert!(error
            .to_string()
            .contains("number too large to fit in target type"));
    }

    #[test]
    fn parses_advertise_ip() {
        let config = parse_from(["kineplex-node", "--advertise-ip", "192.168.1.20"])
            .expect("a valid advertise address must parse");

        assert_eq!(
            config.advertise_ip,
            Some(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 20)))
        );
    }

    #[test]
    fn rejects_ports_without_adjacent_gossip_capacity() {
        for arguments in [
            ["kineplex-node", "--port", "65535"],
            ["kineplex-node", "--seed", "127.0.0.1:65535"],
        ] {
            let error = parse_from(arguments)
                .expect_err("port 65535 cannot reserve an adjacent Gossip port");

            assert_eq!(error.kind(), ErrorKind::ValueValidation);
            assert!(error.to_string().contains("cannot reserve Gossip port +1"));
        }
    }
}
