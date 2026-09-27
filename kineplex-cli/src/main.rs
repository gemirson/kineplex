//! Command-line interface for submitting jobs and managing graphs in KinePlex.

use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use anyhow::Result;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use clap::{Parser, Subcommand};
use kineplex_sdk::{GraphConfigDto, GraphSubmitRequest, KinePlexClient};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

#[derive(Debug, Parser)]
#[command(name = "kineplex-cli", about = "Submit KinePlex graph jobs and manage executions")]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Server URL for client-based commands
    #[arg(short, long, default_value = "http://localhost:8080")]
    server: String,

    /// API key for authentication
    #[arg(short, long)]
    api_key: Option<String>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Submit a JSON/YAML graph file to a seed node, or submit with client options
    Submit {
        /// Path to the graph job definition (YAML or JSON)
        #[arg(value_name = "JOB_FILE")]
        job: Option<PathBuf>,

        /// Seed node socket address (IP:PORT)
        #[arg(long, value_name = "IP:PORT")]
        seed: Option<SocketAddr>,

        /// Maximum wait time for allocation to reach RUNNING state
        #[arg(long, default_value_t = 300, value_name = "SECONDS")]
        timeout_secs: u64,

        /// Tenant ID for multi-tenant isolation
        #[arg(short, long)]
        tenant_id: Option<String>,

        /// Maximum memory in MB
        #[arg(short, long)]
        max_memory: Option<u64>,

        /// Idempotency key
        #[arg(short, long)]
        idempotency_key: Option<String>,
    },

    /// Get graph status
    Status {
        /// Graph ID
        graph_id: String,
    },

    /// Cancel a running graph
    Cancel {
        /// Graph ID
        graph_id: String,
    },

    /// List running graphs
    List,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let server_url = cli.server.clone();
    let api_key = cli.api_key.clone();

    let result = match cli.command {
        Command::Submit {
            job: Some(job),
            seed: Some(seed),
            timeout_secs,
            ..
        } => submit_file(&job, seed, Duration::from_secs(timeout_secs)).await,

        Command::Submit {
            tenant_id,
            max_memory,
            idempotency_key,
            job,
            ..
        } => {
            let mut client = KinePlexClient::new(&server_url);
            if let Some(key) = &api_key {
                client = client.with_api_key(key);
            }
            let mut config = GraphConfigDto::default();
            config.max_memory_mb = max_memory;

            let tenant = tenant_id
                .or_else(|| job.as_ref().map(|p| p.to_string_lossy().to_string()))
                .unwrap_or_else(|| "default".to_string());

            let request = GraphSubmitRequest {
                tenant_id: tenant,
                config,
                idempotency_key,
            };

            match client.submit_graph(request).await {
                Ok(graph_id) => {
                    println!("Graph submitted: {}", graph_id.0);
                    Ok(())
                }
                Err(error) => Err(format!("Failed to submit graph: {error}")),
            }
        }

        Command::Status { graph_id } => {
            let mut client = KinePlexClient::new(&server_url);
            if let Some(key) = &api_key {
                client = client.with_api_key(key);
            }
            let parsed_id = match uuid::Uuid::parse_str(&graph_id) {
                Ok(u) => kineplex_core::GraphId(u),
                Err(e) => {
                    eprintln!("Invalid graph ID: {e}");
                    return ExitCode::FAILURE;
                }
            };
            match client.get_status(&parsed_id).await {
                Ok(status) => {
                    println!("Graph ID: {}", status.graph_id);
                    println!("Status: {}", status.status);
                    if !status.stages_completed.is_empty() {
                        println!("Completed stages: {:?}", status.stages_completed);
                    }
                    Ok(())
                }
                Err(e) => Err(format!("Failed to get status: {e}")),
            }
        }

        Command::Cancel { graph_id } => {
            let mut client = KinePlexClient::new(&server_url);
            if let Some(key) = &api_key {
                client = client.with_api_key(key);
            }
            let parsed_id = match uuid::Uuid::parse_str(&graph_id) {
                Ok(u) => kineplex_core::GraphId(u),
                Err(e) => {
                    eprintln!("Invalid graph ID: {e}");
                    return ExitCode::FAILURE;
                }
            };
            match client.cancel(&parsed_id).await {
                Ok(()) => {
                    println!("Graph cancelled: {}", graph_id);
                    Ok(())
                }
                Err(e) => Err(format!("Failed to cancel graph: {e}")),
            }
        }

        Command::List => {
            println!("Listing running graphs: (not implemented on seed)");
            Ok(())
        }
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let mut stderr = std::io::stderr().lock();
            let _write = writeln!(stderr, "kineplex-cli: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn submit_file(job: &Path, seed: SocketAddr, timeout_duration: Duration) -> Result<(), String> {
    let mut payload = read_job(job).await?;
    let job_dir = job.parent().unwrap_or_else(|| Path::new("."));
    package_wasm_files(&mut payload, job_dir).await?;

    let (status, body) = request(seed, "POST", "/submit", Some(payload)).await?;
    if status != 200 {
        let message = String::from_utf8_lossy(&body).trim().to_owned();
        return Err(format!("seed rejected the job with status {status}: {message}"));
    }

    let response: Value = serde_json::from_slice(&body)
        .map_err(|error| format!("seed response is not valid JSON: {error}"))?;
    let graph_id = response
        .get("graph_id")
        .and_then(Value::as_str)
        .ok_or_else(|| "seed response did not contain 'graph_id'".to_owned())?;
    let start = Instant::now();

    loop {
        if start.elapsed() > timeout_duration {
            return Err(format!(
                "timed out waiting for graph allocation after {}s",
                timeout_duration.as_secs()
            ));
        }

        let (status, body) = request(seed, "GET", "/status", None).await?;
        if status != 200 {
            let message = String::from_utf8_lossy(&body).trim().to_owned();
            return Err(format!("status query failed with code {status}: {message}"));
        }

        let report: Value = serde_json::from_slice(&body)
            .map_err(|error| format!("status response is not valid JSON: {error}"))?;
        let allocations = report
            .get("allocations")
            .and_then(Value::as_array)
            .ok_or_else(|| "seed status response missing 'allocations' array".to_owned())?;

        let current = allocations.iter().find(|entry| {
            entry
                .get("graph_id")
                .and_then(Value::as_str)
                .is_some_and(|candidate| candidate == graph_id)
        });

        let Some(current) = current else {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        };

        let state = current
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("UNKNOWN");
        progress(&format!("graph {graph_id} state: {state}"));

        match state {
            "RUNNING" => {
                println!();
                return Ok(());
            }
            "FAILED" | "CANCELLED" => {
                return Err(format!("graph allocation ended in state {state}"))
            }
            _ => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
}

async fn read_job(path: &Path) -> Result<Value, String> {
    let bytes = tokio::fs::read(path).await.map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            format!("job file not found: {}", path.display())
        } else {
            format!("cannot read job file {}: {error}", path.display())
        }
    })?;
    match path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "yaml" | "yml" => {
            let yaml: serde_yaml::Value = serde_yaml::from_slice(&bytes)
                .map_err(|error| format!("invalid YAML job file: {error}"))?;
            serde_json::to_value(yaml).map_err(|error| format!("cannot convert YAML job: {error}"))
        }
        _ => serde_json::from_slice(&bytes)
            .map_err(|error| format!("invalid JSON job file: {error}")),
    }
}

async fn package_wasm_files(job: &mut Value, base: &Path) -> Result<(), String> {
    let nodes = job
        .get_mut("nodes")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| "job file must contain a 'nodes' array".to_owned())?;
    for node in nodes {
        let Some(values) = node.as_object_mut() else {
            continue;
        };
        let Some(reference) = values
            .get("wasm")
            .and_then(Value::as_str)
            .map(str::to_owned)
        else {
            continue;
        };
        let candidate = PathBuf::from(&reference);
        let wasm_path = if candidate.is_absolute() {
            candidate
        } else {
            base.join(candidate)
        };
        let wasm = tokio::fs::read(&wasm_path).await.map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                format!("Wasm file not found: {}", wasm_path.display())
            } else {
                format!("cannot read Wasm file {}: {error}", wasm_path.display())
            }
        })?;
        values.insert("wasm".to_owned(), Value::String(STANDARD.encode(wasm)));
        values.insert(
            "wasm_encoding".to_owned(),
            Value::String("base64".to_owned()),
        );
    }
    Ok(())
}

async fn request(
    address: SocketAddr,
    method: &str,
    path: &str,
    payload: Option<Value>,
) -> Result<(u16, Vec<u8>), String> {
    timeout(Duration::from_secs(10), async {
        let mut stream = TcpStream::connect(address)
            .await
            .map_err(|error| format!("cannot connect to seed {address}: {error}"))?;
        let body = payload
            .map(|value| serde_json::to_vec(&value).map_err(|error| error.to_string()))
            .transpose()?
            .unwrap_or_default();
        let headers = format!(
            "{method} {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        stream
            .write_all(headers.as_bytes())
            .await
            .map_err(|error| error.to_string())?;
        if !body.is_empty() {
            stream
                .write_all(&body)
                .await
                .map_err(|error| error.to_string())?;
        }
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .map_err(|error| error.to_string())?;
        let separator = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .ok_or_else(|| "seed returned a malformed HTTP response".to_owned())?;
        let status = String::from_utf8_lossy(&response[..separator])
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse::<u16>().ok())
            .ok_or_else(|| "seed returned an invalid HTTP status".to_owned())?;
        Ok((status, response[separator + 4..].to_vec()))
    })
    .await
    .map_err(|_| format!("request to seed {address} timed out"))?
}

fn progress(message: &str) {
    let mut stdout = std::io::stdout().lock();
    let _write = write!(stdout, "\r{message}");
    let _flush = stdout.flush();
}
