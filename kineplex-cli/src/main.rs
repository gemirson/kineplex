use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use clap::{Parser, Subcommand};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

#[derive(Debug, Parser)]
#[command(name = "kineplex-cli", about = "Submit KinePlex graph jobs")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Submit a JSON/YAML graph and wait until the seed reports RUNNING.
    Submit {
        job: PathBuf,
        #[arg(long, value_name = "IP:PORT")]
        seed: SocketAddr,
        #[arg(long, default_value_t = 300, value_name = "SECONDS")]
        timeout_secs: u64,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Submit {
            job,
            seed,
            timeout_secs,
        } => submit(&job, seed, Duration::from_secs(timeout_secs)).await,
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

async fn submit(job_path: &Path, seed: SocketAddr, maximum_wait: Duration) -> Result<(), String> {
    let mut job = read_job(job_path).await?;
    let object = job
        .as_object_mut()
        .ok_or_else(|| "job file must contain a JSON/YAML object".to_owned())?;
    object
        .entry("client_id")
        .or_insert_with(|| Value::String("kineplex-cli".to_owned()));
    if !object.get("nodes").is_some_and(Value::is_array) {
        return Err("job file must contain a 'nodes' array".to_owned());
    }
    object
        .entry("edges")
        .or_insert_with(|| Value::Array(Vec::new()));
    package_wasm_files(
        &mut job,
        job_path.parent().unwrap_or_else(|| Path::new(".")),
    )
    .await?;

    let (_, response) = request(seed, "POST", "/submit_graph", Some(job)).await?;
    let response: Value = serde_json::from_slice(&response)
        .map_err(|error| format!("seed returned invalid JSON: {error}"))?;
    let graph_id = response
        .get("graph_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            response
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("seed response did not include graph_id")
                .to_owned()
        })?;

    let start = Instant::now();
    loop {
        if start.elapsed() >= maximum_wait {
            progress(&format!(
                "\nTimed out waiting for graph {graph_id} to reach RUNNING"
            ));
            return Err("allocation timed out".to_owned());
        }
        let (status, body) =
            request(seed, "GET", &format!("/graph_status/{graph_id}"), None).await?;
        if !(200..300).contains(&status) {
            return Err(format!(
                "seed status endpoint returned HTTP {status}: {}",
                String::from_utf8_lossy(&body)
            ));
        }
        let status_json: Value = serde_json::from_slice(&body)
            .map_err(|error| format!("seed returned invalid status JSON: {error}"))?;
        let state = status_json
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("UNKNOWN");
        progress(&format!("Allocating graph {graph_id}: {state}"));
        match state {
            "RUNNING" => {
                progress("\nGraph is RUNNING");
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
        let mut stream = TcpStream::connect(address).await
            .map_err(|error| format!("cannot connect to seed {address}: {error}"))?;
        let body = payload.map(|value| serde_json::to_vec(&value).map_err(|error| error.to_string())).transpose()?.unwrap_or_default();
        let headers = format!(
            "{method} {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        stream.write_all(headers.as_bytes()).await.map_err(|error| error.to_string())?;
        if !body.is_empty() { stream.write_all(&body).await.map_err(|error| error.to_string())?; }
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.map_err(|error| error.to_string())?;
        let separator = response.windows(4).position(|window| window == b"\r\n\r\n")
            .ok_or_else(|| "seed returned a malformed HTTP response".to_owned())?;
        let status = String::from_utf8_lossy(&response[..separator]).lines().next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse::<u16>().ok())
            .ok_or_else(|| "seed returned an invalid HTTP status".to_owned())?;
        Ok((status, response[separator + 4..].to_vec()))
    }).await.map_err(|_| format!("request to seed {address} timed out"))?
}

fn progress(message: &str) {
    let mut stdout = std::io::stdout().lock();
    let _write = write!(stdout, "\r{message}");
    let _flush = stdout.flush();
}
