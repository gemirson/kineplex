//! KinePlex CLI - Command line interface for KinePlex

use anyhow::Result;
use clap::{Parser, Subcommand};
use kineplex_sdk::{GraphConfigDto, GraphSubmitRequest, KinePlexClient};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser)]
#[command(name = "kineplex")]
#[command(about = "KinePlex - Distributed data plane execution", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    #[arg(short, long, default_value = "http://localhost:8080")]
    server: String,
    #[arg(short, long)]
    api_key: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    Submit {
        #[arg(short, long)]
        tenant_id: String,
        #[arg(short, long)]
        max_memory: Option<u64>,
        #[arg(short, long)]
        idempotency_key: Option<String>,
    },
    Status { graph_id: String },
    Cancel { graph_id: String },
    List,
}

async fn run(cli: Cli) -> Result<()> {
    let mut client = KinePlexClient::new(&cli.server);
    if let Some(key) = &cli.api_key {
        client = client.with_api_key(key);
    }

    match cli.command {
        Commands::Submit { tenant_id, max_memory, idempotency_key } => {
            let request = GraphSubmitRequest {
                tenant_id,
                config: GraphConfigDto {
                    max_memory_mb: max_memory,
                    ..GraphConfigDto::default()
                },
                idempotency_key,
            };
            let graph_id = client.submit_graph(request).await?;
            println!("Graph submitted: {}", graph_id.0);
        }
        Commands::Status { graph_id } => {
            let graph_id = uuid::Uuid::parse_str(&graph_id)
                .map_err(|e| anyhow::anyhow!("Invalid graph ID: {}", e))?;
            let status = client.get_status(&kineplex_core::GraphId(graph_id)).await?;
            println!("Graph ID: {}", status.graph_id);
            println!("Status: {}", status.status);
            if !status.stages_completed.is_empty() {
                println!("Completed stages: {:?}", status.stages_completed);
            }
        }
        Commands::Cancel { graph_id } => {
            let graph_id = uuid::Uuid::parse_str(&graph_id)
                .map_err(|e| anyhow::anyhow!("Invalid graph ID: {}", e))?;
            client.cancel(&kineplex_core::GraphId(graph_id)).await?;
            println!("Graph cancelled: {}", graph_id);
        }
        Commands::List => println!("Listing graphs not yet implemented"),
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    run(Cli::parse()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    fn server(status: &str, body: &str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let status = status.to_string();
        let body = body.to_string();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 2048];
            let _ = stream.read(&mut request);
            let response = format!(
                "HTTP/1.1 {}\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
                status, body.len(), body
            );
            stream.write_all(response.as_bytes()).unwrap();
        });
        address
    }

    fn cli(command: Commands, server: String) -> Cli {
        Cli { command, server, api_key: Some("key".to_string()) }
    }

    #[tokio::test]
    async fn list_command_runs_without_server() {
        run(cli(Commands::List, "http://127.0.0.1:1".to_string().to_string())).await.unwrap();
    }

    #[tokio::test]
    async fn submit_command_runs_successfully() {
        let id = uuid::Uuid::new_v4();
        let server = server("200 OK", &format!(r#"{{"graph_id":"{}"}}"#, id));
        let command = Commands::Submit {
            tenant_id: "tenant".to_string(),
            max_memory: Some(1024),
            idempotency_key: Some("key".to_string()),
        };
        run(cli(command, server)).await.unwrap();
    }

    #[tokio::test]
    async fn status_command_runs_with_and_without_completed_stages() {
        let id = uuid::Uuid::new_v4();
        let server = server("200 OK", &format!(r#"{{"graph_id":"{}","status":"running","stages_completed":["receptor"],"metrics":null}}"#, id));
        run(cli(Commands::Status { graph_id: id.to_string() }, server)).await.unwrap();
        let invalid = run(cli(Commands::Status { graph_id: "invalid".to_string() }, "http://127.0.0.1:1".to_string()));
        assert!(invalid.await.is_err());
    }

    #[tokio::test]
    async fn cancel_command_runs_and_rejects_invalid_id() {
        let id = uuid::Uuid::new_v4();
        let server = server("204 No Content", "");
        run(cli(Commands::Cancel { graph_id: id.to_string() }, server)).await.unwrap();
        let invalid = run(cli(Commands::Cancel { graph_id: "invalid".to_string() }, "http://127.0.0.1:1".to_string()));
        assert!(invalid.await.is_err());
    }
}