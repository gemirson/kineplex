//! KinePlex CLI - Command line interface for KinePlex

use clap::{Parser, Subcommand};
use kineplex_sdk::{KinePlexClient, GraphSubmitRequest, GraphConfigDto};
use anyhow::Result;
use tracing_subscriber::FmtSubscriber;

#[derive(Parser)]
#[command(name = "kineplex")]
#[command(about = "KinePlex - Distributed data plane execution", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    
    /// Server URL
    #[arg(short, long, default_value = "http://localhost:8080")]
    server: String,
    
    /// API key for authentication
    #[arg(short, long)]
    api_key: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    /// Submit a graph for execution
    Submit {
        /// Tenant ID
        #[arg(short, long)]
        tenant_id: String,
        
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

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    
    let cli = Cli::parse();
    
    // Create client
    let mut client = KinePlexClient::new(&cli.server);
    if let Some(key) = &cli.api_key {
        client = client.with_api_key(key);
    }
    
    match cli.command {
        Commands::Submit { 
            tenant_id, 
            max_memory, 
            idempotency_key 
        } => {
            let mut config = GraphConfigDto::default();
            config.max_memory_mb = max_memory;
            
            let request = GraphSubmitRequest {
                tenant_id,
                config,
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
        
        Commands::List => {
            println!("Listing graphs not yet implemented");
        }
    }
    
    Ok(())
}