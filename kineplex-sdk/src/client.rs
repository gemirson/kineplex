//! KinePlex client implementation

use crate::{GraphSubmitRequest, GraphStatusResponse};
use kineplex_core::GraphId;
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("Request failed: {0}")]
    RequestFailed(#[from] reqwest::Error),
    
    #[error("Server error: {0}")]
    ServerError(String),
    
    #[error("Graph not found: {0}")]
    GraphNotFound(String),
    
    #[error("Authentication required")]
    AuthenticationRequired,
}

pub type Result<T> = std::result::Result<T, ClientError>;

/// KinePlex client for submitting and managing graphs
pub struct KinePlexClient {
    base_url: String,
    client: Client,
    api_key: Option<String>,
}

impl KinePlexClient {
    /// Create a new client
    pub fn new(base_url: impl Into<String>) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .expect("Failed to create HTTP client");
        
        Self {
            base_url: base_url.into(),
            client,
            api_key: None,
        }
    }
    
    /// Set API key for authentication
    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }
    
    /// Submit a graph for execution
    pub async fn submit_graph(&self, request: GraphSubmitRequest) -> Result<GraphId> {
        let url = format!("{}/api/v1/graphs", self.base_url);
        
        let mut req = self.client.post(&url).json(&request);
        
        if let Some(ref key) = self.api_key {
            req = req.header("Authorization", format!("Bearer {}", key));
        }
        
        let response = req.send().await?;
        
        if response.status().is_success() {
            #[derive(Deserialize)]
            struct SubmitResponse {
                graph_id: String,
            }
            
            let result: SubmitResponse = response.json().await?;
            Ok(GraphId(uuid::Uuid::parse_str(&result.graph_id).unwrap()))
        } else if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            Err(ClientError::AuthenticationRequired)
        } else {
            Err(ClientError::ServerError(response.text().await?))
        }
    }
    
    /// Get graph status
    pub async fn get_status(&self, graph_id: &GraphId) -> Result<GraphStatusResponse> {
        let url = format!("{}/api/v1/graphs/{}", self.base_url, graph_id.0);
        
        let mut req = self.client.get(&url);
        
        if let Some(ref key) = self.api_key {
            req = req.header("Authorization", format!("Bearer {}", key));
        }
        
        let response = req.send().await?;
        
        if response.status().is_success() {
            Ok(response.json().await?)
        } else if response.status() == reqwest::StatusCode::NOT_FOUND {
            Err(ClientError::GraphNotFound(graph_id.0.to_string()))
        } else {
            Err(ClientError::ServerError(response.text().await?))
        }
    }
    
    /// Cancel graph execution
    pub async fn cancel(&self, graph_id: &GraphId) -> Result<()> {
        let url = format!("{}/api/v1/graphs/{}/cancel", self.base_url, graph_id.0);
        
        let mut req = self.client.post(&url);
        
        if let Some(ref key) = self.api_key {
            req = req.header("Authorization", format!("Bearer {}", key));
        }
        
        let response = req.send().await?;
        
        if response.status().is_success() {
            Ok(())
        } else if response.status() == reqwest::StatusCode::NOT_FOUND {
            Err(ClientError::GraphNotFound(graph_id.0.to_string()))
        } else {
            Err(ClientError::ServerError(response.text().await?))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_client_creation() {
        let client = KinePlexClient::new("http://localhost:8080");
        assert_eq!(client.base_url, "http://localhost:8080");
    }
    
    #[test]
    fn test_client_with_api_key() {
        let client = KinePlexClient::new("http://localhost:8080")
            .with_api_key("test-key");
        assert!(client.api_key.is_some());
    }
}

// ClientError tests
    #[test]
    fn test_client_error_server_error() {
        let err = ClientError::ServerError("Internal error".to_string());
        assert_eq!(err.to_string(), "Server error: Internal error");
    }
    
    #[test]
    fn test_client_error_graph_not_found() {
        let err = ClientError::GraphNotFound("graph-123".to_string());
        assert_eq!(err.to_string(), "Graph not found: graph-123");
    }
    
    #[test]
    fn test_client_error_authentication_required() {
        let err = ClientError::AuthenticationRequired;
        assert_eq!(err.to_string(), "Authentication required");
    }
    
    #[test]
    fn test_client_error_display() {
        let err = ClientError::ServerError("test".to_string());
        let _ = format!("{}", err);
    }
    
    #[test]
    fn test_client_default_url() {
        let client = KinePlexClient::new("localhost:8080");
        assert_eq!(client.base_url, "localhost:8080");
    }
    
    #[test]
    fn test_client_with_https_url() {
        let client = KinePlexClient::new("https://api.example.com");
        assert_eq!(client.base_url, "https://api.example.com");
    }
    
    #[test]
    fn test_client_api_key_persistence() {
        let client = KinePlexClient::new("http://localhost:8080")
            .with_api_key("my-secret-key");
        
        assert_eq!(client.api_key.unwrap(), "my-secret-key");
    }
    
    #[test]
    fn test_client_multiple_api_key() {
        let client = KinePlexClient::new("http://localhost:8080")
            .with_api_key("key1")
            .with_api_key("key2");
        
        // Last key wins
        assert_eq!(client.api_key.unwrap(), "key2");
    }



#[cfg(test)]
mod http_tests {
    use super::*;
    use crate::types::{GraphConfigDto, GraphSubmitRequest};
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

    fn submit_request() -> GraphSubmitRequest {
        GraphSubmitRequest {
            tenant_id: "tenant".to_string(),
            config: GraphConfigDto::default(),
            idempotency_key: Some("key".to_string()),
        }
    }

    #[tokio::test]
    async fn submit_graph_success_and_auth_header_path() {
        let id = uuid::Uuid::new_v4();
        let client = KinePlexClient::new(server("200 OK", &format!(r#"{{"graph_id":"{}"}}"#, id)))
            .with_api_key("secret");
        assert_eq!(client.submit_graph(submit_request()).await.unwrap().0, id);
    }

    #[tokio::test]
    async fn submit_graph_maps_auth_and_server_errors() {
        let unauthorized = KinePlexClient::new(server("401 Unauthorized", "unauthorized"));
        assert!(matches!(unauthorized.submit_graph(submit_request()).await, Err(ClientError::AuthenticationRequired)));
        let failed = KinePlexClient::new(server("500 Internal Server Error", "failed"));
        assert!(matches!(failed.submit_graph(submit_request()).await, Err(ClientError::ServerError(_))));
    }

    #[tokio::test]
    async fn status_success_not_found_and_server_error() {
        let id = GraphId::new();
        let body = format!(r#"{{"graph_id":"{}","status":"running","stages_completed":[],"metrics":null}}"#, id.0);
        let client = KinePlexClient::new(server("200 OK", &body));
        assert_eq!(client.get_status(&id).await.unwrap().status, "running");
        let missing = KinePlexClient::new(server("404 Not Found", "missing"));
        assert!(matches!(missing.get_status(&id).await, Err(ClientError::GraphNotFound(_))));
        let failed = KinePlexClient::new(server("500 Internal Server Error", "failed"));
        assert!(matches!(failed.get_status(&id).await, Err(ClientError::ServerError(_))));
    }

    #[tokio::test]
    async fn cancel_success_not_found_and_server_error() {
        let id = GraphId::new();
        let client = KinePlexClient::new(server("204 No Content", ""));
        assert!(client.cancel(&id).await.is_ok());
        let missing = KinePlexClient::new(server("404 Not Found", "missing"));
        assert!(matches!(missing.cancel(&id).await, Err(ClientError::GraphNotFound(_))));
        let failed = KinePlexClient::new(server("500 Internal Server Error", "failed"));
        assert!(matches!(failed.cancel(&id).await, Err(ClientError::ServerError(_))));
    }

    #[tokio::test]
    async fn request_error_is_mapped() {
        let client = KinePlexClient::new("http://127.0.0.1:1");
        assert!(matches!(client.cancel(&GraphId::new()).await, Err(ClientError::RequestFailed(_))));
    }
}