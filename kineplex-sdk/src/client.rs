//! KinePlex client implementation

use crate::{GraphSubmitRequest, GraphStatusResponse};
use kineplex_core::GraphId;
use reqwest::Client;
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
    fn test_client_error_request_failed() {
        let err = ClientError::RequestFailed(reqwest::Error::new(
            reqwest::error::Kind::Request, 
            None
        ));
        assert!(err.to_string().contains("Request failed"));
    }
    
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