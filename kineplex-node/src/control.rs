//! TCP Control Plane endpoint for graph submissions.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::time::Duration;

use axum::extract::{DefaultBodyLimit, Json, Request};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{serve, Router};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use uuid::Uuid;

const GRAPH_BODY_LIMIT_BYTES: usize = 1_048_576;
const GRAPH_READ_TIMEOUT: Duration = Duration::from_secs(2);

/// Raw graph payload accepted by the Control Plane.
///
/// The node and edge entries remain opaque JSON at this boundary so the control
/// plane can accept the graph representation before a later validator assigns
/// domain-specific semantics.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitGraphRequest {
    /// Client or application submitting the graph.
    pub client_id: String,
    /// Raw graph node definitions.
    pub nodes: Vec<serde_json::Value>,
    /// Raw directed graph edge definitions.
    pub edges: Vec<serde_json::Value>,
}

impl SubmitGraphRequest {
    fn validate(&self) -> Result<(), &'static str> {
        if self.client_id.trim().is_empty() {
            return Err("client_id must not be empty");
        }
        if self.nodes.is_empty() {
            return Err("nodes must contain at least one node");
        }
        Ok(())
    }
}

/// Accepted response returned after a graph passes boundary validation.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SubmitGraphResponse {
    /// Server-generated graph identifier.
    pub graph_id: Uuid,
    /// Lifecycle state of the accepted submission.
    pub status: &'static str,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

/// Error returned while binding or stopping the Control Plane server.
#[derive(Debug)]
pub enum ControlServerError {
    /// The TCP listener or HTTP server returned an error.
    Server(String),
    /// The isolated server task could not be joined.
    Join(tokio::task::JoinError),
}

impl Display for ControlServerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Server(error) => write!(formatter, "Control Plane server failed: {error}"),
            Self::Join(error) => write!(formatter, "Control Plane task failed: {error}"),
        }
    }
}

impl Error for ControlServerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Server(_) => None,
            Self::Join(error) => Some(error),
        }
    }
}

/// Isolated TCP Control Plane server.
pub struct ControlServer {
    local_addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), ControlServerError>>,
}

impl ControlServer {
    /// Binds the Control Plane TCP listener before returning.
    ///
    /// The listener uses the node's base port; Gossip remains on its UDP
    /// base-port-plus-one endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`ControlServerError::Server`] if the TCP address cannot be bound.
    pub async fn bind(address: SocketAddr) -> Result<Self, ControlServerError> {
        let listener = TcpListener::bind(address)
            .await
            .map_err(|error| ControlServerError::Server(error.to_string()))?;
        let local_addr = listener
            .local_addr()
            .map_err(|error| ControlServerError::Server(error.to_string()))?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            serve(listener, router())
                .with_graceful_shutdown(async {
                    let _shutdown_result = shutdown_rx.await;
                })
                .await
                .map_err(|error| ControlServerError::Server(error.to_string()))
        });

        tracing::debug!(%local_addr, "Control Plane TCP server bound");
        Ok(Self {
            local_addr,
            shutdown: Some(shutdown_tx),
            task,
        })
    }

    /// Returns the actual TCP address, including an OS-assigned port when bound to 0.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stops the HTTP task and waits for its resources to be released.
    ///
    /// # Errors
    ///
    /// Returns an error if the server task fails or cannot be joined.
    pub async fn shutdown(mut self) -> Result<(), ControlServerError> {
        if let Some(shutdown) = self.shutdown.take() {
            let _send_result = shutdown.send(());
        }
        self.task.await.map_err(ControlServerError::Join)?
    }
}

fn router() -> Router {
    Router::new()
        .route("/submit_graph", post(submit_graph))
        .layer(DefaultBodyLimit::max(GRAPH_BODY_LIMIT_BYTES))
        .layer(middleware::from_fn(request_timeout))
}

async fn request_timeout(request: Request, next: Next) -> Response {
    match timeout(GRAPH_READ_TIMEOUT, next.run(request)).await {
        Ok(response) => response,
        Err(_) => (
            StatusCode::REQUEST_TIMEOUT,
            Json(ErrorResponse {
                error: "request body read timed out".to_owned(),
            }),
        )
            .into_response(),
    }
}

async fn submit_graph(
    payload: Result<Json<SubmitGraphRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let payload = match payload {
        Ok(Json(payload)) => payload,
        Err(error) => return bad_request(format!("invalid graph JSON: {error}")),
    };

    if let Err(error) = payload.validate() {
        return bad_request(error.to_owned());
    }

    let graph_id = Uuid::new_v4();
    tracing::info!(%graph_id, client_id = %payload.client_id, "graph accepted for validation");
    (
        StatusCode::ACCEPTED,
        Json(SubmitGraphResponse {
            graph_id,
            status: "ACCEPTED_FOR_VALIDATION",
        }),
    )
        .into_response()
}

fn bad_request(error: String) -> Response {
    (StatusCode::BAD_REQUEST, Json(ErrorResponse { error })).into_response()
}

#[cfg(test)]
mod tests {
    use super::SubmitGraphRequest;
    use serde_json::json;

    #[test]
    fn accepts_structurally_valid_graph() {
        let request = SubmitGraphRequest {
            client_id: "app_bi_01".to_owned(),
            nodes: vec![json!({"id": "source"})],
            edges: Vec::new(),
        };
        assert!(request.validate().is_ok());
    }

    #[test]
    fn rejects_empty_client_or_nodes() {
        let empty_client = SubmitGraphRequest {
            client_id: " ".to_owned(),
            nodes: vec![json!({})],
            edges: Vec::new(),
        };
        assert_eq!(empty_client.validate(), Err("client_id must not be empty"));

        let empty_nodes = SubmitGraphRequest {
            client_id: "client".to_owned(),
            nodes: Vec::new(),
            edges: Vec::new(),
        };
        assert_eq!(
            empty_nodes.validate(),
            Err("nodes must contain at least one node")
        );
    }
}
