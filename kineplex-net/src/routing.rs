//! Routing between nodes in the cluster
//! 
//! This module provides routing logic for:
//! - Direct routing to nodes
//! - Multi-hop routing
//! - Route optimization

use crate::{NetNodeId, NetworkAddress};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use parking_lot::RwLock;

/// Routing table entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Route {
    pub target: NetNodeId,
    pub next_hop: Option<NetNodeId>,
    pub distance: u32,
    pub address: NetworkAddress,
}

/// Router for cluster communication
pub struct Router {
    self_node: NetNodeId,
    routes: RwLock<HashMap<NetNodeId, Route>>,
    direct_peers: RwLock<Vec<NetNodeId>>,
}

impl Router {
    pub fn new(self_node: NetNodeId) -> Self {
        Self {
            self_node,
            routes: RwLock::new(HashMap::new()),
            direct_peers: RwLock::new(Vec::new()),
        }
    }
    
    /// Add a direct peer
    pub fn add_peer(&self, node_id: NetNodeId, address: NetworkAddress) {
        let route = Route {
            target: node_id.clone(),
            next_hop: None, // Direct connection
            distance: 1,
            address,
        };
        
        self.routes.write().insert(node_id.clone(), route);
        self.direct_peers.write().push(node_id);
    }
    
    /// Add a route through another node
    pub fn add_route(&self, target: NetNodeId, next_hop: NetNodeId, distance: u32, address: NetworkAddress) {
        let route = Route {
            target: target.clone(),
            next_hop: Some(next_hop),
            distance,
            address,
        };
        
        self.routes.write().insert(target, route);
    }
    
    /// Get route to a target node
    pub fn get_route(&self, target: &NetNodeId) -> Option<Route> {
        self.routes.read().get(target).cloned()
    }
    
    /// Get address for a node
    pub fn get_address(&self, target: &NetNodeId) -> Option<NetworkAddress> {
        self.routes.read().get(target).map(|r| r.address.clone())
    }
    
    /// Get all direct peers
    pub fn get_peers(&self) -> Vec<NetNodeId> {
        self.direct_peers.read().clone()
    }
    
    /// Calculate best route (simplified routing)
    pub fn calculate_route(&self, target: &NetNodeId) -> Option<NetNodeId> {
        let routes = self.routes.read();
        
        if let Some(route) = routes.get(target) {
            // Direct connection
            if route.distance == 1 {
                return Some(target.clone());
            }
            // Multi-hop
            route.next_hop.clone()
        } else {
            None
        }
    }
    
    /// Update routes based on gossip state
    pub fn update_routes(&self, members: Vec<(NetNodeId, NetworkAddress)>) {
        let mut routes = self.routes.write();
        
        for (node_id, address) in members {
            if node_id != self.self_node {
                let route = Route {
                    target: node_id.clone(),
                    next_hop: None,
                    distance: 1,
                    address,
                };
                routes.insert(node_id, route);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_router() {
        let router = Router::new(NetNodeId::new());
        
        let peer_id = NetNodeId::new();
        router.add_peer(peer_id.clone(), NetworkAddress::new("localhost", 8080));
        
        let route = router.get_route(&peer_id);
        assert!(route.is_some());
        assert_eq!(route.unwrap().distance, 1);
    }
}



#[cfg(test)]
mod coverage_tests {
    use super::*;

    #[test]
    fn router_handles_direct_and_multihop_routes() {
        let self_node = NetNodeId::new();
        let router = Router::new(self_node.clone());
        let peer = NetNodeId::new();
        let target = NetNodeId::new();
        let address = NetworkAddress::new("localhost", 9000);
        router.add_peer(peer.clone(), address.clone());
        assert_eq!(router.get_peers(), vec![peer.clone()]);
        assert_eq!(router.calculate_route(&peer), Some(peer.clone()));
        assert_eq!(router.get_address(&peer).unwrap().to_string(), "localhost:9000");
        router.add_route(target.clone(), peer.clone(), 2, address.clone());
        assert_eq!(router.calculate_route(&target), Some(peer));
        assert_eq!(router.get_route(&target).unwrap().distance, 2);
        assert!(router.get_route(&NetNodeId::new()).is_none());
        assert!(router.calculate_route(&NetNodeId::new()).is_none());
        router.update_routes(vec![(self_node.clone(), address.clone()), (NetNodeId::new(), address)]);
        assert_eq!(router.get_peers().len(), 1);
    }
}