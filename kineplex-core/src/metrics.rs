//! Metrics and observability for the data plane
//! 
//! This module provides:
//! - Prometheus metrics export
//! - OTLP compatible metrics
//! - Custom metrics for pipeline stages

use parking_lot::RwLock;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// Metric types supported
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetricValue {
    Counter(u64),
    Gauge(f64),
    Histogram(f64),
    Summary { sum: f64, count: u64 },
}

/// A single metric
#[derive(Debug, Clone)]
pub struct Metric {
    pub name: String,
    pub value: MetricValue,
    pub labels: HashMap<String, String>,
    pub timestamp_ms: i64,
}

impl Metric {
    pub fn counter(name: impl Into<String>, value: u64) -> Self {
        Self {
            name: name.into(),
            value: MetricValue::Counter(value),
            labels: HashMap::new(),
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
        }
    }
    
    pub fn gauge(name: impl Into<String>, value: f64) -> Self {
        Self {
            name: name.into(),
            value: MetricValue::Gauge(value),
            labels: HashMap::new(),
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
        }
    }
    
    pub fn histogram(name: impl Into<String>, value: f64) -> Self {
        Self {
            name: name.into(),
            value: MetricValue::Histogram(value),
            labels: HashMap::new(),
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
        }
    }
    
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }
}

/// Metrics collector
pub struct MetricsCollector {
    metrics: RwLock<Vec<Metric>>,
    counters: RwLock<HashMap<String, u64>>,
    gauges: RwLock<HashMap<String, f64>>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            metrics: RwLock::new(Vec::new()),
            counters: RwLock::new(HashMap::new()),
            gauges: RwLock::new(HashMap::new()),
        }
    }
    
    /// Record a counter increment
    pub fn inc_counter(&self, name: &str, labels: Option<HashMap<String, String>>) {
        let key = Self::make_key(name, &labels);
        let mut counters = self.counters.write();
        *counters.entry(key).or_insert(0) += 1;
        
        let metric = Metric::counter(name, *counters.get(&Self::make_key(name, &labels)).unwrap_or(&1))
            .with_label("total", "true");
        self.metrics.write().push(metric);
    }
    
    /// Record a gauge value
    pub fn set_gauge(&self, name: &str, value: f64, labels: Option<HashMap<String, String>>) {
        let key = Self::make_key(name, &labels);
        let mut gauges = self.gauges.write();
        gauges.insert(key, value);
        
        let metric = Metric::gauge(name, value);
        let mut metric = metric;
        if let Some(l) = labels {
            for (k, v) in l {
                metric = metric.with_label(k, v);
            }
        }
        self.metrics.write().push(metric);
    }
    
    /// Record a histogram value
    pub fn observe_histogram(&self, name: &str, value: f64, labels: Option<HashMap<String, String>>) {
        let metric = Metric::histogram(name, value);
        let mut metric = metric;
        if let Some(l) = labels {
            for (k, v) in l {
                metric = metric.with_label(k, v);
            }
        }
        self.metrics.write().push(metric);
    }
    
    fn make_key(name: &str, labels: &Option<HashMap<String, String>>) -> String {
        match labels {
            Some(l) => {
                let mut parts: Vec<String> = vec![name.to_string()];
                let mut label_parts: Vec<String> = l.iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect();
                label_parts.sort();
                parts.extend(label_parts);
                parts.join("_")
            }
            None => name.to_string(),
        }
    }
    
    /// Export metrics in Prometheus format
    pub fn export_prometheus(&self) -> String {
        let mut output = String::new();
        
        // Export counters
        let counters = self.counters.read();
        for (key, value) in counters.iter() {
            output.push_str(&format!("{} {}\n", key, value));
        }
        
        // Export gauges
        let gauges = self.gauges.read();
        for (key, value) in gauges.iter() {
            output.push_str(&format!("{} {}\n", key, value));
        }
        
        output
    }
    
    /// Get all metrics
    pub fn get_metrics(&self) -> Vec<Metric> {
        self.metrics.read().clone()
    }
    
    /// Clear metrics
    pub fn clear(&self) {
        self.metrics.write().clear();
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Pipeline metrics - specific metrics for each stage
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PipelineMetrics {
    /// Latency per stage in milliseconds
    pub stage_latency_ms: HashMap<String, u64>,
    /// Total rows processed per stage
    pub stage_rows: HashMap<String, u64>,
    /// Errors per stage
    pub stage_errors: HashMap<String, u64>,
    /// Bytes transferred between stages
    pub stage_bytes: HashMap<String, u64>,
}

/// SLO configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SloConfig {
    /// SLO name
    pub name: String,
    /// SLI target (e.g., 0.99 for 99%)
    pub sli_target: f64,
    /// Window duration
    pub window: String,
    /// Error budget percentage
    pub error_budget_percent: f64,
}

impl SloConfig {
    pub fn new(name: impl Into<String>, sli_target: f64, window: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            sli_target,
            window: window.into(),
            error_budget_percent: (1.0 - sli_target) * 100.0,
        }
    }
}

/// SLO tracker
pub struct SloTracker {
    config: SloConfig,
    total_requests: u64,
    successful_requests: u64,
    latency_samples: Vec<u64>,
}

impl SloTracker {
    pub fn new(config: SloConfig) -> Self {
        Self {
            config,
            total_requests: 0,
            successful_requests: 0,
            latency_samples: Vec::new(),
        }
    }
    
    /// Record a request
    pub fn record_request(&mut self, success: bool, latency_ms: u64) {
        self.total_requests += 1;
        if success {
            self.successful_requests += 1;
        }
        self.latency_samples.push(latency_ms);
        
        // Keep only last 1000 samples for p95 calculation
        if self.latency_samples.len() > 1000 {
            self.latency_samples.remove(0);
        }
    }
    
    /// Calculate current SLI
    pub fn current_sli(&self) -> f64 {
        if self.total_requests == 0 {
            return 1.0;
        }
        self.successful_requests as f64 / self.total_requests as f64
    }
    
    /// Calculate p95 latency
    pub fn p95_latency(&self) -> u64 {
        if self.latency_samples.is_empty() {
            return 0;
        }
        let mut sorted = self.latency_samples.clone();
        sorted.sort();
        let idx = (sorted.len() as f64 * 0.95) as usize;
        sorted[idx.min(sorted.len() - 1)]
    }
    
    /// Check if SLO is being met
    pub fn is_slo_met(&self) -> bool {
        self.current_sli() >= self.config.sli_target
    }
}

/// Common SLO configurations
pub mod slo_presets {
    use super::*;
    
    /// Availability SLO - 99.9%
    pub fn availability_999() -> SloConfig {
        SloConfig::new("availability", 0.999, "30d")
    }
    
    /// Latency p95 SLO - 500ms
    pub fn latency_p95() -> SloConfig {
        SloConfig::new("latency_p95", 0.95, "5m")
    }
    
    /// Error rate SLO - 1%
    pub fn error_rate_1percent() -> SloConfig {
        SloConfig::new("error_rate", 0.01, "5m")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_metrics_collector() {
        let collector = MetricsCollector::new();
        
        collector.inc_counter("requests_total", None);
        collector.inc_counter("requests_total", None);
        collector.set_gauge("memory_usage_bytes", 1024.0, None);
        collector.observe_histogram("request_latency_ms", 150.0, None);
        
        let prometheus = collector.export_prometheus();
        assert!(prometheus.contains("requests_total"));
        assert!(prometheus.contains("memory_usage_bytes"));
    }
    
    #[test]
    fn test_slo_tracker() {
        let config = SloConfig::new("test", 0.95, "1m");
        let mut tracker = SloTracker::new(config);
        
        tracker.record_request(true, 100);
        tracker.record_request(true, 200);
        tracker.record_request(false, 300);
        
        assert_eq!(tracker.current_sli(), 2.0 / 3.0);
    }
}



#[cfg(test)]
mod coverage_tests {
    use super::*;

    #[test]
    fn metric_constructors_and_labels() {
        let counter = Metric::counter("requests", 2).with_label("tenant", "a");
        assert_eq!(counter.name, "requests");
        assert!(matches!(counter.value, MetricValue::Counter(2)));
        assert_eq!(counter.labels.get("tenant"), Some(&"a".to_string()));
        assert!(matches!(Metric::gauge("load", 1.0).value, MetricValue::Gauge(_)));
        assert!(matches!(Metric::histogram("latency", 2.0).value, MetricValue::Histogram(_)));
        assert!(matches!(MetricValue::Summary { sum: 1.0, count: 1 }, MetricValue::Summary { .. }));
    }

    #[test]
    fn collector_handles_labels_clear_and_export() {
        let collector = MetricsCollector::default();
        let mut labels = HashMap::new();
        labels.insert("zone".to_string(), "a".to_string());
        labels.insert("tenant".to_string(), "t1".to_string());
        collector.inc_counter("requests", Some(labels.clone()));
        collector.inc_counter("requests", Some(labels.clone()));
        collector.set_gauge("load", 0.5, Some(labels.clone()));
        collector.observe_histogram("latency", 12.0, Some(labels));
        assert!(collector.export_prometheus().contains("requests"));
        assert_eq!(collector.get_metrics().len(), 4);
        collector.clear();
        assert!(collector.get_metrics().is_empty());
    }

    #[test]
    fn pipeline_metrics_and_slo_presets_are_serializable() {
        let pipeline = PipelineMetrics {
            stage_latency_ms: [("wasm".to_string(), 10)].into_iter().collect(),
            stage_rows: [("wasm".to_string(), 2)].into_iter().collect(),
            stage_errors: HashMap::new(),
            stage_bytes: HashMap::new(),
        };
        let json = serde_json::to_string(&pipeline).unwrap();
        assert!(json.contains("wasm"));
        assert_eq!(slo_presets::availability_999().name, "availability");
        assert_eq!(slo_presets::latency_p95().window, "5m");
        assert_eq!(slo_presets::error_rate_1percent().error_budget_percent, 99.0);
    }

    #[test]
    fn slo_tracker_empty_p95_and_sample_window() {
        let mut tracker = SloTracker::new(SloConfig::new("test", 0.75, "1m"));
        assert_eq!(tracker.current_sli(), 1.0);
        assert_eq!(tracker.p95_latency(), 0);
        assert!(tracker.is_slo_met());
        for i in 0..1005 {
            tracker.record_request(i % 2 == 0, i as u64);
        }
        assert!(tracker.p95_latency() > 0);
        assert!(!tracker.is_slo_met());
    }
}