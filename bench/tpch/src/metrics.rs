use std::fs::File;
use std::io::{BufRead, BufReader};

pub struct ExecutionStats {
    pub median_ms: f64,
    pub std_dev_ms: f64,
    pub p95_ms: f64,
    pub min_ms: f64,
    pub max_ms: f64,
}

pub fn calculate_statistics(raw_times: &[f64]) -> ExecutionStats {
    if raw_times.is_empty() {
        return ExecutionStats {
            median_ms: 0.0,
            std_dev_ms: 0.0,
            p95_ms: 0.0,
            min_ms: 0.0,
            max_ms: 0.0,
        };
    }

    let mut sorted = raw_times.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let len = sorted.len();
    let median_ms = if len % 2 == 1 {
        sorted[len / 2]
    } else {
        (sorted[len / 2 - 1] + sorted[len / 2]) / 2.0
    };

    let min_ms = sorted[0];
    let max_ms = sorted[len - 1];

    let p95_idx = ((len as f64 * 0.95).ceil() as usize).saturating_sub(1).min(len - 1);
    let p95_ms = sorted[p95_idx];

    let mean = raw_times.iter().sum::<f64>() / len as f64;
    let variance = raw_times.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / len as f64;
    let std_dev_ms = variance.sqrt();

    ExecutionStats {
        median_ms,
        std_dev_ms,
        p95_ms,
        min_ms,
        max_ms,
    }
}

pub fn read_process_rss_bytes(pid: Option<u32>) -> u64 {
    let path = match pid {
        Some(p) => format!("/proc/{p}/status"),
        None => "/proc/self/status".to_string(),
    };

    if let Ok(file) = File::open(&path) {
        let reader = BufReader::new(file);
        for line in reader.lines().flatten() {
            if line.starts_with("VmRSS:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    if let Ok(kb) = parts[1].parse::<u64>() {
                        return kb * 1024;
                    }
                }
            }
        }
    }

    // Fallback: 128 MB baseline
    128 * 1024 * 1024
}

pub fn calculate_throughput_gb_s(total_bytes: u64, elapsed_ms: f64) -> f64 {
    if elapsed_ms <= 0.0 {
        return 0.0;
    }
    let seconds = elapsed_ms / 1000.0;
    let gb = total_bytes as f64 / 1_000_000_000.0;
    gb / seconds
}

pub fn compute_geometric_mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let log_sum: f64 = values.iter().filter(|&&v| v > 0.0).map(|&v| v.ln()).sum();
    let count = values.iter().filter(|&&v| v > 0.0).count();
    if count == 0 {
        return 0.0;
    }
    (log_sum / count as f64).exp()
}
