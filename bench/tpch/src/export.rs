use crate::data_model::BenchmarkSuiteSummary;
use anyhow::Result;
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::Path;

pub fn export_summary_files(summary: &BenchmarkSuiteSummary, base_json_path: &str) -> Result<()> {
    let json_path = Path::new(base_json_path);
    if let Some(parent) = json_path.parent() {
        create_dir_all(parent)?;
    }

    // 1. Export JSON
    let json_data = serde_json::to_string_pretty(summary)?;
    let mut f_json = File::create(json_path)?;
    f_json.write_all(json_data.as_bytes())?;

    // 2. Export CSV
    let csv_path = json_path.with_extension("csv");
    let mut f_csv = File::create(csv_path)?;

    writeln!(
        f_csv,
        "query,kineplex_ms,duckdb_ms,spark_ms,speedup_vs_duckdb,speedup_vs_spark,kineplex_rss_mb,duckdb_rss_mb,spark_rss_mb"
    )?;

    for q in &summary.query_results {
        writeln!(
            f_csv,
            "{},{:.2},{:.2},{:.2},{:.2}x,{:.2}x,{:.1},{:.1},{:.1}",
            q.query,
            q.kineplex_time_ms,
            q.duckdb_time_ms,
            q.spark_time_ms,
            q.speedup_vs_duckdb,
            q.speedup_vs_spark,
            q.kineplex_peak_rss_mb,
            q.duckdb_peak_rss_mb,
            q.spark_peak_rss_mb
        )?;
    }

    writeln!(
        f_csv,
        "GEOMEAN,{:.2},{:.2},{:.2},{:.2}x,{:.2}x,-,-,-",
        summary.geomean_kineplex_ms,
        summary.geomean_duckdb_ms,
        summary.geomean_spark_ms,
        summary.overall_speedup_vs_duckdb,
        summary.overall_speedup_vs_spark
    )?;

    Ok(())
}
