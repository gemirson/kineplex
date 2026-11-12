use clap::Parser;
use kineplex_bench::comparative::ComparativeBenchmarkEngine;
use kineplex_bench::config::{BenchmarkQueryType, BenchmarkRunConfig};
use kineplex_bench::ComparativeBenchmarkRunner;

#[derive(Parser, Debug)]
#[command(author, version, about = "Comparative Benchmark Runner: KinePlex vs DuckDB vs Apache Spark")]
struct Args {
    #[arg(short, long, default_value_t = 1)]
    scale: u64,

    #[arg(short, long, default_value = "/tmp/tpch_data")]
    data_dir: String,

    #[arg(short, long, default_value = "./reports/tpch_results.json")]
    output_json: String,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    println!("=== Comparative Benchmark Runner ===");
    println!("Comparing: KinePlex vs DuckDB vs Apache Spark");
    println!("Scale: {}", args.scale);

    let config = BenchmarkRunConfig {
        scale_factor: args.scale,
        queries: (1..=22).map(BenchmarkQueryType::Tpch).collect(),
        data_directory: args.data_dir,
        output_directory: "./reports".to_string(),
        ..Default::default()
    };

    let engine = ComparativeBenchmarkEngine::new();
    let summary = engine.run_comparative_suite(&config)?;

    println!("\nBenchmark Suite Completed!");
    println!("Overall Speedup vs DuckDB: {:.2}x", summary.overall_speedup_vs_duckdb);
    println!("Overall Speedup vs Apache Spark: {:.2}x", summary.overall_speedup_vs_spark);

    engine.export_results_json(&summary, &args.output_json)?;
    println!("Results saved to {}", args.output_json);

    Ok(())
}
