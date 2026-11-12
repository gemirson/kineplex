use clap::Parser;
use kineplex_bench::config::{BenchmarkEngine, BenchmarkQueryType, BenchmarkRunConfig};
use kineplex_bench::runner_kineplex::KineplexQueryExecutor;
use kineplex_bench::TpchQueryExecutor;

#[derive(Parser, Debug)]
#[command(author, version, about = "KinePlex Native TPC-H Runner")]
struct Args {
    #[arg(short, long, default_value_t = 1)]
    scale: u64,

    #[arg(short, long, default_value = "/tmp/tpch_data")]
    data_dir: String,

    #[arg(long, default_value_t = 1)]
    warmup: usize,

    #[arg(short, long, default_value_t = 5)]
    runs: usize,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    println!("=== KinePlex Native TPC-H Runner ===");
    println!("Scale Factor: {}", args.scale);

    let config = BenchmarkRunConfig {
        scale_factor: args.scale,
        engine: BenchmarkEngine::KinePlex,
        queries: (1..=22).map(BenchmarkQueryType::Tpch).collect(),
        warmup_runs: args.warmup,
        measurement_runs: args.runs,
        data_directory: args.data_dir,
        ..Default::default()
    };

    let executor = KineplexQueryExecutor::new(config)?;
    let queries: Vec<_> = (1..=22).map(BenchmarkQueryType::Tpch).collect();
    let metrics = executor.execute_all_queries(&queries)?;

    println!("\n| Query | Median Time (ms) | Std Dev (ms) | Throughput (GB/s) | Peak RSS (MB) |");
    println!("|---|---|---|---|---|");
    for m in &metrics {
        println!(
            "| {} | {:.2} | {:.2} | {:.2} | {:.1} |",
            m.query,
            m.median_time_ms,
            m.std_dev_ms,
            m.throughput_gb_per_sec,
            m.peak_rss_bytes as f64 / (1024.0 * 1024.0)
        );
    }

    Ok(())
}
