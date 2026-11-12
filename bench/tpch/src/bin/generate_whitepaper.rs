use clap::Parser;
use kineplex_bench::data_model::BenchmarkSuiteSummary;
use kineplex_bench::whitepaper::WhitepaperPublisherEngine;
use kineplex_bench::WhitepaperPublisher;
use std::fs::File;
use std::io::Read;

#[derive(Parser, Debug)]
#[command(author, version, about = "Compile and generate the scientific whitepaper from benchmark results")]
struct Args {
    #[arg(short, long, default_value = "./reports/tpch_results.json")]
    results_json: String,

    #[arg(short, long, default_value = "docs/whitepaper/kineplex_analytical_superiority.md")]
    output: String,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    println!("=== KinePlex Whitepaper Generator ===");
    println!("Reading results: {}", args.results_json);

    let mut file = File::open(&args.results_json)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    let summary: BenchmarkSuiteSummary = serde_json::from_str(&contents)?;

    let publisher = WhitepaperPublisherEngine::new();
    publisher.compile_whitepaper(&summary, None, &args.output)?;

    println!("Scientific Whitepaper compiled successfully to: {}", args.output);
    Ok(())
}
