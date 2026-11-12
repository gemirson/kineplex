use clap::Parser;
use kineplex_bench::datagen::TpchDataGenerator;

#[derive(Parser, Debug)]
#[command(author, version, about = "TPC-H synthetic data generator with Laguerre CCVT partitioning")]
struct Args {
    #[arg(short, long, default_value_t = 1)]
    scale: u64,

    #[arg(short, long, default_value = "/tmp/tpch_data")]
    out_dir: String,

    #[arg(long, default_value_t = true)]
    laguerre_partition: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    println!("=== KinePlex TPC-H DataGen ===");
    println!("Scale Factor: {}", args.scale);
    println!("Output Dir: {}", args.out_dir);
    println!("Laguerre CCVT Partition: {}", args.laguerre_partition);

    let gen = TpchDataGenerator::new(args.scale, &args.out_dir, args.laguerre_partition);
    gen.generate_all_tables()?;

    println!("Data generation complete.");
    Ok(())
}
