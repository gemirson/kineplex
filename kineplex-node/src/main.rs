use std::process::ExitCode;

use kineplex_core::initialize_node_config;
use kineplex_node::cli;

fn main() -> ExitCode {
    let parsed_config = match cli::parse() {
        Ok(config) => config,
        Err(error) => error.exit(),
    };

    let config = match initialize_node_config(parsed_config) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("error: failed to initialize KinePlex node: {error}");
            return ExitCode::FAILURE;
        }
    };

    println!(
        "Starting KinePlex Node on {}. Seeds: {:?}",
        config.bind_addr(),
        config.seeds
    );

    ExitCode::SUCCESS
}
