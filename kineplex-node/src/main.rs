use std::fmt::Display;
use std::io::Write;
use std::process::ExitCode;

use kineplex_core::initialize_node_config;
use kineplex_core::observability::init_tracing;
use kineplex_node::cli;

fn main() -> ExitCode {
    let tracing_guard = match init_tracing() {
        Ok(guard) => guard,
        Err(error) => {
            write_boot_error(&error);
            return ExitCode::FAILURE;
        }
    };

    let exit_code = run();
    drop(tracing_guard);
    exit_code
}

fn run() -> ExitCode {
    let parsed_config = match cli::parse() {
        Ok(config) => config,
        Err(error) => return print_clap_error(&error),
    };

    let config = match initialize_node_config(parsed_config) {
        Ok(config) => config,
        Err(error) => {
            tracing::error!(error = %error, "failed to initialize KinePlex node");
            return ExitCode::FAILURE;
        }
    };

    tracing::info!(
        bind_addr = %config.bind_addr(),
        seeds = ?config.seeds,
        seed_count = config.seeds.len(),
        "Starting KinePlex Node"
    );

    ExitCode::SUCCESS
}

fn print_clap_error(error: &clap::Error) -> ExitCode {
    let exit_code = match error.exit_code() {
        0 => ExitCode::SUCCESS,
        2 => ExitCode::from(2),
        unexpected => {
            tracing::error!(
                exit_code = unexpected,
                "clap returned an unexpected exit code"
            );
            ExitCode::FAILURE
        }
    };

    if let Err(print_error) = error.print() {
        tracing::error!(error = %print_error, "failed to print command-line diagnostic");
        return ExitCode::FAILURE;
    }

    exit_code
}

fn write_boot_error(error: &impl Display) {
    let mut stderr = std::io::stderr().lock();
    let _write_result = writeln!(stderr, "error: failed to initialize tracing: {error}");
}
