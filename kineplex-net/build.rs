use std::env;
use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    let manifest_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("Cargo sets CARGO_MANIFEST_DIR"));
    let schema = manifest_dir.join("src/network/schema/synapse_header.fbs");
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo sets OUT_DIR"));
    let generated = out_dir.join("synapse_header_generated.rs");
    let fallback = manifest_dir.join("src/network/generated/synapse_header_generated.rs");

    println!("cargo:rerun-if-changed={}", schema.display());
    println!("cargo:rerun-if-changed={}", fallback.display());

    let input = [schema.as_path()];
    let args = flatc_rust::Args {
        lang: "rust",
        inputs: &input,
        out_dir: &out_dir,
        extra: &["--gen-all"],
        ..Default::default()
    };

    if let Err(error) = flatc_rust::run(args) {
        fs::copy(&fallback, &generated).unwrap_or_else(|copy_error| {
            panic!(
                "flatc generation failed ({error}) and fallback {} could not be copied: {copy_error}",
                fallback.display()
            )
        });
    }

    assert!(
        Path::new(&generated).exists(),
        "FlatBuffers output was not generated"
    );
}
