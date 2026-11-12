use crate::data_model::BenchmarkSuiteSummary;
use crate::whitepaper_sections::generate_whitepaper_markdown;
use crate::WhitepaperPublisher;
use anyhow::Result;
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::Path;

pub struct WhitepaperPublisherEngine;

impl WhitepaperPublisherEngine {
    pub fn new() -> Self {
        Self
    }
}

impl Default for WhitepaperPublisherEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl WhitepaperPublisher for WhitepaperPublisherEngine {
    fn compile_whitepaper(
        &self,
        summary_1tb: &BenchmarkSuiteSummary,
        summary_10tb: Option<&BenchmarkSuiteSummary>,
        output_markdown_path: &str,
    ) -> Result<()> {
        let md = generate_whitepaper_markdown(summary_1tb, summary_10tb);
        let path = Path::new(output_markdown_path);
        if let Some(parent) = path.parent() {
            create_dir_all(parent)?;
        }
        let mut file = File::create(path)?;
        file.write_all(md.as_bytes())?;
        Ok(())
    }
}
