//! Optional native CPU sampling and SVG flamegraph extraction.

use pprof::ProfilerGuard;

/// Process-lifetime CPU profiler guard.
pub struct CpuProfiler {
    guard: ProfilerGuard<'static>,
}

impl CpuProfiler {
    /// Starts the native sampler at the requested samples per second.
    pub fn start(frequency: i32) -> Result<Self, String> {
        ProfilerGuard::new(frequency)
            .map(|guard| Self { guard })
            .map_err(|error| error.to_string())
    }

    /// Captures an SVG flamegraph for samples collected so far.
    pub fn flamegraph(&self) -> Result<Vec<u8>, String> {
        let report = self
            .guard
            .report()
            .build()
            .map_err(|error| error.to_string())?;
        let mut svg = Vec::new();
        report
            .flamegraph(&mut svg)
            .map_err(|error| error.to_string())?;
        Ok(svg)
    }
}
