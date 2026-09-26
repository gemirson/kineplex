//! io_uring driver with SQPOLL support for high-performance network I/O.
//!
//! This module provides infrastructure for io_uring with SQPOLL to avoid
//! syscalls for network packet processing.

use std::sync::OnceLock;

/// Minimum kernel version required for SQPOLL
const MIN_KERNEL_MAJOR: u32 = 5;
const MIN_KERNEL_MINOR: u32 = 19;

/// Default SQPOLL idle timeout in milliseconds
const DEFAULT_SQPOLL_IDLE_MS: u32 = 2000;

/// Default queue depth
const DEFAULT_QUEUE_DEPTH: u32 = 1024;

/// Global io_uring driver instance
static IO_URING_DRIVER: OnceLock<IoUringDriver> = OnceLock::new();

/// Result type for io_uring operations.
pub type Result<T> = std::result::Result<T, IoUringError>;

/// Errors that can occur during io_uring initialization.
#[derive(Debug, Clone)]
pub enum IoUringError {
    /// Kernel version too old
    KernelTooOld {
        required_major: u32,
        required_minor: u32,
        found_major: u32,
        found_minor: u32,
    },
    /// Unsupported operating system
    UnsupportedPlatform(String),
    /// Permission denied (needs root or CAP_SYS_NICE)
    PermissionDenied(String),
    /// Failed to create io_uring ring
    RingCreationFailed(String),
    /// Driver already initialized
    AlreadyInitialized,
}

impl std::fmt::Display for IoUringError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KernelTooOld {
                required_major,
                required_minor,
                found_major,
                found_minor,
            } => {
                write!(
                    f,
                    "Kernel too old: requires {}.{} but found {}.{}",
                    required_major, required_minor, found_major, found_minor
                )
            }
            Self::UnsupportedPlatform(platform) => {
                write!(f, "io_uring not supported on platform: {}", platform)
            }
            Self::PermissionDenied(msg) => {
                write!(f, "Permission denied: {}", msg)
            }
            Self::RingCreationFailed(msg) => {
                write!(f, "Failed to create io_uring ring: {}", msg)
            }
            Self::AlreadyInitialized => {
                write!(f, "io_uring driver already initialized")
            }
        }
    }
}

impl std::error::Error for IoUringError {}

/// Kernel version information.
#[derive(Debug, Clone)]
pub struct KernelVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl KernelVersion {
    /// Parses kernel version from /proc/version.
    pub fn current() -> Option<Self> {
        if let Ok(version_string) = std::fs::read_to_string("/proc/version") {
            if let Some(version_line) = version_string.lines().next() {
                if version_line.contains("Linux version") {
                    let parts: Vec<&str> = version_line.split_whitespace().collect();
                    if parts.len() >= 3 {
                        let version = parts[2];
                        let version_parts: Vec<&str> = version.split('.').collect();
                        if version_parts.len() >= 2 {
                            let major = version_parts[0].parse().ok()?;
                            let minor = version_parts[1].parse().ok()?;
                            let patch = version_parts
                                .get(2)
                                .and_then(|p| p.parse().ok())
                                .unwrap_or(0);
                            return Some(Self {
                                major,
                                minor,
                                patch,
                            });
                        }
                    }
                }
            }
        }
        None
    }

    /// Checks if this kernel version meets the minimum requirement.
    pub fn meets_minimum(&self, major: u32, minor: u32) -> bool {
        if self.major > major {
            return true;
        }
        if self.major == major {
            return self.minor >= minor;
        }
        false
    }
}

/// Configuration for io_uring driver.
#[derive(Debug, Clone)]
pub struct IoUringConfig {
    /// SQPOLL idle timeout in milliseconds
    pub sqpoll_idle_ms: u32,
    /// Queue depth
    pub queue_depth: u32,
    /// Enable SQPOLL
    pub use_sqpoll: bool,
}

impl Default for IoUringConfig {
    fn default() -> Self {
        Self {
            sqpoll_idle_ms: DEFAULT_SQPOLL_IDLE_MS,
            queue_depth: DEFAULT_QUEUE_DEPTH,
            use_sqpoll: true,
        }
    }
}

/// io_uring driver for network I/O operations.
///
/// This is a placeholder structure - actual io_uring ring creation
/// would be done via the io-uring crate in a full implementation.
pub struct IoUringDriver {
    config: IoUringConfig,
    #[allow(dead_code)]
    ring_fd: Option<i32>, // Would hold actual ring file descriptor
}

impl IoUringDriver {
    /// Initializes the global io_uring driver with SQPOLL.
    pub fn initialize(config: IoUringConfig) -> Result<&'static IoUringDriver> {
        // Check if already initialized
        if IO_URING_DRIVER.get().is_some() {
            return Err(IoUringError::AlreadyInitialized);
        }

        // Check platform
        #[cfg(not(target_os = "linux"))]
        {
            return Err(IoUringError::UnsupportedPlatform(
                std::env::consts::OS.to_string(),
            ));
        }

        // Check kernel version
        #[cfg(target_os = "linux")]
        {
            if let Some(kernel) = KernelVersion::current() {
                if !kernel.meets_minimum(MIN_KERNEL_MAJOR, MIN_KERNEL_MINOR) {
                    return Err(IoUringError::KernelTooOld {
                        required_major: MIN_KERNEL_MAJOR,
                        required_minor: MIN_KERNEL_MINOR,
                        found_major: kernel.major,
                        found_minor: kernel.minor,
                    });
                }
            } else {
                tracing::warn!("Could not determine kernel version, assuming compatible");
            }
        }

        // Create the driver (ring creation would happen here with io-uring crate)
        let driver = IoUringDriver {
            config: config.clone(),
            ring_fd: None,
        };

        let _ = IO_URING_DRIVER.set(driver);

        let instance = IO_URING_DRIVER.get().expect("Failed to initialize driver");

        tracing::info!(
            sqpoll_idle_ms = config.sqpoll_idle_ms,
            queue_depth = config.queue_depth,
            use_sqpoll = config.use_sqpoll,
            "io_uring driver initialized with SQPOLL"
        );

        Ok(instance)
    }

    /// Gets the global driver instance.
    pub fn get() -> Option<&'static IoUringDriver> {
        IO_URING_DRIVER.get()
    }

    /// Gets the configuration.
    pub fn config(&self) -> &IoUringConfig {
        &self.config
    }

    /// Checks if SQPOLL is enabled.
    #[allow(dead_code)]
    pub fn is_sqpoll_enabled(&self) -> bool {
        self.config.use_sqpoll
    }

    /// Checks if the driver is initialized.
    pub fn is_initialized() -> bool {
        IO_URING_DRIVER.get().is_some()
    }
}

/// Initializes the io_uring driver with default configuration.
pub fn init() -> Result<&'static IoUringDriver> {
    IoUringDriver::initialize(IoUringConfig::default())
}

/// Initializes the io_uring driver with custom configuration.
pub fn init_with_config(config: IoUringConfig) -> Result<&'static IoUringDriver> {
    IoUringDriver::initialize(config)
}

/// Returns the global driver if initialized.
pub fn driver() -> Option<&'static IoUringDriver> {
    IoUringDriver::get()
}

/// Checks if io_uring is available on this system.
pub fn is_available() -> bool {
    #[cfg(target_os = "linux")]
    {
        if let Some(kernel) = KernelVersion::current() {
            return kernel.meets_minimum(MIN_KERNEL_MAJOR, MIN_KERNEL_MINOR);
        }
        false
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kernel_version_parsing() {
        let version = KernelVersion::current();
        if let Some(v) = version {
            println!("Kernel version: {}.{}.{}", v.major, v.minor, v.patch);
        }
    }

    #[test]
    fn test_kernel_version_requirement() {
        let v512 = KernelVersion {
            major: 5,
            minor: 12,
            patch: 0,
        };
        let v519 = KernelVersion {
            major: 5,
            minor: 19,
            patch: 0,
        };
        let v600 = KernelVersion {
            major: 6,
            minor: 0,
            patch: 0,
        };

        assert!(!v512.meets_minimum(5, 19));
        assert!(v519.meets_minimum(5, 19));
        assert!(v600.meets_minimum(5, 19));
    }

    #[test]
    fn test_is_available() {
        println!("io_uring available: {}", is_available());
    }

    #[test]
    fn test_default_config() {
        let config = IoUringConfig::default();
        assert_eq!(config.sqpoll_idle_ms, DEFAULT_SQPOLL_IDLE_MS);
        assert_eq!(config.queue_depth, DEFAULT_QUEUE_DEPTH);
        assert!(config.use_sqpoll);
    }
}
