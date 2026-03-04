//! Error types for the Kademlia DHT.

use std::fmt;
use std::io;

/// Errors that can occur during DHT operations.
#[derive(Debug)]
pub enum Error {
    /// An I/O error from the underlying transport.
    Io(io::Error),
    /// A wire protocol encoding or decoding error.
    Protocol(String),
    /// An RPC request timed out.
    Timeout,
    /// The requested key was not found in the DHT.
    NotFound,
    /// The value exceeds `MAX_VALUE_SIZE` (65 000
    /// bytes).
    ValueTooLarge,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Protocol(msg) => {
                write!(f, "protocol error: {msg}")
            }
            Error::Timeout => write!(f, "request timed out"),
            Error::NotFound => write!(f, "value not found"),
            Error::ValueTooLarge => {
                write!(f, "value exceeds maximum size")
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

/// A `Result` alias using [`Error`] as the error type.
pub type Result<T> = std::result::Result<T, Error>;

/// Lock a mutex, recovering from poisoning.
///
/// If a thread panicked while holding the lock, the
/// data is still accessible. We treat poisoning as
/// non-fatal since our lock-guarded operations are
/// short and unlikely to leave inconsistent state.
pub fn lock<T>(
    m: &std::sync::Mutex<T>,
) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}
