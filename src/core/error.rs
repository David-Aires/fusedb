/// All errors that can originate from core FuseDB operations.
///
/// This type has zero PyO3 knowledge — conversion to `PyErr` lives in
/// `crate::python::error`.
#[derive(Debug)]
#[non_exhaustive]
pub enum FuseError {
    /// The file is structurally invalid (bad magic, truncated, CRC mismatch).
    Corrupt(String),
    /// The file was written with an unsupported format version.
    Version(u8),
    /// An I/O error occurred (open, read, write, rename, fsync).
    Io(String),
    /// A caller passed an invalid argument.
    InvalidArg(String),
    /// A value could not be encoded to, or decoded from, the on-disk
    /// serialisation format (MessagePack).
    Serialization(String),
}

impl std::fmt::Display for FuseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Corrupt(m) => write!(f, "FuseCorruptError: {m}"),
            Self::Version(v) => write!(f, "FuseVersionError: unsupported version {v}"),
            Self::Io(m) => write!(f, "FuseIOError: {m}"),
            Self::InvalidArg(m) => write!(f, "FuseError: {m}"),
            Self::Serialization(m) => write!(f, "FuseSerializationError: {m}"),
        }
    }
}

impl std::error::Error for FuseError {}

impl From<std::io::Error> for FuseError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.to_string())
    }
}

#[cfg(feature = "msgpack")]
impl From<rmp_serde::encode::Error> for FuseError {
    fn from(e: rmp_serde::encode::Error) -> Self {
        Self::Serialization(format!("msgpack encode: {e}"))
    }
}

#[cfg(feature = "msgpack")]
impl From<rmp_serde::decode::Error> for FuseError {
    fn from(e: rmp_serde::decode::Error) -> Self {
        Self::Serialization(format!("msgpack decode: {e}"))
    }
}

pub type FuseResult<T> = Result<T, FuseError>;
