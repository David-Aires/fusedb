/// All errors that can originate from core FuseDB operations.
///
/// This type has zero PyO3 knowledge — conversion to `PyErr` lives in
/// `crate::python::error`.
#[derive(Debug)]
pub enum FuseError {
    /// The file is structurally invalid (bad magic, truncated, CRC mismatch).
    Corrupt(String),
    /// The file was written with an unsupported format version.
    Version(u8),
    /// An I/O error occurred (open, read, write, rename, fsync).
    Io(String),
    /// A caller passed an invalid argument.
    InvalidArg(String),
}

impl std::fmt::Display for FuseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Corrupt(m)    => write!(f, "FuseCorruptError: {m}"),
            Self::Version(v)    => write!(f, "FuseVersionError: unsupported version {v}"),
            Self::Io(m)         => write!(f, "FuseIOError: {m}"),
            Self::InvalidArg(m) => write!(f, "FuseError: {m}"),
        }
    }
}

impl std::error::Error for FuseError {}

pub type FuseResult<T> = Result<T, FuseError>;