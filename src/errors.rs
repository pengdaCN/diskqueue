use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid metadata file, may be file corruption")]
    InvalidMetadataFile,
    #[error("open metadata failed reason: {0}")]
    OpenMetadataFailed(String),
}