use fuser::FileType;
use grammers_client::types::iter_buffer::InvocationError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    // User errors
    #[error("Object not found")]
    NotFound,
    #[error("Not a directory")]
    NotADirectory,
    #[error("Is a directory")]
    IsADirectory,
    #[error("Directory not empty")]
    DirectoryNotEmpty,
    #[error("File exists")]
    FileExists,
    #[error("File changed in remote side, please re-open it")]
    Invalidated,

    // sql error
    #[error("sql error: {0}")]
    Sql(#[from] sqlx::Error),

    // grammers error
    #[error("grammers error: {0}")]
    Grammers(#[from] InvocationError),
    #[error("Download failed")]
    DownloadFailed,
    #[error("Media invalid")]
    MediaInvalid,

    // IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    // Not supported.
    #[error("File type not support: {0:?}")]
    InvalidFileType(FileType),
}

impl Error {
    pub fn into_c_err(self) -> libc::c_int {
        match &self {
            // User errors.
            Self::NotFound => libc::ENOENT,
            Self::NotADirectory => libc::ENOTDIR,
            Self::IsADirectory => libc::EISDIR,
            Self::DirectoryNotEmpty => libc::ENOTEMPTY,
            Self::FileExists => libc::EEXIST,
            Self::Invalidated => libc::EPERM,

            // sql error
            Self::Sql(_) => {
                log::error!("{}", self);
                log::debug!("{:?}", self);
                libc::EIO
            }

            // grammers
            Self::Grammers(_) => {
                log::error!("{}", self);
                log::debug!("{:?}", self);
                libc::EIO
            }
            Self::DownloadFailed | Self::MediaInvalid => libc::EIO,

            // Network errors.
            Self::Io(_) => {
                log::error!("{}", self);
                log::debug!("{:?}", self);
                libc::EIO
            }

            // Not supported
            Self::InvalidFileType(_) => {
                log::info!("{:?}", self);
                libc::EPERM
            }
        }
    }
}
