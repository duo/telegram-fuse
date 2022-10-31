use grammers_client::types::iter_buffer::InvocationError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    // User errors
    #[error("Object not found")]
    NotFound,
    #[error("Directory not empty")]
    DirectoryNotEmpty,
    #[error("File exists")]
    FileExists,

    // sql error
    #[error("sql error: {0}")]
    Sql(#[from] sqlx::Error),

    // grammers error
    #[error("grammers error: {0}")]
    Grammers(#[from] InvocationError),

    // IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

impl Error {
    pub fn into_c_err(self) -> libc::c_int {
        match &self {
            // User errors.
            Self::NotFound => libc::ENOENT,
            Self::DirectoryNotEmpty => libc::ENOTEMPTY,
            Self::FileExists => libc::EEXIST,

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

            // Network errors.
            Self::Io(_) => {
                log::error!("{}", self);
                log::debug!("{:?}", self);
                libc::EIO
            }
        }
    }
}
