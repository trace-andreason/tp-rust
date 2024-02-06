use::std::io;
use std::string::FromUtf8Error;

use failure::Fail;

#[derive(Fail, Debug)]
pub enum KvsError{
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),

    #[fail(display = "{}", _0)]
    Serde(#[cause] serde_json::Error),

    #[fail(display = "Key not found")]
    KeyNotFound,

    #[fail(display = "wrong meta")]
    WrongMeta,

    #[fail(display = "{}", _0)]
    UTF8(#[cause] FromUtf8Error),
}
pub type Result<T> = std::result::Result<T, KvsError>;

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::Io(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> KvsError {
        KvsError::Serde(err)
    }
}

impl From<FromUtf8Error> for KvsError {
    fn from(err: FromUtf8Error) -> KvsError {
        KvsError::UTF8(err)
    }
}