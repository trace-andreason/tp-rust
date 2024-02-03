use crate::{Result};

pub trait KvsEngine {
    fn set(&mut self, k: String, v: String) -> Result<()>;
    fn get(&mut self, k: String) -> Result<Option<String>>;
    fn remove(&mut self, k: String) -> Result<()>;
}

mod kvs;
pub use self::kvs::KvStore;