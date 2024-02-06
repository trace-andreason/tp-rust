use super::KvsEngine;
use crate::{KvsError, Result};
use std::path::{self, PathBuf};
use std::fs::OpenOptions;

use std::fs;
use std::io::{Read, Write, ErrorKind};

pub struct Sled {
    db: sled::Db,
}

impl Sled {
    pub fn open(p: &path::Path) -> Result<Sled> {
        let meta_f = p.join("meta.txt");
        check_meta(&meta_f)?;
        create_meta(&meta_f)?;
        let tree = sled::open(p)?;
        Ok(Sled{db: tree})
    }
}

impl KvsEngine for Sled {
    fn set(&mut self, k: String, v: String) -> Result<()> {
        self.db.insert(k,v)?;
        self.db.flush()?;
        Ok(())
    }
    fn get(&mut self, k: String) -> Result<Option<String>> {
        Ok(self.db
            .get(k)?
            .map(|i_vec| AsRef::<[u8]>::as_ref(&i_vec).to_vec())
            .map(String::from_utf8)
            .transpose()?)
    }
    fn remove(&mut self, k: String) -> Result<()> {
        let res = self.db.remove(k)?;
        self.db.flush()?;
        match res {
            Some(_) => {
                return Ok(());
            },
            None => {
                return Err(KvsError::KeyNotFound);
            }
        }
    }
}

fn check_meta (f: &path::PathBuf) -> Result<()> {
    let res = OpenOptions::new().read(true).open(f.clone());
    match res {
        Ok(f) => {
            let s = read_meta(f);
            if s == "sled" {
                return Ok(());
            }
            return Err(KvsError::WrongMeta);
        },
        Err(err) => { match err.kind() {
            ErrorKind::NotFound => {
                return Ok(());
            },
            _ => {
                return Err(KvsError::from(err));
            }
        }
        }
    }
}

fn read_meta(mut f: fs::File) -> String {
    let mut s = String::new();
    if let Err(e) = f.read_to_string(&mut s) {
        return "".to_string();
    }
    s
}

fn create_meta(pbuf: &path::PathBuf) -> Result<()> {
    let mut f = OpenOptions::new().create(true).write(true).truncate(true).open(pbuf.clone())?;
    f.write_all("sled".as_bytes())?;
    Ok(())
}