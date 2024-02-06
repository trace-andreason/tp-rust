use std::io::{BufRead, BufReader, BufWriter, Read, Write, Seek, ErrorKind};
use std::path::{self, PathBuf};
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use std::fs::OpenOptions;
use crate::{KvsError, Result, KvsEngine};
use std::fs;


#[derive(Serialize, Deserialize, Debug)]
enum Entry{
    Set {key: String, value: String},
    Remove {key: String},
}

pub struct KvStore{
    kv: HashMap<String,String>,
    writer: BufWriter<std::fs::File>,
    reader: BufReader<std::fs::File>,
    pbuf: PathBuf,
}

impl KvStore {
    pub fn open(p: &path::Path) -> Result<KvStore> {
        let f = p.join("tmp.log");
        let meta_f = p.join("meta.txt");
        check_meta(&meta_f)?;
        create_meta(&meta_f)?;
        Ok(KvStore{
            kv: HashMap::new(),
            writer:  BufWriter::new(OpenOptions::new().create(true).append(true).open(f.clone())?),
            reader:  BufReader::new(OpenOptions::new().read(true).open(f.clone())?),
            pbuf: f,
        })
    }

    pub fn set(&mut self, k: String, v: String) -> Result<()> {
        self.kv.insert(k.clone(), v.clone());
        let val =   Entry::Set{key: k, value: v};
        serde_json::to_writer(&mut self.writer, &val)?;
        writeln!(self.writer,"")?;
        self.writer.flush()?;
        self.compact()?;
        Ok(())
    }

    pub fn get(&mut self, k: String) -> Result<Option<String>> {
        self.populate_hash()?;
        Ok(self.kv.get(&k).cloned())
    }

    pub fn remove(&mut self, k: String) -> Result<()> {
        self.populate_hash()?;
        if let Some(_) = self.kv.get(&k) {
            let val =   Entry::Remove { key: k };
            serde_json::to_writer(&mut self.writer, &val)?;
            writeln!(self.writer,"")?;
            self.writer.flush()?;
        } else {
            return Err(KvsError::KeyNotFound)
        }
        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        let mut compactor: BufWriter<fs::File> = BufWriter::new(OpenOptions::new().create(true).write(true).truncate(true).open(self.pbuf.clone())?);
        for (k, v) in &mut self.kv.clone().into_iter() {
            let val =   Entry::Set{key: k, value: v};
            serde_json::to_writer(&mut compactor, &val)?;
            writeln!(compactor,"")?;
        }
        compactor.flush()?;
        Ok(())
    }

    fn populate_hash(&mut self) -> Result<()> {
        let mut new_kv: HashMap<String, String> = HashMap::new();
        for line in self.reader.by_ref().lines() {
            let sd = line?;
            let val: Entry = serde_json::from_str(&sd)?;
            match val {
                Entry::Set {key, value} => {
                    new_kv.insert(key, value);
                },
                Entry::Remove { key} => {
                    new_kv.remove(&key);
                }
            }
        }
        self.reader.rewind()?;
        self.kv = new_kv;
        self.compact()?;
        Ok(())
    }
}

impl KvsEngine for KvStore{
    fn set(&mut self, k: String, v: String) -> Result<()> {
        self.kv.insert(k.clone(), v.clone());
        let val =   Entry::Set{key: k, value: v};
        serde_json::to_writer(&mut self.writer, &val)?;
        writeln!(self.writer,"")?;
        self.writer.flush()?;
        self.compact()?;
        Ok(())
    }

    fn get(&mut self, k: String) -> Result<Option<String>> {
        self.populate_hash()?;
        Ok(self.kv.get(&k).cloned())
    }

    fn remove(&mut self, k: String) -> Result<()> {
        self.populate_hash()?;
        if let Some(_) = self.kv.get(&k) {
            let val =   Entry::Remove { key: k };
            serde_json::to_writer(&mut self.writer, &val)?;
            writeln!(self.writer,"")?;
            self.writer.flush()?;
        } else {
            return Err(KvsError::KeyNotFound)
        }
        Ok(())
    }
}

fn check_meta (f: &path::PathBuf) -> Result<()> {
    let res = OpenOptions::new().read(true).open(f.clone());
    match res {
        Ok(f) => {
            let s = read_meta(f);
            if s == "kvs" {
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
    f.write_all("kvs".as_bytes())?;
    Ok(())
}