use std::{io::{Read, Write}, net::{SocketAddr, TcpListener}, str::Bytes};
use std::io::{BufReader, BufWriter};
use serde_json::{Deserializer,Serializer};
use serde::{Serialize, Deserialize};
use log::{info};
use std::net::TcpStream;

use crate::{KvsEngine, Result, KvsError};

pub struct KvsServer<E: KvsEngine>{
    engine: E,
}

impl <E: KvsEngine> KvsServer<E> {
    pub fn new(engine: E) -> Self {
        KvsServer{engine}
    }


    pub fn run(mut self, addr: SocketAddr) -> Result<()> {
        println!("{:?}", serde_json::to_string(&Req::Remove{key:"hI".to_string()}));
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            match stream {
                Ok(ts) => {
                    let br = BufReader::new(&ts);
                    let req_reader = Deserializer::from_reader(br).into_iter::<Req>();
                    for res in req_reader {
                        match res {
                            Ok(req) => {
                                match req {
                                    Req::Get { key } => {
                                        let res = self.engine.get(key);
                                        match res {
                                            Ok(r) => {
                                                match r {
                                                    Some(opt) => {
                                                        send_resp(&ts, GetResponse::Ok(Some(opt)))?;
                                                    }
                                                    None => {
                                                        send_resp(&ts, GetResponse::Ok(None))?;
                                                    }
                                                }
                                            },
                                            Err(err) => {
                                                send_resp(&ts, GetResponse::Err("oops".to_string()))?;
                                            }
                                        }
                                    },
                                    Req::Set { key, value } => {
                                        let res = self.engine.set(key, value);
                                        match res {
                                            Ok(()) =>  {
                                                send_resp(&ts, SetResponse::Ok(()))?;
                                            },
                                            Err(_) => {
                                                send_resp(&ts, SetResponse::Err("oops".to_string()))?;
                                            }
                                        }
                                    },
                                    Req::Remove { key } => {
                                        let res = self.engine.remove(key);
                                        match res {
                                            Ok(()) =>  {
                                                send_resp(&ts, RemoveResponse::Ok(()))?;
                                            },
                                            Err(_) => {
                                                send_resp(&ts, RemoveResponse::Err("oops".to_string()))?;
                                            }
                                        }
                                    }
                                }
                            },
                            Err(error) => {
                                return Err(KvsError::from(error));
                            }
                        }
                    }
                },
                Err(error) => {
                    return Err(KvsError::from(error));
                }
            }
        }
        Ok(())
    }
}

fn send_resp<T: Serialize>(ts: &TcpStream, s: T) -> Result<()> {
    let mut wr =  BufWriter::new(ts);
    serde_json::to_writer(&mut wr ,&s)?;
    wr.flush()?;
    Ok(())
}


#[derive(Serialize, Deserialize, Debug)]
pub enum Req{
    Set {key: String, value: String},
    Remove {key: String},
    Get {key: String},
}

#[derive(Debug, Serialize, Deserialize)]
pub enum GetResponse {
    Ok(Option<String>),
    Err(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SetResponse {
    Ok(()),
    Err(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RemoveResponse {
    Ok(()),
    Err(String),
}