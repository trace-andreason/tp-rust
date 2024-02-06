use kvs::{GetResponse, KvStore, KvsError, RemoveResponse, Req, Result, SetResponse};
use serde::Deserialize;
use std::env::current_dir;
use std::ops::Rem;
use std::process::exit;
use std::net::SocketAddr;
use structopt::StructOpt;
use std::net::{TcpStream};
use std::io::{self, Read, Write, BufReader};
use serde_json::Deserializer;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";
const ADDRESS_FORMAT: &str = "IP:PORT";

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs-client", author, about)]
struct Args {
    #[structopt(subcommand)]
    command: Commands,
}
#[derive(StructOpt, Debug)]
enum Commands {
    Set {
        key: String,
        value: String,
        #[structopt(
            long="addr", 
            value_name = ADDRESS_FORMAT, 
            default_value = DEFAULT_LISTENING_ADDRESS,
            parse(try_from_str)
        )]
        addr: SocketAddr
    } ,
    Get {key: String,
        #[structopt(
            long="addr", 
            value_name = ADDRESS_FORMAT, 
            default_value = DEFAULT_LISTENING_ADDRESS,
            parse(try_from_str)
        )]
        addr: SocketAddr
    },
    Rm {key: String,
        #[structopt(
            long="addr", 
            value_name = ADDRESS_FORMAT, 
            default_value = DEFAULT_LISTENING_ADDRESS,
            parse(try_from_str)
        )]
        addr: SocketAddr
    },
}

fn main() -> Result<()> {

    let cli = Args::from_args();
    match &cli.command {
        Commands::Set {key, value ,addr} => {
            let stream = TcpStream::connect(addr)?;
            serde_json::to_writer(&stream ,&Req::Set{key: key.to_string(),value: value.to_string()})?;
            let mut br = Deserializer::from_reader(BufReader::new(&stream));  
            let resp = SetResponse::deserialize(&mut br);
            //println!("{:?}", resp);
            Ok(())
        }
        Commands::Get {key, addr} => {
            let stream = TcpStream::connect(addr)?;
            serde_json::to_writer(&stream ,&Req::Get{key: key.to_string()})?;
            let mut br = Deserializer::from_reader(BufReader::new(&stream));  
            let resp = GetResponse::deserialize(&mut br)?;
            match resp {
                GetResponse::Ok(x) => {
                    match x {
                        Some(s) => {
                            println!("{}", s);
                        },
                        None => {
                            println!("{}", "Key not found");
                        }
                    }
                },
                GetResponse::Err(error) => {
                    panic!("{}",error);
                }
            }
            Ok(())
        }
        Commands::Rm {key, addr} => {
            let stream = TcpStream::connect(addr)?;
            serde_json::to_writer(&stream ,&Req::Remove{key: key.to_string()})?;
            let mut br = Deserializer::from_reader(BufReader::new(&stream));  
            let resp = RemoveResponse::deserialize(&mut br)?;
            match resp {
                RemoveResponse::Ok(()) => {},
                RemoveResponse::Err(error) => {
                    eprint!("{}", error);
                    exit(1);
                }
            }
            Ok(())
        }
    }
}