use kvs::{KvStore, Result, KvsServer, KvsEngine};
use std::env::current_dir;
use std::process::exit;
use std::net::SocketAddr;
use structopt::StructOpt;
use log::*;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";
const ADDRESS_FORMAT: &str = "IP:PORT";

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs-server", author, about)]
struct Args {
    #[structopt(
        long="addr", 
        value_name = ADDRESS_FORMAT, 
        default_value = DEFAULT_LISTENING_ADDRESS,
        parse(try_from_str)
    )]
    addr: SocketAddr,
    #[structopt(
        long="engine", 
        value_name = "ENGINE-NAME", 
        default_value = "kvs",
    )]
    engine: String,
}

fn main() -> Result<()> {
    stderrlog::new().module(module_path!()).verbosity(10).init().unwrap();
    let temp_dir = current_dir()?;
    let mut kv = KvStore::open(temp_dir.as_path()).expect("failed to open kv");
    let cli = Args::from_args();
    info!("server version: {}", env!("CARGO_PKG_VERSION"));
    info!("listening on: {}", cli.addr);
    info!("using engine: {}", cli.engine);
    match cli.engine.as_str() {
        "kvs" => {
            let kvs = KvsServer::new(kv);
            kvs.run(cli.addr);
            Ok(())
        },
        "sled" => {
            panic!();
        },
        _ => {
            eprintln!("unsupported engine");
            exit(1);
        }
    }
}