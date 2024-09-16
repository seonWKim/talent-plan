use env_logger::Env;
use kvs::command::Command;
use log::{error, info, log};
use serde_json::from_str;
use std::io::{BufReader, Read};
use std::net::{SocketAddr, TcpListener};
use std::process::exit;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "kvs-server", about = "A key-value store server")]
struct Config {
    /// IP address and port to bind to
    #[structopt(long, default_value = "127.0.0.1:4000")]
    addr: SocketAddr,

    /// Storage engine to use (e.g., kvs, sled)
    #[structopt(long, default_value = "kvs")]
    engine: String,

    /// Print version information
    #[structopt(short = "V", long = "version")]
    version: bool,
}

fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let config = Config::from_args();
    if config.version {
        println!("{}", env!("CARGO_PKG_VERSION"));
    }

    info!(
        "Starting kvs-server on {} using {} engine. Version: {}c",
        config.addr,
        config.engine,
        env!("CARGO_PKG_VERSION")
    );

    let listener = TcpListener::bind(config.addr).unwrap_or_else(|e| {
        error!("Failed to bind to {}: {}", config.addr, e);
        exit(1);
    });

    info!("Server is listening on {}", config.addr);

    // TODO: create a KvStore if not exists

    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                let mut reader = BufReader::new(&stream);
                let mut buffer = String::new();
                match reader.read_to_string(&mut buffer) {
                    Ok(_) => {
                        match from_str::<Command>(&buffer) {
                            Ok(command) => {
                                info!("Received command: {:?}", command);
                            }
                            Err(e) => {
                                error!("Failed to deserialize command: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to read from connection: {}", e);
                    }
                }
            }
            Err(e) => {
                error!("Connection failed: {}", e);
            }
        }
    }

    exit(0);
}
