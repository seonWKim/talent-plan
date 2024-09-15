use env_logger::Env;
use log::{error, info, log};
use std::net::SocketAddr;
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
}
