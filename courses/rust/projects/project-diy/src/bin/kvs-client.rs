use log::{error, info};
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::process::exit;
use structopt::StructOpt;
use kvs::command::Command;

#[derive(StructOpt, Debug)]
#[structopt(name = "kvs-client", about = "A key-value store")]
struct Config {
    #[structopt(subcommand)]
    cmd: Option<Command>,

    #[structopt(long, default_value = "127.0.0.1:4000")]
    addr: SocketAddr,

    #[structopt(short = "V", long = "version")]
    version: bool,
}

fn main() {
    let config = Config::from_args();
    if config.version {
        println!("Running client version {}", env!("CARGO_PKG_VERSION"));
        exit(0);
    }

    if let Some(cmd) = config.cmd {
        info!("Trying to connect to server at {}", config.addr);
        let mut stream = TcpStream::connect(config.addr).expect("Could not connect to the server");

        info!("Connected, sending command: {:?}", cmd);
        let serialized_cmd = serde_json::to_string(&cmd).expect("Failed to serialize command");
        stream.write_all(serialized_cmd.as_bytes()).expect("Failed to send command");
    } else {
        error!("No command provided");
        exit(1)
    }

    exit(0);
}
