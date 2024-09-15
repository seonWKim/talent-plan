use log::{error, info};
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::process::exit;
use structopt::StructOpt;

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

#[derive(StructOpt, Debug)]
enum Command {
    #[structopt(about = "Set the value of a key to a string")]
    Set { key: String, value: String },

    #[structopt(about = "Get the string value of a given string key")]
    Get { key: String },

    #[structopt(about = "Remove a given key")]
    Rm { key: String },
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
        match cmd {
            Command::Set { key, value } => {
                let command = format!("SET {} {}\n", key, value);
                stream.write_all(command.as_bytes()).expect("Failed to send SET command");
            }
            Command::Get { key } => {
                let command = format!("GET {}\n", key);
                stream.write_all(command.as_bytes()).expect("Failed to send GET command");
            }
            Command::Rm { key } => {
                let command = format!("RM {}\n", key);
                stream.write_all(command.as_bytes()).expect("Failed to send RM command");
            }
        }
    } else {
        error!("No command provided");
        exit(1)
    }

    exit(0);
}
