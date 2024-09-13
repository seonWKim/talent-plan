use std::process::exit;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "kvs", about = "A key-value store")]
struct Config {
    #[structopt(subcommand)]
    cmd: Command,
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

    match config.cmd {
        Command::Set { key, value } => {
            eprintln!("unimplemented");
            exit(1);
        }
        Command::Get { key } => {
            eprintln!("unimplemented");
            exit(1);
        }
        Command::Rm { key } => {
            eprintln!("unimplemented");
            exit(1);
        }
    }
}
