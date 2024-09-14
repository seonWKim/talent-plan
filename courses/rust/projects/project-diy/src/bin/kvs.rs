use kvs::KvStore;
use std::path::PathBuf;
use std::process::exit;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "kvs", about = "A key-value store")]
struct Config {
    #[structopt(subcommand)]
    cmd: Command,

    #[structopt(parse(from_os_str), short, long, default_value = ".")]
    current_dir: PathBuf,
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
    let current_dir = config.current_dir;
    let mut kvs = KvStore::open(current_dir.as_path()).expect("Unable to create kvs store");

    match config.cmd {
        Command::Set { key, value } => match kvs.set(key, value) {
            Ok(_) => exit(0),
            Err(_) => exit(1),
        },
        Command::Get { key } => match kvs.get(key) {
            Ok(Some(v)) => {
                println!("{}", v);
                exit(0);
            }
            Ok(None) => {
                println!("Key not found");
                exit(0);
            }
            Err(_) => exit(1),
        },
        Command::Rm { key } => match kvs.remove(key) {
            Ok(v) => {}
            Err(_) => exit(1),
        },
    }
}
