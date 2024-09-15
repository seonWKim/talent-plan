use serde::{Deserialize, Serialize};
use structopt::StructOpt;

#[derive(StructOpt, Debug, Serialize, Deserialize)]
pub enum Command {
    #[structopt(about = "Set the value of a key to a string")]
    Set { key: String, value: String },

    #[structopt(about = "Get the string value of a given string key")]
    Get { key: String },

    #[structopt(about = "Remove a given key")]
    Rm { key: String },
}
