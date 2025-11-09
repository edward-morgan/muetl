mod actors;
mod daemons;

mod messages;
mod task_defs;
mod util;

use std::{collections::HashMap, thread::sleep, time::Duration};

use kameo::prelude::*;
use kameo_actors::pubsub::PubSub;

use crate::{
    actors::daemon::DaemonActor,
    daemons::ticker::Ticker,
    task_defs::{TaskConfigValue, TaskDef},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello, world!");

    let mut cfg = HashMap::<String, TaskConfigValue>::new();
    cfg.insert("period_ms".to_string(), TaskConfigValue::Uint(2000));

    let ticker = Ticker::new(&cfg).unwrap();

    let monitor_chan = PubSub::new(kameo_actors::DeliveryStrategy::Guaranteed);
    let ticker_daemon = DaemonActor::<Ticker>::new(Some(ticker), monitor_chan);

    let ticker_daemon_ref = DaemonActor::spawn(ticker_daemon);

    sleep(Duration::from_secs(20));

    Ok(())
}
