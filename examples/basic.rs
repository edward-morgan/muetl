use muetl::{actors::NegotiatedType, system::*, task_defs::HasOutputs};

use std::{any::TypeId, collections::HashMap, thread::sleep, time::Duration};

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

    // Negotiating outputs gets complex when you have subscribers
    let mut ticker_negotiated_outputs = HashMap::new();
    ticker_negotiated_outputs.insert(
        "tick".to_string(),
        NegotiatedType::Singleton(TypeId::of::<u64>()),
    );

    let mut ticker_sender_ids = HashMap::new();
    ticker_sender_ids.insert("tick".to_string(), 170);

    let monitor_chan = PubSub::new(kameo_actors::DeliveryStrategy::Guaranteed);
    let ticker_daemon = DaemonActor::<Ticker>::new(
        Some(ticker),
        monitor_chan,
        ticker_negotiated_outputs,
        ticker_sender_ids,
    );

    let ticker_daemon_ref = DaemonActor::spawn(ticker_daemon);

    sleep(Duration::from_secs(20));

    Ok(())
}
