use muetl::{
    runtime::{
        connection::{Connection, IncomingConnections, OutgoingConnections},
        sink_actor::SinkActor,
        EventMessage, NegotiatedType,
    },
    sinks::log_sink::LogSink,
    system::*,
    task_defs::HasOutputs,
};

use std::{any::TypeId, collections::HashMap, sync::Arc, thread::sleep, time::Duration};

use kameo::prelude::*;
use kameo_actors::pubsub::{PubSub, Subscribe};

use crate::{
    daemons::ticker::Ticker,
    runtime::daemon_actor::DaemonActor,
    task_defs::{TaskConfigValue, TaskDef},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello, world!");

    let conns = vec![Connection::new(
        NegotiatedType::Singleton(TypeId::of::<u64>()),
        "tick".to_string(),
        "input".to_string(),
    )];

    let monitor_chan = PubSub::spawn(PubSub::new(kameo_actors::DeliveryStrategy::Guaranteed));
    let log_sink = SinkActor::<LogSink>::new(
        Some(Box::new(LogSink {})),
        monitor_chan.clone(),
        IncomingConnections::from(&conns),
    );
    let log_sink_ref = SinkActor::spawn(log_sink);

    let mut cfg = HashMap::<String, TaskConfigValue>::new();
    cfg.insert("period_ms".to_string(), TaskConfigValue::Uint(2000));

    let ticker = Ticker::new(&cfg).unwrap();
    let ticker_daemon = DaemonActor::<Ticker>::new(
        Some(ticker),
        monitor_chan.clone(),
        OutgoingConnections::from(&conns),
    );

    let ticker_daemon_ref = DaemonActor::spawn(ticker_daemon);

    sleep(Duration::from_secs(20));

    Ok(())
}
