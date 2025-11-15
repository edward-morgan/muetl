use muetl::{
    actors::{sink::SinkActor, EventMessage, NegotiatedType, Subscription},
    sinks::log_sink::LogSink,
    system::*,
    task_defs::HasOutputs,
};

use std::{any::TypeId, collections::HashMap, sync::Arc, thread::sleep, time::Duration};

use kameo::prelude::*;
use kameo_actors::pubsub::{PubSub, Subscribe};

use crate::{
    actors::daemon::DaemonActor,
    daemons::ticker::Ticker,
    task_defs::{TaskConfigValue, TaskDef},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello, world!");

    let mut log_sink_sender_ids = HashMap::new();
    log_sink_sender_ids.insert(170 as u64, "input".to_string());
    let monitor_chan1 = PubSub::new(kameo_actors::DeliveryStrategy::Guaranteed);
    let log_sink = SinkActor::<LogSink>::new(
        Some(Box::new(LogSink {})),
        monitor_chan1,
        log_sink_sender_ids,
    );
    let log_sink_ref = SinkActor::spawn(log_sink);

    let tick_chan = Arc::new(PubSub::<EventMessage>::spawn(PubSub::new(
        kameo_actors::DeliveryStrategy::Guaranteed,
    )));
    tick_chan.tell(Subscribe(log_sink_ref)).await.unwrap();

    // Negotiating outputs gets complex when you have subscribers
    let mut ticker_negotiated_outputs = HashMap::new();
    ticker_negotiated_outputs.insert(
        "tick".to_string(),
        Subscription {
            chan_ref: tick_chan.clone(),
            chan_type: NegotiatedType::Singleton(TypeId::of::<u64>()),
        },
    );

    let mut ticker_sender_ids = HashMap::new();
    ticker_sender_ids.insert("tick".to_string(), 170);

    let monitor_chan2 = PubSub::new(kameo_actors::DeliveryStrategy::Guaranteed);

    let mut cfg = HashMap::<String, TaskConfigValue>::new();
    cfg.insert("period_ms".to_string(), TaskConfigValue::Uint(2000));
    let ticker = Ticker::new(&cfg).unwrap();
    let ticker_daemon = DaemonActor::<Ticker>::new(
        Some(ticker),
        monitor_chan2,
        ticker_sender_ids,
        ticker_negotiated_outputs,
    );

    let ticker_daemon_ref = DaemonActor::spawn(ticker_daemon);

    sleep(Duration::from_secs(20));

    Ok(())
}
