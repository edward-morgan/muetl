use std::{collections::HashMap, sync::Arc};

use kameo::{prelude::Message, Actor};
use kameo_actors::{broker::Broker, pubsub::PubSub};
use tokio::sync::mpsc;

use crate::{
    messages::{event::Event, Status, StatusUpdate},
    runtime::event::InternalEvent,
    task_defs::{sink::Sink, MuetlSinkContext},
    util::new_id,
};

pub type OwnedSink<T> = Option<Box<T>>;

#[derive(Actor)]
pub struct SinkActor<T: 'static>
where
    T: Sink,
{
    id: u64,
    sink: OwnedSink<T>,
    monitor_chan: PubSub<StatusUpdate>,
    /// The mapping set by the system at runtime to tell this actor which
    /// input conn_name events with a given sender_id should go to.
    internal_event_routes: HashMap<u64, String>,
}

impl<T: Sink> SinkActor<T> {
    pub fn new(
        sink: OwnedSink<T>,
        monitor_chan: PubSub<StatusUpdate>,
        internal_event_routes: HashMap<u64, String>,
    ) -> Self {
        SinkActor {
            id: new_id(),
            sink,
            monitor_chan,
            internal_event_routes,
        }
    }
}

impl<T: Sink> Message<InternalEvent> for SinkActor<T> {
    type Reply = ();
    async fn handle(
        &mut self,
        mut msg: InternalEvent,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Map the incoming event to the right input conn_name
        if let Some(input_conn_name) = self.internal_event_routes.get(&msg.sender_id) {
            // TODO: ideally we don't have to do this.
            msg.event.conn_name = input_conn_name.clone();
            let (status_tx, mut status_rx) = mpsc::channel(100);
            let ctx = MuetlSinkContext { status: status_tx };

            let mut sink = self.sink.take().unwrap();
            let m = Arc::new(msg.event);
            let conn_name = input_conn_name.clone();

            let fut = tokio::spawn(async move {
                sink.handle_event(&ctx, &conn_name, m.clone()).await;
                sink
            });

            loop {
                tokio::select! {
                    res = status_rx.recv() => {
                        if let Some(status) = res {
                            println!("Received status {:?}", status);
                            let update = StatusUpdate{status: status, id: self.id};
                            self.monitor_chan.publish(update).await;
                        } else {
                            break;
                        }
                    },
                }
            }

            match fut.await {
                Ok(sink) => {
                    println!("Sink run finished");
                    self.sink = Some(sink);
                }
                Err(e) => {
                    println!("Sink task panicked: {:?}", e);
                    // Send a failure message to the monitor
                    self.monitor_chan
                        .publish(StatusUpdate {
                            id: self.id,
                            status: Status::Failed(e.to_string()),
                        })
                        .await;
                }
            }
        } else {
            println!(
                "Unknown input connection mapping for sender_id {}; looking for one of {:?}",
                msg.sender_id, self.internal_event_routes
            );
        }
    }
}
