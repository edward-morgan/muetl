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

    fn get_conn_for_sender_id(&self, sender_id: u64) -> Result<String, String> {
        if let Some(conn_name) = self.internal_event_routes.get(&sender_id) {
            Ok(conn_name.clone())
        } else {
            Err(format!(
                "cannot find input conn_name for sender_id {}",
                sender_id
            ))
        }
    }
}

impl<T: Sink> Message<Arc<InternalEvent>> for SinkActor<T> {
    type Reply = ();
    async fn handle(
        &mut self,
        msg: Arc<InternalEvent>,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        println!("Sink running");
        // Map the incoming event to the right input conn_name
        match self.get_conn_for_sender_id(msg.sender_id) {
            Ok(input_conn_name) => {
                // TODO: ideally we don't have to do this.
                // msg.event.conn_name = input_conn_name.clone();
                let (status_tx, mut status_rx) = mpsc::channel(100);
                let ctx = MuetlSinkContext { status: status_tx };

                let mut sink = self.sink.take().unwrap();
                let m = msg.event.clone();
                let conn_name = input_conn_name.clone();

                let fut = tokio::spawn(async move {
                    sink.handle_event(&ctx, &conn_name, m).await;
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
            }
            Err(e) => {
                println!("Runtime error: {}", e)
            }
        }
    }
}
