use std::collections::HashSet;
use std::sync::Arc;

use kameo::{actor::ActorRef, prelude::Message, Actor};
use kameo_actors::pubsub::{PubSub, Publish};
use tokio::sync::mpsc;

use crate::messages::SystemEvent;
use crate::runtime::connection::IncomingConnections;
use crate::{
    messages::{Status, StatusUpdate},
    runtime::event::InternalEvent,
    task_defs::{sink::Sink, MuetlSinkContext},
    util::new_id,
};

use super::event::Payload;

pub struct SinkActor {
    id: u64,
    sink: Option<Box<dyn Sink>>,
    monitor_chan: ActorRef<PubSub<StatusUpdate>>,
    /// The mapping set by the system at runtime to tell this actor which
    /// input conn_name events with a given sender_id should go to.
    subscriptions: IncomingConnections,
    active_subscriptions: HashSet<u64>,
}

impl SinkActor {
    pub fn new(
        sink: Option<Box<dyn Sink>>,
        monitor_chan: ActorRef<PubSub<StatusUpdate>>,
        subscriptions: IncomingConnections,
    ) -> Self {
        let incoming_sender_ids = subscriptions.incoming_sender_ids();
        SinkActor {
            id: new_id(),
            sink,
            monitor_chan,
            subscriptions,
            active_subscriptions: incoming_sender_ids,
        }
    }

    /// Determines whether or not this Sink should shut down, which relies on looking at each incoming connection
    /// and determining if any of them can potentially receive data.
    pub fn should_shut_down(&self) -> bool {
        self.active_subscriptions.is_empty()
    }
}

impl Message<Arc<SystemEvent>> for SinkActor {
    type Reply = ();
    async fn handle(
        &mut self,
        msg: Arc<SystemEvent>,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        println!("SinkActor {} received shutdown signal", self.id);
        ctx.stop();
    }
}

impl Message<Arc<InternalEvent>> for SinkActor {
    type Reply = ();
    async fn handle(
        &mut self,
        msg: Arc<InternalEvent>,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match &msg.event {
            Payload::Stopped => {
                // Mark the current IncomingConnection as stopped
                self.active_subscriptions.remove(&msg.sender_id);
                // If no incoming connections are active, then stop the Actor.
                if self.should_shut_down() {
                    println!("No incoming connections are still active; Sink will shut down.");
                    ctx.stop();
                }
            }
            Payload::Data(ev) => {
                // Map the incoming event to the right input conn_name
                match self.subscriptions.conn_name_for(msg.clone()) {
                    Ok(input_conn_name) => {
                        let (status_tx, mut status_rx) = mpsc::channel(100);
                        let ctx = MuetlSinkContext {
                            status: status_tx,
                            event_name: ev.name.clone(),
                            event_headers: ev.headers.clone(),
                        };

                        let mut sink = self.sink.take().unwrap();
                        let m = ev.clone();
                        let conn_name = input_conn_name.clone();

                        let fut = tokio::spawn(async move {
                            sink.handle_event_for_conn(&ctx, &conn_name, m).await;
                            sink
                        });

                        loop {
                            tokio::select! {
                                res = status_rx.recv() => {
                                    if let Some(status) = res {
                                        println!("Received status {:?}", status);
                                        let update = StatusUpdate{status: status, id: self.id};
                                        self.monitor_chan.tell(Publish(update)).await.unwrap();
                                    } else {
                                        break;
                                    }
                                },
                            }
                        }

                        match fut.await {
                            Ok(sink) => {
                                self.sink = Some(sink);
                            }
                            Err(e) => {
                                println!("Sink task panicked: {:?}", e);
                                // Send a failure message to the monitor
                                self.monitor_chan
                                    .tell(Publish(StatusUpdate {
                                        id: self.id,
                                        status: Status::Failed(e.to_string()),
                                    }))
                                    .await
                                    .unwrap()
                            }
                        }
                    }
                    Err(e) => {
                        println!("Runtime error: {}", e)
                    }
                }
            }
        }
    }
}
impl Actor for SinkActor {
    type Args = Self;
    type Error = String;
    async fn on_start(
        args: Self::Args,
        actor_ref: kameo::prelude::ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        // Subscribe to each of the subscriptions we've been initialized with
        match args.subscriptions.subscribe_to_all(actor_ref).await {
            Ok(()) => Ok(args),
            Err(e) => Err(e),
        }
    }
}
