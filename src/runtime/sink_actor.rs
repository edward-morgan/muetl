use std::{collections::HashMap, future::Future, sync::Arc};

use kameo::{actor::ActorRef, error::Infallible, prelude::Message, Actor, Reply};
use kameo_actors::{
    broker::Broker,
    pubsub::{PubSub, Publish, Subscribe},
};
use tokio::sync::mpsc;

use crate::runtime::connection::{Connection, IncomingConnection, IncomingConnections};
use crate::{
    messages::{event::Event, Status, StatusUpdate},
    runtime::event::InternalEvent,
    task_defs::{sink::Sink, MuetlSinkContext},
    util::new_id,
};

pub type OwnedSink<T> = Option<Box<T>>;

// #[derive(Actor)]
pub struct SinkActor<T: 'static>
where
    T: Sink,
{
    id: u64,
    sink: OwnedSink<T>,
    monitor_chan: ActorRef<PubSub<StatusUpdate>>,
    /// The mapping set by the system at runtime to tell this actor which
    /// input conn_name events with a given sender_id should go to.
    // internal_event_routes: HashMap<u64, String>,
    // subscriptions: Vec<IncomingConnection>,
    subscriptions: IncomingConnections,
}

impl<T: Sink> SinkActor<T> {
    pub fn new(
        sink: OwnedSink<T>,
        monitor_chan: ActorRef<PubSub<StatusUpdate>>,
        // internal_event_routes: HashMap<u64, String>,
        subscriptions: IncomingConnections,
    ) -> Self {
        SinkActor {
            id: new_id(),
            sink,
            monitor_chan,
            // internal_event_routes,
            subscriptions,
        }
    }

    // fn get_conn_for_sender_id(&self, sender_id: u64) -> Result<String, String> {
    //     if let Some(conn_name) = self.internal_event_routes.get(&sender_id) {
    //         Ok(conn_name.clone())
    //     } else {
    //         Err(format!(
    //             "cannot find input conn_name for sender_id {}",
    //             sender_id
    //         ))
    //     }
    // }
}

impl<T: Sink> Message<Arc<InternalEvent>> for SinkActor<T> {
    type Reply = ();
    async fn handle(
        &mut self,
        msg: Arc<InternalEvent>,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Map the incoming event to the right input conn_name
        match self.subscriptions.conn_name_for(msg.clone()) {
            Ok(input_conn_name) => {
                // TODO: ideally we don't have to do this. OTOH, I don't like the incoming events to have
                // the wrong conn_name associated with them...
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
                                self.monitor_chan.tell(Publish(update)).await.unwrap();
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
impl<T: Sink> Actor for SinkActor<T> {
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
