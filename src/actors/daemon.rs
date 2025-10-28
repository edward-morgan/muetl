use std::{
    any::TypeId,
    collections::HashMap,
    pin::{self, pin},
    sync::Arc,
    time::Duration,
};

use futures::{stream, Stream};
use kameo::{actor::ActorRef, error::Infallible, message::StreamMessage, prelude::Message, Actor};
use kameo_actors::pubsub::PubSub;
use tokio::sync::{self, mpsc};
use tokio_stream::StreamExt;

use crate::{
    messages::{event::Event, StatusUpdate},
    task_defs::{daemon::Daemon, TaskResult},
    util::new_id,
};

use super::EventMessage;

pub struct DaemonActor<T: 'static>
where
    T: Daemon,
{
    id: u32,
    daemon: Box<T>,
    outputs: HashMap<String, Vec<TypeId>>,
    /// For each output conn_name, keep a mapping of negotiated types to the PubSub channels results will be sent on.
    subscriber_chans: HashMap<String, HashMap<TypeId, PubSub<EventMessage>>>,
    monitor_chan: PubSub<StatusUpdate>,
}

impl<T: Daemon> DaemonActor<T> {
    fn new(daemon: Box<T>, monitor_chan: PubSub<StatusUpdate>) -> Self {
        let outputs = daemon.get_outputs();
        let mut subscriber_chans = HashMap::<String, HashMap<TypeId, PubSub<EventMessage>>>::new();
        for (name, supported_types) in &outputs {
            let mut chans_for_conn = HashMap::new();
            supported_types.iter().for_each(|tpe| {
                chans_for_conn.insert(
                    tpe.clone(),
                    PubSub::<EventMessage>::new(kameo_actors::DeliveryStrategy::Guaranteed),
                );
            });
            subscriber_chans.insert(name.clone(), chans_for_conn);
        }

        DaemonActor {
            id: new_id(),
            daemon,
            outputs,
            subscriber_chans,
            monitor_chan,
        }
    }
    /// Subscribes the given ActorRef to the output conn_name. If the
    /// output conn_name cannot be found, or if the Node was improperly
    /// instantiated, then an error is returned.
    ///
    /// ## Parameters
    /// - `conn_name`: The **output** conn_name of this Node that is being subscribed to.
    /// - `r`: The `ActorRef` that will be produced to.
    /// - `supported_types`: The list of supported types that the subscriber can handle, in priority order.
    /// This is used for type negotiation between this Node and the subscriber.
    ///
    /// ## Returns:
    /// - `Ok(negotiated_type)` - If the subscription is successful, return a single `TypeId` that this Node has
    /// agreed to produce.
    /// - `Err(reason)` - If the output conn_name could not be found, or if the type could not be negotiated.
    fn handle_subscribe_to<Subscriber: Actor + Message<EventMessage>>(
        &mut self,
        conn_name: String,
        r: ActorRef<Subscriber>,
        supported_types: &Vec<TypeId>,
    ) -> Result<TypeId, String> {
        if !self.subscriber_chans.contains_key(&conn_name) {
            return Err(format!(
                "cannot subscribe to conn named '{}' (expected one of {:?})",
                conn_name,
                self.outputs.keys()
            ));
        }

        // TODO: the subscribe logic here should match what's in Node exactly.
        let chans_per_type = self.subscriber_chans.get_mut(&conn_name).unwrap();
        let mut types = vec![];
        for (tpe, chan) in chans_per_type {
            for supported_type in supported_types {
                if supported_type == tpe {
                    chan.subscribe(r);
                    return Ok(tpe.clone());
                }
            }
            types.push(tpe.clone());
        }
        Err(format!("supported type set for output named '{}' ({:?}) is disjoint with supported types for subscriber ({:?})",
            conn_name, types, supported_types))
    }

    async fn produce_outputs(&mut self, events: Vec<Event>) -> Result<(), String> {
        for event in events {
            // TODO: we need to check that if an Event is produced for a conn_name, a matching Event has been produced
            // for EVERY VARIANT of that conn_name's negotiated types
            if let Some(subs_for_conn) = self.subscriber_chans.get_mut(&event.conn_name) {
                if let Some(chan) = subs_for_conn.get_mut(&event.get_data().type_id()) {
                    chan.publish(Arc::new(event)).await;
                } else {
                    return Err(format!(
                        "output validation failed: type for output conn named '{}' ({:?}) does not match negotiated types {:?}",
                        event.conn_name,
                        event.get_data().type_id(),
                        subs_for_conn.keys(),
                    ));
                }
            } else {
                return Err(format!(
                    "output validation failed: failed to find output named '{}'",
                    event.conn_name
                ));
            }
        }
        Ok(())
    }
}

impl<T: Daemon> Message<()> for DaemonActor<T> {
    type Reply = ();
    async fn handle(
        &mut self,
        _: (),
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // TODO: Add MuetlContext similar to Node's. Maybe pull that common functionality out into a trait?
        match self.daemon.run() {
            Ok((events, status)) => {
                match self.produce_outputs(events).await {
                    Ok(()) => {}
                    Err(reason) => println!("failed to produce events: {}", reason),
                }
                if let Some(stat) = status {
                    self.monitor_chan
                        .publish(StatusUpdate {
                            id: self.id,
                            status: stat,
                        })
                        .await
                }
                // Immediately enqueue another run
                ctx.actor_ref().tell(()).await.unwrap();
            }
            Err(reason) => println!("Daemon encountered error: {}", reason),
        }
    }
}

impl<T: Daemon> Actor for DaemonActor<T> {
    type Args = Self;
    type Error = Infallible;
    async fn on_start(args: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        // Send a trigger message to kick off the daemon.
        actor_ref.tell(()).await.unwrap();
        Ok(args)
    }
}
