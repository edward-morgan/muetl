use std::{any::TypeId, collections::HashMap, sync::Arc};

use futures::FutureExt;
use kameo::{actor::ActorRef, error::Infallible, prelude::Message, Actor};
use kameo_actors::pubsub::PubSub;
use tokio::{
    select,
    sync::mpsc::{self},
};
use tokio_stream::StreamExt;

use crate::{
    messages::{event::Event, Status, StatusUpdate},
    task_defs::{daemon::Daemon, MuetlContext},
    util::new_id,
};

use super::EventMessage;

type OwnedDaemon<T> = Option<Box<T>>;

pub struct DaemonActor<T: 'static>
where
    T: Daemon + Send,
{
    id: u32,
    daemon: OwnedDaemon<T>,
    outputs: HashMap<String, Vec<TypeId>>,
    /// For each output conn_name, keep a mapping of negotiated types to the PubSub channels results will be sent on.
    subscriber_chans: HashMap<String, HashMap<TypeId, PubSub<EventMessage>>>,
    monitor_chan: PubSub<StatusUpdate>,
    current_context: MuetlContext,
}

impl<T: Daemon> DaemonActor<T> {
    pub fn new(daemon: OwnedDaemon<T>, monitor_chan: PubSub<StatusUpdate>) -> Self {
        let outputs = daemon.as_ref().unwrap().get_outputs();
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

        // Throwaway
        let (results_tx, _) = mpsc::channel(1);
        let (status_tx, _) = mpsc::channel(1);

        DaemonActor {
            id: new_id(),
            daemon,
            outputs,
            subscriber_chans,
            monitor_chan,
            current_context: MuetlContext {
                current_subscribers: HashMap::new(),
                results: results_tx,
                status: status_tx,
            },
        }
    }

    /// Update the current MuetlContext with the given TypeId for the given output conn_name.
    /// This updates the context that is passed to the wrapped node and should be called
    /// whenever a new subscription request is validated against this Node.
    fn add_subscriber_to_context(&mut self, conn_name: String, tpe: TypeId) {
        // Allow panics here because the input should have already been validated.
        let subs = self
            .current_context
            .current_subscribers
            .get_mut(&conn_name)
            .unwrap();

        if !subs.contains(&tpe) {
            subs.push(tpe);
        }
    }

    fn remove_subscriber_from_context(&mut self, conn_name: String, tpe: TypeId) {
        if let Some(subs_for_conn) = self.current_context.current_subscribers.get_mut(&conn_name) {
            subs_for_conn.extract_if(.., |t| tpe == *t);
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
    pub fn handle_subscribe_to<Subscriber: Actor + Message<EventMessage>>(
        &mut self,
        conn_name: String,
        r: ActorRef<Subscriber>,
        requested_types: &Vec<TypeId>,
    ) -> Result<TypeId, String> {
        // If the conn_name exists
        if let Some(supported_output_types) = self.outputs.get(&conn_name) {
            // Search for the first output type that is contained in the set of
            // types requested by the subscriber. Note that ordering matters here -
            // the earlier in the requested type set a TypeId is, the higher
            // priority it has of being matched.
            if let Some(matching_type) = requested_types
                .iter()
                .find(|&requested_type| supported_output_types.contains(requested_type))
            {
                // TODO: we need to add special support for matching against RegisteredType, which
                // should consist of making sure the given TypeId exists in some global registry of
                // structs.
                let subscriber_chans = self.subscriber_chans.get_mut(&conn_name).unwrap();
                // If it already exists in subscriber_chans, just subscribe to the PubSub
                if let Some(chan_for_type) = subscriber_chans.get_mut(matching_type) {
                    chan_for_type.subscribe(r);
                } else {
                    // Create a new PubSub for the given type and subscribe to it.
                    let mut chan =
                        PubSub::<EventMessage>::new(kameo_actors::DeliveryStrategy::Guaranteed);
                    chan.subscribe(r);
                    subscriber_chans.insert(matching_type.clone(), chan);
                }
                // Make sure to update the current MuetlContext so that the wrapped Node knows that
                // it should generate outputs for the given type.
                self.add_subscriber_to_context(conn_name, matching_type.clone());
                return Ok(matching_type.clone());
            } else {
                Err(format!("supported type set for output named '{}' ({:?}) is disjoint with types requested by subscriber ({:?})",
                    conn_name, supported_output_types, requested_types))
            }
        } else {
            return Err(format!(
                "cannot subscribe to conn named '{}' (expected one of {:?})",
                conn_name,
                self.outputs.keys()
            ));
        }
    }

    async fn produce_output(&mut self, event: Event) -> Result<(), String> {
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
        let (result_tx, mut result_rx) = mpsc::channel(100);
        let (status_tx, mut status_rx) = mpsc::channel(100);

        // Create a context for the daemon to own
        let daemon_context = MuetlContext {
            current_subscribers: self.current_context.current_subscribers.clone(),
            results: result_tx,
            status: status_tx,
        };
        let mut daemon = self.daemon.take().unwrap();

        let fut = tokio::spawn(async move {
            daemon.run(&daemon_context).await;
            daemon
        });

        loop {
            select! {
                res = result_rx.recv() => {
                    if let Some(result) = res {
                        println!("Received result {:?}", result);
                        match self.produce_output(result).await {
                            Ok(()) => break,
                             Err(reason) => println!("failed to produce events: {}", reason),
                        }
                    } else if status_rx.is_closed() {
                        break;
                    }
                },
                res = status_rx.recv() => {
                    if let Some(status) = res {
                        println!("Received status {:?}", status);
                        let update = StatusUpdate{status: status, id: self.id};
                        self.monitor_chan.publish(update).await;
                    } else if result_rx.is_closed() {
                        break;
                    }
                },
            }
        }

        match fut.await {
            Ok(daemon) => {
                println!("Run finished");
                // Replace the daemon
                self.daemon = Some(daemon);
                // Enqueue another iteration
                ctx.actor_ref().tell(()).await.unwrap();
            }
            // Don't enqueue another iteration
            Err(e) => {
                println!("Daemon task panicked: {:?}", e);
                // Send a failure message to the monitor
                self.monitor_chan
                    .publish(StatusUpdate {
                        id: self.id,
                        status: Status::Failed(e.to_string()),
                    })
                    .await;
                // Stop the current task
                ctx.stop();
            }
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
