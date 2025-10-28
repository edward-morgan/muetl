use crate::messages::StatusUpdate;
use crate::task_defs::MuetlContext;
use crate::util::new_id;
use kameo::actor::ActorRef;
use kameo::message::{Context, Message};
use kameo::Actor;
use kameo_actors::pubsub::PubSub;
use std::any::TypeId;
use std::collections::HashMap;
use std::ops::Deref;
use std::panic;
use std::sync::Arc;

use crate::messages::event::Event;
use crate::task_defs::{node::Node, Input};

use super::EventMessage;

#[derive(Actor)]
pub struct NodeActor<T: 'static>
where
    T: Node,
{
    id: u32,
    node: Box<T>,
    /// The inputs of the wrapped node, retrieved via its get_inputs() function.
    inputs: HashMap<String, Vec<TypeId>>,
    /// The outputs of the wrapped node, retrieved via its get_outputs() function.
    outputs: HashMap<String, Vec<TypeId>>,
    /// For each output conn_name, keep a mapping of negotiated types to the PubSub channels results will be sent on.
    subscriber_chans: HashMap<String, HashMap<TypeId, PubSub<EventMessage>>>,
    /// A mapping of senders to this node, from source (output) conn_name to input conn_name
    input_conn_name_mapping: HashMap<String, String>,
    monitor_chan: PubSub<StatusUpdate>,
    current_context: MuetlContext,
}

impl<T: Node> NodeActor<T> {
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

    // TODO: On unsubscribe, add a corresponding remove_subscriber_from_context()
}

impl<T: Node> NodeActor<T> {
    fn new(node: Box<T>, monitor_chan: PubSub<StatusUpdate>) -> NodeActor<T> {
        let inputs = node.get_inputs();
        let outputs = node.get_outputs();
        let mut subscriber_chans = HashMap::<String, HashMap<TypeId, PubSub<EventMessage>>>::new();
        // Start with an empty set of subscribers for each output. This should be dynamically updated
        // as calls to handle_subscribe_to() are made.
        outputs.iter().for_each(|(conn_name, _)| {
            subscriber_chans.insert(conn_name.clone(), HashMap::new());
        });

        NodeActor {
            id: new_id(),
            node,
            inputs,
            outputs,
            subscriber_chans,
            input_conn_name_mapping: HashMap::new(),
            monitor_chan,
            current_context: MuetlContext {
                current_subscribers: HashMap::new(),
            },
        }
    }

    fn set_source_conn_mapping(&mut self, from_conn_name: String, to_conn_name: String) {
        self.input_conn_name_mapping
            .insert(from_conn_name, to_conn_name);
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

    async fn produce_outputs(&mut self, events: Vec<Event>) -> Result<(), String> {
        for event in events {
            if let Some(subs_for_conn) = self.subscriber_chans.get_mut(&event.conn_name) {
                // TODO: we need to check that if an Event is produced for a conn_name, a matching Event has been produced
                // for EVERY VARIANT of that conn_name's negotiated types
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

impl<T: Node> Message<Arc<Event>> for NodeActor<T> {
    type Reply = ();

    async fn handle(
        &mut self,
        ev: Arc<Event>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Before sending the event to the Node, remap its conn_name from the output of the sender
        // to the input of this Node.
        if let Some(input_conn_name) = self.input_conn_name_mapping.get(&ev.conn_name) {
            match self
                .node
                .handle_event(&self.current_context, input_conn_name, ev.clone())
            {
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
                }
                Err(reason) => println!("Error handling event named {}: {}", ev.name, reason),
            }
        }
    }
}
