use kameo::prelude::*;
use std::{
    any::{Any, TypeId},
    collections::{HashMap, HashSet},
    future::Future,
    sync::Arc,
};

use kameo_actors::{pubsub::PubSub, DeliveryStrategy};

use crate::{messages::event::Event, task_defs::OutputType};

pub mod daemon;
pub mod monitor;
pub mod node;

/// The Message type that internal actors pass around.
pub type EventMessage = Arc<Event>;

pub enum NegotiatedType {
    Singleton(TypeId),
    AllOf(Vec<TypeId>),
}

impl NegotiatedType {
    // The set of events passed in is validated against the negotiated type:
    // 1. If a singleton type, all events must be of that type.
    // 2. If an AllOf type, then the list of events must contain exactly one
    // event for each type specified.
    pub fn validate_types(&self, events: Vec<&Event>) -> Result<(), String> {
        match self {
            Self::Singleton(tpe) => {
                let illegal_events: Vec<String> = events
                    .iter()
                    .filter_map(|ev| {
                        if ev.get_data().type_id() != *tpe {
                            Some(format!(
                                "[{}: {:?}]",
                                ev.name.clone(),
                                ev.get_data().downcast::<dyn Any>().type_id()
                            ))
                        } else {
                            None
                        }
                    })
                    .collect();
                if illegal_events.len() > 0 {
                    Err(format!(
                        "all events must match single type {:?}, found illegal events {:?}",
                        tpe, illegal_events,
                    ))
                } else {
                    Ok(())
                }
            }
            Self::AllOf(types) => {
                if types.len() != events.len() {
                    return Err(format!(
                        "number of events ({}) does not match the list of required types ({})",
                        events.len(),
                        types.len()
                    ));
                }
                let mut present = HashMap::new();
                for ev in events {
                    if present.contains_key(&ev.get_data().type_id()) {
                        return Err(format!("duplicate event for type {:?}; must have a single event for each of {:?}", ev.get_data().type_id(), types));
                    } else {
                        present.insert(ev.get_data().type_id(), ());
                    }
                }
                Ok(())
            }
        }
    }
}

pub struct Subscription {
    pub chan: PubSub<EventMessage>,
    pub chan_type: NegotiatedType,
}
impl Subscription {
    pub fn new(tpe: NegotiatedType, delivery_strategy: DeliveryStrategy) -> Self {
        Self {
            chan: PubSub::new(delivery_strategy),
            chan_type: tpe,
        }
    }
}

pub trait HasSubscriptions {
    fn get_outputs(&mut self) -> HashMap<String, OutputType>;
    fn get_subscriber_channel(&mut self, conn_name: &String) -> Option<&mut Subscription>;
    fn add_subscriber(&mut self, conn_name: String, tpe: TypeId);
    fn remove_subscriber(&mut self, conn_name: String, tpe: TypeId);

    // /// Subscribes the given ActorRef to the output conn_name. If the
    // /// output conn_name cannot be found, or if the Node was improperly
    // /// instantiated, then an error is returned.
    // ///
    // /// ## Parameters
    // /// - `conn_name`: The **output** conn_name of this Task that is being subscribed to.
    // /// - `r`: The `ActorRef` that will be produced to.
    // /// - `supported_types`: The list of supported types that the subscriber can handle, in priority order.
    // /// This is used for type negotiation between this Node and the subscriber.
    // ///
    // /// ## Returns:
    // /// - `Ok(negotiated_type)` - If the subscription is successful, return a single `TypeId` that this Task has
    // /// agreed to produce.
    // /// - `Err(reason)` - If the output conn_name could not be found, or if the type could not be negotiated.
    // fn handle_subscribe_to<Subscriber: Actor + Message<EventMessage>>(
    //     &mut self,
    //     conn_name: String,
    //     r: ActorRef<Subscriber>,
    //     requested_types: &Vec<TypeId>,
    // ) -> Result<TypeId, String> {
    //     // If the conn_name exists
    //     if let Some(supported_output_types) = self.get_outputs().get_mut(&conn_name) {
    //         // Search for the first output type that is contained in the set of
    //         // types requested by the subscriber. Note that ordering matters here -
    //         // the earlier in the requested type set a TypeId is, the higher
    //         // priority it has of being matched.
    //         if let Some(matching_type) = requested_types
    //             .iter()
    //             .find(|&requested_type| supported_output_types.contains(requested_type))
    //         {
    //             // TODO: we need to add special support for matching against RegisteredType, which
    //             // should consist of making sure the given TypeId exists in some global registry of
    //             // structs.
    //             let subscriber_chans = self.get_subscriber_channel(&conn_name).unwrap();
    //             // If it already exists in subscriber_chans, just subscribe to the PubSub
    //             if let Some(chan_for_type) = subscriber_chans.get_mut(matching_type) {
    //                 chan_for_type.subscribe(r);
    //             } else {
    //                 // Create a new PubSub for the given type and subscribe to it.
    //                 let mut chan =
    //                     PubSub::<EventMessage>::new(kameo_actors::DeliveryStrategy::Guaranteed);
    //                 chan.subscribe(r);
    //                 subscriber_chans.insert(matching_type.clone(), chan);
    //             }
    //             // Make sure to update the current MuetlContext so that the wrapped Node knows that
    //             // it should generate outputs for the given type.
    //             self.add_subscriber(conn_name, matching_type.clone());
    //             return Ok(matching_type.clone());
    //         } else {
    //             Err(format!("supported type set for output named '{}' ({:?}) is disjoint with types requested by subscriber ({:?})",
    //                 conn_name, supported_output_types, requested_types))
    //         }
    //     } else {
    //         return Err(format!(
    //             "cannot subscribe to conn named '{}' (expected one of {:?})",
    //             conn_name,
    //             self.get_outputs().keys()
    //         ));
    //     }
    // }

    /// Produce the given Event to all subscribers for its conn_name. This function
    /// performs a runtime type check to make sure that:
    /// 1. The Event's `conn_name` exists in the outputs of the owned TaskDef.
    /// 2. The underlying type of the Event's `data` matches the type of the TaskDef's output.
    fn produce_output(&mut self, event: Event) -> impl Future<Output = Result<(), String>> {
        async move {
            if let Some(subcription) = self.get_subscriber_channel(&event.conn_name) {
                // Ensure that the event being produced matches the types listed for
                // this output.
                match subcription.chan_type.validate_types(vec![&event]) {
                    Ok(()) => subcription.chan.publish(Arc::new(event)).await,
                    e @ Err(_) => return e,
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

    // fn produce_outputs(&mut self, events: Vec<Event>) -> impl Future<Output = Result<(), String>> {
    //     async move {
    //         if let Some(subcription) = self.get_subscriber_channel(&event.conn_name) {
    //             // Ensure that the event being produced matches the types listed for
    //             // this output.
    //             match subcription.chan_type.validate_types(vec![&event]) {
    //                 Ok(()) => subcription.chan.publish(Arc::new(event)).await,
    //                 e @ Err(_) => return e,
    //             }
    //         } else {
    //             return Err(format!(
    //                 "output validation failed: failed to find output named '{}'",
    //                 event.conn_name
    //             ));
    //         }
    //         Ok(())
    //     }
    // }
    // /// Produce the given Event to all subscribers for its conn_name. This function
    // /// performs a runtime type check to make sure that:
    // /// 1. The Event's `conn_name` exists in the outputs of the owned TaskDef.
    // /// 2. THe underlying type of the Event's `data` matches
    // fn produce_outputs(
    //     &mut self,
    //     conn_name: String,
    //     events: Vec<Event>,
    // ) -> impl Future<Output = Result<(), String>> {
    //     async move {
    //         // First, make sure the conn_name is valid for this Task
    //         if let Some(subs_for_conn) = self.get_subscriber_channel(&conn_name) {
    //             // Check to make sure that if the conn_name has multiple negotiated types, then we have
    //             // exactly one  matching Event for each.
    //             //
    //             // We ideally need a number of Events that's an integer multiple of the number of distinct
    //             // negotiated types for this conn_name - i.e. one per type, two per type, etc.

    //             // The set of type ids that are represented in events
    //             let distinct_event_types =
    //                 HashSet::from_iter(events.iter().map(|ev| &ev.get_data().type_id()));

    //             // If there's only one type for this conn_name, we're okay as long as all Events have the
    //             // right type.
    //             if subs_for_conn.len() == 1 {
    //                 let single_type = subs_for_conn.keys().last().unwrap();
    //                 if distinct_event_types.len() != 1 {
    //                     return Err(format!("output named '{}' has negotiated type '{:?}' but outgoing events have types {:?}", conn_name, single_type, distinct_event_types));
    //                 } else if *distinct_event_types.iter().last().unwrap() != single_type {
    //                     // TODO: Maybe this is covered below?
    //                     return Err(format!(""));
    //                 }
    //             }

    //             if events.len() == 1 {
    //                 // Skip all this below
    //             }

    //             // TODO: Notes
    //             //
    //             // I think this is getting too complex - in practice, how many TaskDefs will support sending multiple output types?
    //             // In addition, doing things this way has a few problems:
    //             // 1. What if a TaskDef wants send only a single event type as output, but it can be one of a set of types?
    //             // 2. What if the process of retrieving an output from a TaskDef 'consumes' something, such that it can't easily
    //             //    copy that value to multiple types?
    //             //
    //             // Because of this, I think the architecture should be simplified to function as TLS algorithm negotiation does -
    //             // multiple options may be supported, but at runtime a single one is chosen. This makes other parts of the system
    //             // more difficult:
    //             // - Now, the process of deciding which negotiated type to use is harder, because what if one type isn't enough to
    //             // cover all subscribers?

    //             // What we want to catch:
    //             // 1. caller sent an extra Event for the same type ID
    //             // 2. caller didn't send Event for each negotiated output type
    //             // 3. caller sent one Event for each negotiated output type, but also an extra one (whether or not the type is valid)
    //         } else {
    //             return Err(format!(
    //                 "output validation failed: failed to find output named '{}'",
    //                 conn_name
    //             ));
    //         }

    //         if let Some(subs_for_conn) = self.get_subscriber_channel(&event.conn_name) {
    //             // TODO: This doesn't work - we have a contract that if an output has negotiated multiple
    //             // types, then EVERY time an Event is published on that output, it must be published against
    //             // EVERY negotiated type.
    //             if let Some(chan) = subs_for_conn.get_mut(&event.get_data().type_id()) {
    //                 chan.publish(Arc::new(event)).await;
    //             } else {
    //                 return Err(format!(
    //                     "output validation failed: type for output conn named '{}' ({:?}) does not match negotiated types {:?}",
    //                     event.conn_name,
    //                     event.get_data().type_id(),
    //                     subs_for_conn.keys(),
    //                 ));
    //             }
    //         } else {
    //             return Err(format!(
    //                 "output validation failed: failed to find output named '{}'",
    //                 event.conn_name
    //             ));
    //         }
    //         Ok(())
    //     }
    // }
}
