use std::{any::TypeId, collections::HashMap, sync::Arc};

use kameo::{actor::ActorRef, prelude::Message, Actor};
use kameo_actors::pubsub::PubSub;

use crate::messages::event::Event;

pub mod daemon;
pub mod monitor;
pub mod node;

/// The Message type that internal actors pass around.
pub type EventMessage = Arc<Event>;

// pub trait Producer {
//     fn get_outputs(&self) -> HashMap<String, Vec<TypeId>>;
//     fn get_output_type_for_conn(&self, conn_name: String) -> Option<Vec<TypeId>>;
//     fn add_subscriber_to_context(&mut self, conn_name: String, tpe: TypeId);

//     /// Subscribes the given ActorRef to the output conn_name. If the
//     /// output conn_name cannot be found, or if the Node was improperly
//     /// instantiated, then an error is returned.
//     ///
//     /// ## Parameters
//     /// - `conn_name`: The **output** conn_name of this Node that is being subscribed to.
//     /// - `r`: The `ActorRef` that will be produced to.
//     /// - `supported_types`: The list of supported types that the subscriber can handle, in priority order.
//     /// This is used for type negotiation between this Node and the subscriber.
//     ///
//     /// ## Returns:
//     /// - `Ok(negotiated_type)` - If the subscription is successful, return a single `TypeId` that this Node has
//     /// agreed to produce.
//     /// - `Err(reason)` - If the output conn_name could not be found, or if the type could not be negotiated.
//     fn handle_subscribe_to<Subscriber: Actor + Message<EventMessage>>(
//         &mut self,
//         conn_name: String,
//         r: ActorRef<Subscriber>,
//         requested_types: &Vec<TypeId>,
//     ) -> Result<TypeId, String> {
//         // If the conn_name exists
//         if let Some(supported_output_types) = self.outputs.get(&conn_name) {
//             // Search for the first output type that is contained in the set of
//             // types requested by the subscriber. Note that ordering matters here -
//             // the earlier in the requested type set a TypeId is, the higher
//             // priority it has of being matched.
//             if let Some(matching_type) = requested_types
//                 .iter()
//                 .find(|&requested_type| supported_output_types.contains(requested_type))
//             {
//                 // TODO: we need to add special support for matching against RegisteredType, which
//                 // should consist of making sure the given TypeId exists in some global registry of
//                 // structs.
//                 let subscriber_chans = self.subscriber_chans.get_mut(&conn_name).unwrap();
//                 // If it already exists in subscriber_chans, just subscribe to the PubSub
//                 if let Some(chan_for_type) = subscriber_chans.get_mut(matching_type) {
//                     chan_for_type.subscribe(r);
//                 } else {
//                     // Create a new PubSub for the given type and subscribe to it.
//                     let mut chan =
//                         PubSub::<EventMessage>::new(kameo_actors::DeliveryStrategy::Guaranteed);
//                     chan.subscribe(r);
//                     subscriber_chans.insert(matching_type.clone(), chan);
//                 }
//                 // Make sure to update the current MuetlContext so that the wrapped Node knows that
//                 // it should generate outputs for the given type.
//                 self.add_subscriber_to_context(conn_name, matching_type.clone());
//                 return Ok(matching_type.clone());
//             } else {
//                 Err(format!("supported type set for output named '{}' ({:?}) is disjoint with types requested by subscriber ({:?})",
//                     conn_name, supported_output_types, requested_types))
//             }
//         } else {
//             return Err(format!(
//                 "cannot subscribe to conn named '{}' (expected one of {:?})",
//                 conn_name,
//                 self.outputs.keys()
//             ));
//         }
//     }

//     async fn produce_outputs(&mut self, events: Vec<Event>) -> Result<(), String> {
//         for event in events {
//             if let Some(subs_for_conn) = self.subscriber_chans.get_mut(&event.conn_name) {
//                 // TODO: we need to check that if an Event is produced for a conn_name, a matching Event has been produced
//                 // for EVERY VARIANT of that conn_name's negotiated types
//                 if let Some(chan) = subs_for_conn.get_mut(&event.get_data().type_id()) {
//                     chan.publish(Arc::new(event)).await;
//                 } else {
//                     return Err(format!(
//                         "output validation failed: type for output conn named '{}' ({:?}) does not match negotiated types {:?}",
//                         event.conn_name,
//                         event.get_data().type_id(),
//                         subs_for_conn.keys(),
//                     ));
//                 }
//             } else {
//                 return Err(format!(
//                     "output validation failed: failed to find output named '{}'",
//                     event.conn_name
//                 ));
//             }
//         }
//         Ok(())
//     }
// }
