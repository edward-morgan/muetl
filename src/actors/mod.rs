use kameo::prelude::*;
use std::{
    any::{Any, TypeId},
    collections::{HashMap, HashSet},
    future::Future,
    sync::Arc,
};

use kameo_actors::{pubsub::PubSub, DeliveryStrategy};

use crate::runtime::event::InternalEvent;
use crate::{messages::event::Event, task_defs::OutputType};

pub mod daemon;
pub mod monitor;
pub mod node;
pub mod sink;

/// The Message type that internal actors pass around.
pub type EventMessage = Arc<InternalEvent>;

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
                        // Extract the underlying type, otherwise we're getting the TypeId of the Arc
                        if (&*ev.get_data()).type_id() != *tpe {
                            Some(format!(
                                "[{}: {:?}]",
                                ev.name.clone(),
                                (&*ev.get_data()).type_id()
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
                    if present.contains_key(&(&*ev.get_data()).type_id()) {
                        return Err(format!("duplicate event for type {:?}; must have a single event for each of {:?}", (&*ev.get_data()).type_id(), types));
                    } else {
                        present.insert((&*ev.get_data()).type_id(), ());
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
    /// Retrieve the internal sender ID for the given node and output conn_name.
    /// If the conn_name doesn't exist, return None.
    fn get_sender_id_for(&self, conn_name: &String) -> Result<u64, String>;
    fn get_outputs(&mut self) -> HashMap<String, OutputType>;
    fn get_subscriber_channel(&mut self, conn_name: &String) -> Result<&mut Subscription, String>;

    /// Produce the given Event to all subscribers for its conn_name. This function
    /// performs a runtime type check to make sure that:
    /// 1. The Event's `conn_name` exists in the outputs of the owned TaskDef.
    /// 2. The underlying type of the Event's `data` matches the type of the TaskDef's output.
    fn produce_output(&mut self, event: Event) -> impl Future<Output = Result<(), String>> {
        async move {
            let sender_id = self.get_sender_id_for(&event.conn_name)?;
            let subscription = self.get_subscriber_channel(&event.conn_name)?;

            // Ensure that the event being produced matches the types listed for
            // this output.
            match subscription.chan_type.validate_types(vec![&event]) {
                Ok(()) => {
                    subscription
                        .chan
                        .publish(Arc::new(InternalEvent {
                            sender_id: sender_id,
                            event: Arc::new(event),
                        }))
                        .await;
                    Ok(())
                }
                e @ Err(_) => e,
            }
        }
    }
}
