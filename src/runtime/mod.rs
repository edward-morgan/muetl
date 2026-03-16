pub mod connection;
pub mod error;
pub mod event;
pub mod monitor_actor;
pub mod operator_actor;
pub mod root;
pub mod sink_actor;
pub mod source_actor;

use std::{any::TypeId, collections::HashMap, sync::Arc};

use crate::messages::event::Event;
use crate::runtime::error::RuntimeError;
use crate::runtime::event::InternalEvent;

/// The Message type that internal actors pass around.
pub type EventMessage = Arc<InternalEvent>;

#[derive(Debug, Clone)]
pub enum NegotiatedType {
    Singleton(TypeId),
    AllOf(Vec<TypeId>),
}

impl NegotiatedType {
    /// The set of events passed in is validated against the negotiated type:
    /// 1. If a singleton type, all events must be of that type.
    /// 2. If an AllOf type, then the list of events must contain exactly one
    /// event for each type specified.
    pub fn validate_types(&self, events: Vec<&Event>) -> Result<(), RuntimeError> {
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
                    Err(RuntimeError::TypeMismatch {
                        event_type: tpe.clone(),
                        illegal_events,
                    })
                } else {
                    Ok(())
                }
            }
            Self::AllOf(types) => {
                if types.len() != events.len() {
                    return Err(RuntimeError::AllOfTypeMismatch {
                        num_events: events.len(),
                        num_types: types.len(),
                    });
                }
                let mut present = HashMap::new();
                for ev in events {
                    if present.contains_key(&(&*ev.get_data()).type_id()) {
                        return Err(RuntimeError::DuplicateTypeError {
                            event_type_id: (&*ev.get_data()).type_id(),
                            possible_type_ids: types.clone(),
                        });
                    } else {
                        present.insert((&*ev.get_data()).type_id(), ());
                    }
                }
                Ok(())
            }
        }
    }
}
