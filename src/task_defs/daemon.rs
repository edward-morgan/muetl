use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::{Arc, Mutex},
    task::Poll,
};

use kameo::{actor::ActorRef, prelude::Message, Actor};
use tokio::sync;
use tokio_stream::Stream;

use crate::messages::event::Event;
use crate::task_defs::*;

pub trait Daemon: HasOutputs + Send + Sync {
    /// After the underlying event handling has returned a set of Events, validate that each one's
    /// conn_name matches the data type. If any Events do not match the expected conn_name - type
    /// declared by the Node's implementation of Output<T>, then an error is returned.
    fn validate_output(&self, events: &Vec<Event>) -> Result<(), String> {
        let outputs = self.get_outputs();
        for event in events {
            if let Some(exp_types) = outputs.get(&event.conn_name) {
                if !exp_types.contains(&event.get_data().type_id()) {
                    return Err(
                        format!("output Event for conn named '{}' has invalid type {:?} (expected one of {:?})",
                            event.conn_name,
                            event.get_data().type_id(),
                            exp_types));
                }
            }
        }
        Ok(())
    }

    fn run(&mut self, ctx: &MuetlContext) -> TaskResult;
}
