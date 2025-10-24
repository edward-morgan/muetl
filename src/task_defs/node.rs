use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::Arc,
};

use crate::messages::event::Event;
use crate::task_defs::TaskDef;
use crate::task_defs::TaskResult;

pub trait Node: TaskDef + Send + Sync {
    fn get_inputs(&self) -> HashMap<String, Vec<TypeId>>;
    fn get_outputs(&self) -> HashMap<String, Vec<TypeId>>;
    /// Handle an Event sent to conn_name. The handler should disregard the conn_name
    /// inside the Event; that will be the name of the output conn from the source.
    fn handle_event(&mut self, conn_name: &String, ev: Arc<Event>) -> TaskResult;

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
}
