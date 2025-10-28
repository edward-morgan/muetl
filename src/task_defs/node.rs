use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::Arc,
};

use crate::messages::event::Event;
use crate::task_defs::TaskDef;
use crate::task_defs::TaskResult;

use super::{HasInputs, HasOutputs, MuetlContext};

pub trait Node: HasInputs + HasOutputs + Send + Sync {
    // fn get_negotiated_types_for_conn(&self) -> Vec<TypeId>;
    /// Handle an Event sent to conn_name. The handler should disregard the conn_name
    /// inside the Event; that will be the name of the output conn from the source.
    fn handle_event_for_conn(
        &mut self,
        ctx: &MuetlContext,
        conn_name: &String,
        ev: Arc<Event>,
    ) -> TaskResult;

    /// Wrapper around `handle_event_for_conn()` that performs runtime type checking against the types
    /// specified by `get_outputes()`.
    fn handle_event(
        &mut self,
        ctx: &MuetlContext,
        conn_name: &String,
        ev: Arc<Event>,
    ) -> TaskResult {
        match self.handle_event_for_conn(ctx, conn_name, ev.clone()) {
            Ok((events, status)) => {
                // Validate each of the outputs against the types
                if let Err(reason) = self.validate_output(&events) {
                    Err(reason)
                } else {
                    Ok((events, status))
                }
            }
            e => e,
        }
    }
}
