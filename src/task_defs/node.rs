use std::{
    future::Future,
    sync::Arc,
};

use tokio::sync::mpsc;

use crate::messages::{event::Event, Status};
use crate::task_defs::TaskResult;

use super::{HasInputs, HasOutputs, MuetlContext};

pub trait Node: HasInputs + HasOutputs + Send + Sync {
    /// Handle an Event sent to conn_name. The handler should disregard the conn_name
    /// inside the Event; that will be the name of the output conn from the source.
    fn handle_event_for_conn(
        &mut self,
        ctx: &MuetlContext,
        conn_name: &String,
        ev: Arc<Event>,
        status: &mut mpsc::Sender<Status>,
    ) -> impl Future<Output = TaskResult> + Send;

    /// Wrapper around `handle_event_for_conn()` that performs runtime type checking against the types
    /// specified by `get_outputes()`.
    fn handle_event(
        &mut self,
        ctx: &MuetlContext,
        conn_name: &String,
        ev: Arc<Event>,
        status: &mut mpsc::Sender<Status>,
    ) -> impl std::future::Future<Output = TaskResult> + std::marker::Send {
        async move {
            match self
                .handle_event_for_conn(ctx, conn_name, ev.clone(), status)
                .await
            {
                // Ok((events, status)) => {
                //     // Validate each of the outputs against the types
                //     if let Err(reason) = self.validate_output(&events) {
                //         Err(reason)
                //     } else {
                //         Ok((events, status))
                //     }
                // }
                // e => e,
                TaskResult::Events(events) => {
                    // Validate each of the outputs against the types
                    if let Err(reason) = self.validate_output(&events) {
                        TaskResult::Error(reason)
                    } else {
                        TaskResult::Events(events)
                    }
                }
                res => res,
            }
        }
    }
}
