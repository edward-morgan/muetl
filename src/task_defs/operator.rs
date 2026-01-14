use std::sync::Arc;

use async_trait::async_trait;

use crate::messages::event::Event;

use super::{MuetlContext, TaskDef};

#[async_trait]
pub trait Operator: TaskDef + Send + Sync {
    /// Handle an Event sent to conn_name. The handler should disregard the conn_name
    /// inside the Event; that will be the name of the output conn from the source.
    ///
    /// Unlike a Sink, an Operator can produce output events by sending them to the
    /// context's `results` channel.
    async fn handle_event_for_conn(
        &mut self,
        ctx: &MuetlContext,
        conn_name: &String,
        ev: Arc<Event>,
    );

    /// Called before the operator shuts down, allowing it to flush any buffered data.
    /// Default implementation does nothing.
    async fn prepare_shutdown(&mut self, _ctx: &MuetlContext) {
        // Default no-op implementation
    }
}
