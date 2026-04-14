use std::sync::Arc;

use crate::messages::event::Event;
use async_trait::async_trait;

use super::{MuetlSinkContext, TaskDef};

#[async_trait]
pub trait Sink: TaskDef + Send + Sync {
    /// Handle an Event sent to conn_name. The handler should disregard the conn_name
    /// inside the Event; that will be the name of the output conn from the source.
    async fn handle_event_for_conn(
        &mut self,
        ctx: &MuetlSinkContext,
        conn_name: &String,
        ev: Arc<Event>,
    );

    /// Called before the sink shuts down, allowing it to flush any buffered data.
    /// Default implementation does nothing.
    async fn prepare_shutdown(&mut self, _ctx: &MuetlSinkContext) {
        // Default no-op implementation
    }
}
