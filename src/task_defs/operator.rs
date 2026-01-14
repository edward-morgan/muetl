use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::messages::event::Event;

use super::{MuetlContext, TaskDef};

pub trait Operator: TaskDef + Send + Sync {
    /// Handle an Event sent to conn_name. The handler should disregard the conn_name
    /// inside the Event; that will be the name of the output conn from the source.
    ///
    /// Unlike a Sink, an Operator can produce output events by sending them to the
    /// context's `results` channel.
    fn handle_event_for_conn<'a>(
        &'a mut self,
        ctx: &'a MuetlContext,
        conn_name: &'a String,
        ev: Arc<Event>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

    /// Called before the operator shuts down, allowing it to flush any buffered data.
    /// Default implementation does nothing.
    fn prepare_shutdown<'a>(&'a mut self, _ctx: &'a MuetlContext)
        -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            // Default no-op implementation
        })
    }
}
