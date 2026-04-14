use std::sync::Arc;

use crate::{messages::event::Event, runtime::sink_actor::SinkActor};
use async_trait::async_trait;
use kameo::actor::ActorRef;

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

    /// As opposed to new(), which takes place before actor instantiation, on_start() is called
    /// just after the Actor wrapping this Sink is spawned. This function can be used for
    /// post-spawn initialization, specifically initialization that requires a reference
    /// to the Actor responsible for this Task.
    ///
    /// This function is optional; the default implementation of `on_start` is a no-op.
    async fn on_start(&mut self, _actor_ref: ActorRef<SinkActor>) -> Result<(), String> {
        Ok(())
    }
}
