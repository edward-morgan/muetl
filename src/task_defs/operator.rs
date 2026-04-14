use std::sync::Arc;

use async_trait::async_trait;
use kameo::actor::ActorRef;

use crate::{messages::event::Event, runtime::operator_actor::OperatorActor};

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

    /// As opposed to new(), which takes place before actor instantiation, on_start() is called
    /// just after the Actor wrapping this Operator is spawned. This function can be used for
    /// post-spawn initialization, specifically initialization that requires a reference
    /// to the Actor responsible for this Task.
    ///
    /// This function is optional; the default implementation of `on_start` is a no-op.
    async fn on_start(&mut self, _actor_ref: ActorRef<OperatorActor>) -> Result<(), String> {
        Ok(())
    }
}
