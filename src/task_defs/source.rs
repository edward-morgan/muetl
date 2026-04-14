use async_trait::async_trait;
use kameo::actor::ActorRef;

use crate::{runtime::source_actor::SourceActor, task_defs::*};

#[async_trait]
pub trait Source: TaskDef + Send + Sync {
    async fn run(&mut self, ctx: &MuetlContext);

    /// Called before the source shuts down, allowing it to flush any buffered data.
    /// Default implementation does nothing.
    async fn prepare_shutdown(&mut self, _ctx: &MuetlContext) {
        // Default no-op implementation
    }

    /// As opposed to new(), which takes place before actor instantiation, on_start() is called
    /// just after the Actor wrapping this Source is spawned. This function can be used for
    /// post-spawn initialization, specifically initialization that requires a reference
    /// to the Actor responsible for this Task.
    ///
    /// This function is optional; the default implementation of `on_start` is a no-op.
    async fn on_start(&mut self, _actor_ref: ActorRef<SourceActor>) -> Result<(), String> {
        Ok(())
    }
}
