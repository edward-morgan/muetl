use async_trait::async_trait;

use crate::task_defs::*;

#[async_trait]
pub trait Source: TaskDef + Send + Sync {
    async fn run(&mut self, ctx: &MuetlContext);

    /// Called before the source shuts down, allowing it to flush any buffered data.
    /// Default implementation does nothing.
    async fn prepare_shutdown(&mut self, _ctx: &MuetlContext) {
        // Default no-op implementation
    }
}
