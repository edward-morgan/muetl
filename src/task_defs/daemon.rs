use async_trait::async_trait;

use crate::task_defs::*;

#[async_trait]
pub trait Daemon: TaskDef + Send + Sync {
    async fn run(&mut self, ctx: &MuetlContext);
}
