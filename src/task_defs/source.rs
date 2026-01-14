use std::future::Future;
use std::pin::Pin;

use crate::task_defs::*;

pub trait Source: TaskDef + Send + Sync {
    fn run<'a>(&'a mut self, ctx: &'a MuetlContext)
        -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

    /// Called before the source shuts down, allowing it to flush any buffered data.
    /// Default implementation does nothing.
    fn prepare_shutdown<'a>(&'a mut self, _ctx: &'a MuetlContext)
        -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            // Default no-op implementation
        })
    }
}
