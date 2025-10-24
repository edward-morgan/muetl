pub mod daemon;
pub mod node;
pub mod sink;
pub mod source;
pub mod task;

use crate::messages::{event::Event, Status};

/// A TaskDef represents any process that is executed by muetl.
pub trait TaskDef {
    fn init(&mut self) -> Result<(), String>;
    fn deinit(self) -> Result<(), String>;
}

pub trait Input<T> {
    const conn_name: &'static str;
    fn handle(&mut self, input: &T) -> TaskResult;
}

pub trait Output<T> {
    const conn_name: &'static str;
}

pub type TaskResult = Result<(Vec<Event>, Option<Status>), String>;
