use crate::{
    impl_sink_handler,
    task_defs::{MuetlSinkContext, SinkInput, TaskConfig, TaskDef},
};

pub struct LogSink {}

impl LogSink {
    pub fn new(_config: &TaskConfig) -> Result<Box<dyn crate::task_defs::sink::Sink>, String> {
        Ok(Box::new(LogSink {}))
    }
}

impl SinkInput<u64> for LogSink {
    const conn_name: &'static str = "input";
    async fn handle(&mut self, _ctx: &MuetlSinkContext, input: &u64) {
        tracing::info!(value = %input, "LogSink received");
    }
}

impl TaskDef for LogSink {}

impl_sink_handler!(LogSink, "input" => u64);
