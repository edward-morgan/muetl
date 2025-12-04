use std::{any::TypeId, collections::HashMap};

use async_trait::async_trait;

use crate::task_defs::{sink::Sink, Input, SinkInput, TaskConfig, TaskDef};

pub struct LogSink {}

impl LogSink {
    pub fn new(config: &TaskConfig) -> Result<Box<dyn Sink>, String> {
        Ok(Box::new(LogSink {}))
    }
}

impl SinkInput<u64> for LogSink {
    const conn_name: &'static str = "input";
    async fn handle(&mut self, ctx: &crate::task_defs::MuetlSinkContext, input: &u64) {
        println!("[LogSink] {}", input)
    }
}
impl SinkInput<String> for LogSink {
    const conn_name: &'static str = "input";
    async fn handle(&mut self, ctx: &crate::task_defs::MuetlSinkContext, input: &String) {
        println!("[LogSink] {}", input)
    }
}

// impl HasInputs for LogSink {
//     fn get_inputs(&self) -> std::collections::HashMap<String, std::any::TypeId> {
//         let mut hm = HashMap::new();
//         hm.insert("input".to_string(), TypeId::of::<u64>());
//         hm
//     }
// }

impl TaskDef for LogSink {
    fn deinit(&mut self) -> Result<(), String> {
        Ok(())
    }
}

#[async_trait]
impl Sink for LogSink {
    async fn handle_event_for_conn(
        &mut self,
        ctx: &crate::task_defs::MuetlSinkContext,
        conn_name: &String,
        ev: std::sync::Arc<crate::messages::event::Event>,
    ) {
        match conn_name.as_str() {
            "input" => {
                // TODO: handle strings too
                self.handle(ctx, &*ev.get_data().downcast::<u64>().unwrap())
                    .await
            }
            _ => println!("unknown incoming conn_name {}", conn_name),
        }
    }
}
