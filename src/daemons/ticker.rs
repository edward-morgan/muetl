use std::{any::TypeId, collections::HashMap, sync::Arc};

use crate::{
    messages::event::Event,
    task_defs::{daemon::Daemon, HasOutputs, Output, TaskDef},
};

pub struct Ticker {
    t: u64,
}
impl TaskDef for Ticker {
    fn new() -> Self {
        Ticker { t: 0 }
    }
    fn task_config_tpl(&self) -> Option<crate::task_defs::TaskConfigTpl> {
        None
    }
    fn init(&mut self, config: crate::task_defs::TaskConfig) -> Result<(), String> {
        Ok(())
    }
    fn deinit(self) -> Result<(), String> {
        Ok(())
    }
}

impl Output<u64> for Ticker {
    const conn_name: &'static str = "tick";
}

impl HasOutputs for Ticker {
    fn get_outputs(&self) -> std::collections::HashMap<String, Vec<std::any::TypeId>> {
        let mut hm = HashMap::new();
        hm.insert("tick".to_string(), vec![TypeId::of::<u64>()]);
        hm
    }
}

impl Daemon for Ticker {
    async fn run(&mut self, ctx: &crate::task_defs::MuetlContext) -> () {
        ctx.results
            .send(Event::new(
                "tick".to_string(),
                "tick".to_string(),
                HashMap::new(),
                Arc::new(self.t),
            ))
            .await
            .unwrap();
        self.t += 1;
    }
}
