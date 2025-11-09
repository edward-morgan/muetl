use std::{any::TypeId, collections::HashMap, sync::Arc, time::Duration};

use tokio::time::sleep;

use crate::{
    messages::event::Event,
    task_defs::{
        daemon::Daemon, ConfigField, HasOutputs, Output, TaskConfig, TaskConfigTpl,
        TaskConfigValue, TaskDef,
    },
};

pub struct Ticker {
    t: u64,
    period: Duration,
}
impl TaskDef for Ticker {
    fn new(config: &TaskConfig) -> Result<Box<Self>, String> {
        Ok(Box::new(Ticker {
            t: 0,
            period: Duration::from_millis(u64::try_from(config.get("period_ms").unwrap()).unwrap()),
        }))
    }

    fn task_config_tpl(&self) -> Option<crate::task_defs::TaskConfigTpl> {
        Some(TaskConfigTpl {
            fields: vec![ConfigField::optional_with_default(
                "period_ms",
                TaskConfigValue::Uint(1000),
            )],
            disallow_unknown_fields: true,
        })
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
                format!("tick-{}", self.t),
                "tick".to_string(),
                HashMap::new(),
                Arc::new(self.t),
            ))
            .await
            .unwrap();
        self.t += 1;
        sleep(self.period).await;
    }
}
