use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use tokio::time::sleep;

use crate::{
    messages::event::Event,
    task_defs::{
        source::Source, ConfigField, Output, TaskConfig, TaskConfigTpl, TaskConfigValue, TaskDef,
    },
};

pub struct Ticker {
    t: u64,
    period: Duration,
    iterations: u64,
}
impl Ticker {
    pub fn new(config: &TaskConfig) -> Result<Box<dyn Source>, String> {
        Ok(Box::new(Ticker {
            t: 0,
            period: Duration::from_millis(u64::try_from(config.get("period_ms").unwrap()).unwrap()),
            iterations: u64::try_from(config.get("iterations").unwrap()).unwrap(),
        }))
    }
}
impl TaskDef for Ticker {
    fn task_config_tpl(&self) -> Option<crate::task_defs::TaskConfigTpl> {
        Some(TaskConfigTpl {
            fields: vec![
                ConfigField::optional_with_default("period_ms", TaskConfigValue::Uint(1000)),
                ConfigField::optional_with_default("iterations", TaskConfigValue::Uint(10)),
            ],
            disallow_unknown_fields: true,
        })
    }

    fn deinit(&mut self) -> Result<(), String> {
        Ok(())
    }
}

impl Output<u64> for Ticker {
    const conn_name: &'static str = "tick";
}

#[async_trait]
impl Source for Ticker {
    async fn run(&mut self, ctx: &crate::task_defs::MuetlContext) -> () {
        if self.t == self.iterations {
            println!("Reached max iters ({})", self.t);
            ctx.status
                .send(crate::messages::Status::Finished)
                .await
                .unwrap();
        } else {
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
}
