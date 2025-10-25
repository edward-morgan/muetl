use std::any::TypeId;
use std::collections::HashMap;

use crate::task_defs::daemon::Daemon;
use crate::task_defs::{ConfigField, HasOutputs, TaskConfig, TaskConfigTpl, TaskDef};
use envconfig::Envconfig;
use futures::Stream;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::{consumer::StreamConsumer, ClientConfig};

pub struct KafkaConsumer {
    consumer: Option<StreamConsumer>,
    negotiated_outputs_for_conns: HashMap<String, Vec<TypeId>>,
}

impl KafkaConsumer {
    fn new() -> Self {
        Self {
            consumer: None,
            negotiated_outputs_for_conns: HashMap::new(),
        }
    }
}

impl TaskDef for KafkaConsumer {
    fn task_config_tpl(&self) -> Option<crate::task_defs::TaskConfigTpl> {
        Some(TaskConfigTpl {
            fields: vec![
                ConfigField::required("bootstrap.servers"),
                ConfigField::required("input.topic"),
            ],
            disallow_unknown_fields: false,
        })
    }
    fn init(&mut self, task_config: TaskConfig) -> Result<(), String> {
        let mut config = ClientConfig::new();
        config
            .set(
                "bootstrap.servers",
                task_config
                    .get("bootstrap.servers")
                    .unwrap()
                    .try_into_str()
                    .unwrap(),
            )
            .set_log_level(rdkafka::config::RDKafkaLogLevel::Debug);

        let consumer: StreamConsumer = config.create().expect("Kafka consumer creation failed!");

        let topic = task_config
            .get("input.topic")
            .unwrap()
            .try_into_str()
            .unwrap();

        consumer
            .subscribe(vec![topic.as_str()].as_slice())
            .expect(format!("failed to subscribe to topic '{}'", topic).as_str());

        self.consumer = Some(consumer);

        Ok(())
    }
    fn deinit(self) -> Result<(), String> {
        Ok(())
    }
}

impl HasOutputs for KafkaConsumer {
    fn get_outputs(&self) -> std::collections::HashMap<String, Vec<std::any::TypeId>> {
        HashMap::new()
    }
    fn set_negotiated_outputs_for_conn(&mut self, conn_name: String, types: Vec<TypeId>) {
        self.negotiated_outputs_for_conns.insert(conn_name, types);
    }
    fn get_negotiated_outputs_for_conn(&mut self, conn_name: String) -> Option<&Vec<TypeId>> {
        self.negotiated_outputs_for_conns.get(&conn_name)
    }
}

impl Daemon for KafkaConsumer {
    fn get_outputs(&self) -> std::collections::HashMap<String, Vec<std::any::TypeId>> {}
}
