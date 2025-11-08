use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::messages::event::Event;
use crate::messages::Status;
use crate::task_defs::daemon::Daemon;
use crate::task_defs::{
    ConfigField, HasOutputs, MuetlContext, Output, RegisteredType, TaskConfig, TaskConfigTpl,
    TaskDef, TaskResult,
};
use envconfig::Envconfig;
use futures::Stream;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::util::Timeout;
use rdkafka::Message;
use rdkafka::{consumer::StreamConsumer, ClientConfig};

pub struct KafkaConsumer {
    consumer: Option<BaseConsumer>,
}

impl KafkaConsumer {
    fn new() -> Self {
        Self { consumer: None }
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

        let consumer: BaseConsumer = config.create().expect("Kafka consumer creation failed!");

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

/// A KafkaConsumer can output any discrete registered type, provided it is deserializable.
/// Note that this only means the consumer will *try* to deserialize Kafka messages into the
/// given type; if they aren't deserializable, only support deserialization from a particular
/// type (ex. Protobuf when the topic contains XML), then runtime errors will occur.
impl Output<OwnedMessage> for KafkaConsumer {
    const conn_name: &'static str = "deserialized_message";
}

impl HasOutputs for KafkaConsumer {
    fn get_outputs(&self) -> std::collections::HashMap<String, Vec<std::any::TypeId>> {
        let mut hm = HashMap::new();
        hm.insert(
            "deserialized_message".to_string(),
            vec![TypeId::of::<RegisteredType>()],
        );
        hm
    }
}

impl Daemon for KafkaConsumer {
    fn run(&mut self, ctx: &MuetlContext) {
        match self
            .consumer
            .as_ref()
            .unwrap()
            .poll(Timeout::After(Duration::from_millis(1000)))
        {
            None => {}
            Some(Err(e)) => {
                println!("Error consuming from kafka: {}", e.to_string());
            }
            Some(Ok(m)) => {
                ctx.results.send(Event::new(
                    "".to_string(),
                    "deserialized_message".to_string(),
                    HashMap::new(),
                    Arc::new(m.detach()),
                ));
            }
        }
    }
}
