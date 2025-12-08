use std::time::Duration;

use rdkafka::consumer::BaseConsumer;
use rdkafka::message::OwnedMessage;
use rdkafka::util::Timeout;

use muetl::prelude::*;

pub struct KafkaConsumer {
    consumer: Option<BaseConsumer>,
}

impl KafkaConsumer {
    fn new() -> Self {
        Self { consumer: None }
    }
}

impl TaskDef for KafkaConsumer {

    fn deinit(&mut self) -> Result<(), String> {
        Ok(())
    }
}

impl Output<OwnedMessage> for KafkaConsumer {
    const conn_name: &'static str = "deserialized_message";
}

#[async_trait]
impl Source for KafkaConsumer {
    async fn run(&mut self, ctx: &MuetlContext) {
        match self
            .consumer
            .as_ref()
            .unwrap()
            .poll(Timeout::After(Duration::from_millis(1000)))
        {
            None => {}
            Some(Err(e)) => {
                tracing::error!(error = %e, "Error consuming from Kafka");
            }
            Some(Ok(m)) => {
                ctx.results
                    .send(Event::new(
                        "".to_string(),
                        "deserialized_message".to_string(),
                        HashMap::new(),
                        Arc::new(m.detach()),
                    ))
                    .await
                    .unwrap();
            }
        }
    }
}
