use std::{collections::HashMap, sync::Arc};

use muetl::{
    impl_operator_handler, impl_sink_handler, impl_source_handler,
    messages::{event::Event, Status},
    task_defs::{
        operator::Operator, sink::Sink, source::Source, ConfigTemplate, Input, MuetlContext,
        MuetlSinkContext, SinkInput, TaskConfig, TaskDef,
    },
};

struct FlushOnShutdown {
    flushed: bool,
    handled: usize,
}

impl FlushOnShutdown {
    pub async fn new(_config: TaskConfig) -> Result<Box<dyn Operator>, String> {
        Ok(Box::new(Self {
            flushed: false,
            handled: 0,
        }))
    }
}

impl TaskDef for FlushOnShutdown {}
impl ConfigTemplate for FlushOnShutdown {}

impl Input<i64> for FlushOnShutdown {
    const conn_name: &'static str = "input";

    async fn handle(&mut self, _ctx: &MuetlContext, _input: &i64) {
        self.handled += 1;
    }
}

impl_operator_handler!(
    FlushOnShutdown,
    task_id = "flush_on_shutdown",
    inputs("input" => i64),
    outputs("output" => i64),
    prepare_shutdown(this, ctx) {
        this.flushed = true;
        ctx.status.send(Status::Finished).await.unwrap();
    }
);

#[tokio::test]
async fn operator_handler_macro_supports_prepare_shutdown_hook() {
    let (results, _results_rx) = tokio::sync::mpsc::channel::<Event>(1);
    let (status, mut status_rx) = tokio::sync::mpsc::channel::<Status>(1);
    let ctx = MuetlContext {
        current_subscribers: HashMap::new(),
        results,
        status,
        event_name: None,
        event_headers: HashMap::new(),
    };

    let mut operator = FlushOnShutdown {
        flushed: false,
        handled: 0,
    };

    operator
        .handle_event_for_conn(
            &ctx,
            &"input".to_string(),
            Arc::new(Event::new(
                "event".to_string(),
                "input".to_string(),
                HashMap::new(),
                Arc::new(42_i64),
            )),
        )
        .await;
    operator.prepare_shutdown(&ctx).await;

    assert_eq!(operator.handled, 1);
    assert!(operator.flushed);
    assert_eq!(status_rx.recv().await, Some(Status::Finished));
}

struct FlushSink {
    flushed: bool,
    handled: usize,
}

impl FlushSink {
    pub async fn new(_config: TaskConfig) -> Result<Box<dyn Sink>, String> {
        Ok(Box::new(Self {
            flushed: false,
            handled: 0,
        }))
    }
}

impl TaskDef for FlushSink {}
impl ConfigTemplate for FlushSink {}

impl SinkInput<i64> for FlushSink {
    const conn_name: &'static str = "input";

    async fn handle(&mut self, _ctx: &MuetlSinkContext, _input: &i64) {
        self.handled += 1;
    }
}

impl_sink_handler!(
    FlushSink,
    task_id = "flush_sink",
    "input" => i64,
    prepare_shutdown(this, ctx) {
        this.flushed = true;
        ctx.status.send(Status::Finished).await.unwrap();
    }
);

#[tokio::test]
async fn sink_handler_macro_supports_prepare_shutdown_hook() {
    let (status, mut status_rx) = tokio::sync::mpsc::channel::<Status>(1);
    let ctx = MuetlSinkContext {
        status,
        event_name: "event".to_string(),
        event_headers: HashMap::new(),
    };

    let mut sink = FlushSink {
        flushed: false,
        handled: 0,
    };

    sink.handle_event_for_conn(
        &ctx,
        &"input".to_string(),
        Arc::new(Event::new(
            "event".to_string(),
            "input".to_string(),
            HashMap::new(),
            Arc::new(42_i64),
        )),
    )
    .await;
    sink.prepare_shutdown(&ctx).await;

    assert_eq!(sink.handled, 1);
    assert!(sink.flushed);
    assert_eq!(status_rx.recv().await, Some(Status::Finished));
}

struct ShutdownSource {
    flushed: bool,
    ran: bool,
}

impl ShutdownSource {
    pub async fn new(_config: TaskConfig) -> Result<Box<dyn Source>, String> {
        Ok(Box::new(Self {
            flushed: false,
            ran: false,
        }))
    }
}

impl TaskDef for ShutdownSource {}
impl ConfigTemplate for ShutdownSource {}

impl_source_handler!(
    ShutdownSource,
    task_id = "shutdown_source",
    outputs("output" => i64),
    run(this, ctx) {
        this.ran = true;
        ctx.results
            .send(Event::new(
                "event".to_string(),
                "output".to_string(),
                HashMap::new(),
                Arc::new(42_i64),
            ))
            .await
            .unwrap();
    },
    prepare_shutdown(this, ctx) {
        this.flushed = true;
        ctx.status.send(Status::Finished).await.unwrap();
    }
);

#[tokio::test]
async fn source_handler_macro_supports_prepare_shutdown_hook() {
    let (results, mut results_rx) = tokio::sync::mpsc::channel::<Event>(1);
    let (status, mut status_rx) = tokio::sync::mpsc::channel::<Status>(1);
    let ctx = MuetlContext {
        current_subscribers: HashMap::new(),
        results,
        status,
        event_name: None,
        event_headers: HashMap::new(),
    };

    let mut source = ShutdownSource {
        flushed: false,
        ran: false,
    };

    source.run(&ctx).await;
    source.prepare_shutdown(&ctx).await;

    assert!(source.ran);
    assert!(source.flushed);
    assert_eq!(results_rx.recv().await.unwrap().conn_name, "output");
    assert_eq!(status_rx.recv().await, Some(Status::Finished));
}
