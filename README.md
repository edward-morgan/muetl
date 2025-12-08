# muetl

An actor-driven ETL framework in Rust for building modular, type-safe data pipelines.

## Overview

muetl models data pipelines as directed acyclic graphs (DAGs) of three task types:

- **Sources** - Produce events (no inputs)
- **Operators** - Transform events (inputs → outputs)
- **Sinks** - Consume events (no outputs)

Tasks communicate through typed **Events** carrying `Arc<dyn Any + Send + Sync>` payloads. Connections between tasks are validated at load time to ensure type compatibility.

## Architecture

```
┌─────────┐     ┌──────────┐     ┌──────┐
│ Sources │ ──► │ Operators│ ──► │ Sinks│
└─────────┘     └──────────┘     └──────┘
     │               │               │
     └───────────────┴───────────────┘
                     │
              Events (typed)
```

**Core components:**

| Component | Purpose |
|-----------|---------|
| `Event` | Message carrying typed data + metadata between tasks |
| `Flow` | Validated DAG of nodes and type-negotiated edges |
| `Registry` | Maps task IDs to definitions and factory functions |
| `Root` | Runtime actor that spawns tasks and wires connections |

## Implementing Tasks

### Source

Sources produce events. The runtime calls `run()` repeatedly until `Status::Finished` is sent.

```rust
use muetl::prelude::*;

pub struct Counter { current: u64, limit: u64 }

impl Counter {
    pub fn new(config: &TaskConfig) -> Result<Box<dyn Source>, String> {
        Ok(Box::new(Counter {
            current: 0,
            limit: config.get_u64("limit").unwrap_or(10),
        }))
    }
}

impl TaskDef for Counter {}

impl Output<u64> for Counter {
    const conn_name: &'static str = "output";
}

#[async_trait]
impl Source for Counter {
    async fn run(&mut self, ctx: &MuetlContext) {
        if self.current >= self.limit {
            ctx.status.send(Status::Finished).await.unwrap();
        } else {
            ctx.results.send(Event::new(
                format!("count-{}", self.current),
                "output".to_string(),
                HashMap::new(),
                Arc::new(self.current),
            )).await.unwrap();
            self.current += 1;
        }
    }
}
```

### Operator

Operators transform events. Use the `impl_operator_handler!` macro for typed dispatch.

```rust
use muetl::prelude::*;

pub struct Double;

impl TaskDef for Double {}

impl Input<u64> for Double {
    const conn_name: &'static str = "input";
    async fn handle(&mut self, ctx: &MuetlContext, value: &u64) {
        ctx.results.send(Event::new(
            ctx.event_name.clone().unwrap_or_default(),
            "output".to_string(),
            ctx.event_headers.clone().unwrap_or_default(),
            Arc::new(value * 2),
        )).await.unwrap();
    }
}

impl Output<u64> for Double {
    const conn_name: &'static str = "output";
}

impl_operator_handler!(Double, "input" => u64);
```

### Sink

Sinks consume events. Use `impl_sink_handler!` for typed dispatch.

```rust
use muetl::prelude::*;

pub struct Logger;

impl TaskDef for Logger {}

impl SinkInput<u64> for Logger {
    const conn_name: &'static str = "input";
    async fn handle(&mut self, ctx: &MuetlSinkContext, value: &u64) {
        tracing::info!(event = %ctx.event_name, value = %value, "received");
    }
}

impl_sink_handler!(Logger, "input" => u64);
```

## Configuration

Tasks declare configuration schemas via `TaskConfigTpl`:

```rust
impl TaskDef for MyTask {
    fn task_config_tpl(&self) -> Option<TaskConfigTpl> {
        Some(TaskConfigTpl {
            fields: vec![
                ConfigField::required("path", ConfigType::Str),
                ConfigField::with_default("batch_size", ConfigValue::Uint(100)),
                ConfigField::optional("filter", ConfigType::Str),
            ],
            disallow_unknown_fields: true,
        })
    }
}
```

Access values in the factory function:

```rust
pub fn new(config: &TaskConfig) -> Result<Box<dyn Source>, String> {
    let path = config.require_str("path");
    let batch_size = config.get_u64("batch_size").unwrap_or(100);
    let filter = config.get_str("filter");
    // ...
}
```

## Using as a Library

```rust
use muetl::{flow::*, registry::*, runtime::Root};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create registry and register tasks
    let mut registry = Registry::new();

    registry.add_def(TaskInfo {
        task_id: "counter".to_string(),
        config_tpl: None,
        info: TaskDefInfo::SourceDef {
            outputs: [("output".into(), vec![TypeId::of::<u64>()])].into(),
            build_source: Counter::new,
        },
    });

    registry.add_def(TaskInfo {
        task_id: "logger".to_string(),
        config_tpl: None,
        info: TaskDefInfo::SinkDef {
            inputs: [("input".into(), vec![TypeId::of::<u64>()])].into(),
            build_sink: Logger::new,
        },
    });

    // 2. Define flow
    let raw_flow = RawFlow {
        nodes: vec![
            Node::new("src", "counter", HashMap::new()),
            Node::new("sink", "logger", HashMap::new()),
        ],
        edges: vec![
            Edge::new(
                NodeRef::new("src", "output"),
                NodeRef::new("sink", "input"),
            ),
        ],
    };

    // 3. Parse and validate
    let flow = Flow::parse_from(raw_flow, Arc::new(registry))?;

    // 4. Run
    let monitor = PubSub::spawn(PubSub::new(DeliveryStrategy::Guaranteed));
    let root = Root::new(flow, monitor);
    let root_ref = Root::spawn(root);
    root_ref.wait_for_shutdown().await;

    Ok(())
}
```
