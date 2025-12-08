//! Custom tracing Layer for per-task log routing.

use std::{collections::HashMap, fmt, sync::Arc};

use tracing::{
    field::{Field, Visit},
    span, Event, Subscriber,
};
use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer};

use super::{entry::LogEntry, registry::TaskLogRegistry};

/// Keys for span fields that identify task context.
const TRACE_ID_FIELD: &str = "trace_id";
const TASK_ID_FIELD: &str = "task_id";
const TASK_NAME_FIELD: &str = "task_name";

/// Custom tracing layer that routes log events to per-task broadcast channels.
///
/// This layer intercepts all tracing events, extracts task context from the
/// current span hierarchy, and publishes log entries to the appropriate
/// task's broadcast channel via the TaskLogRegistry.
pub struct TaskLogLayer {
    registry: Arc<TaskLogRegistry>,
}

impl TaskLogLayer {
    pub fn new(registry: Arc<TaskLogRegistry>) -> Self {
        Self { registry }
    }
}

/// Visitor that extracts the message and fields from a tracing event.
struct EventVisitor {
    message: String,
    fields: HashMap<String, String>,
}

impl EventVisitor {
    fn new() -> Self {
        Self {
            message: String::new(),
            fields: HashMap::new(),
        }
    }
}

impl Visit for EventVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{:?}", value);
        } else {
            self.fields
                .insert(field.name().to_string(), format!("{:?}", value));
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }
}

/// Storage for task context extracted from spans.
#[derive(Clone, Default)]
struct TaskContext {
    trace_id: Option<u64>,
    task_id: Option<u64>,
    task_name: Option<String>,
}

/// Visitor that extracts task context fields from a span.
struct SpanVisitor {
    context: TaskContext,
}

impl SpanVisitor {
    fn new() -> Self {
        Self {
            context: TaskContext::default(),
        }
    }
}

impl Visit for SpanVisitor {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            TRACE_ID_FIELD => self.context.trace_id = Some(value),
            TASK_ID_FIELD => self.context.task_id = Some(value),
            _ => {}
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        match field.name() {
            TRACE_ID_FIELD => self.context.trace_id = Some(value as u64),
            TASK_ID_FIELD => self.context.task_id = Some(value as u64),
            _ => {}
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == TASK_NAME_FIELD {
            self.context.task_name = Some(value.to_string());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == TASK_NAME_FIELD {
            self.context.task_name = Some(format!("{:?}", value));
        }
    }
}

impl<S> Layer<S> for TaskLogLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        // Extract task context from span attributes and store it
        let mut visitor = SpanVisitor::new();
        attrs.record(&mut visitor);

        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(visitor.context);
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        // Find task context by walking up the span hierarchy
        let mut task_context = TaskContext::default();

        if let Some(scope) = ctx.event_scope(event) {
            for span in scope {
                if let Some(stored_ctx) = span.extensions().get::<TaskContext>() {
                    // Merge context, preferring values from inner spans
                    if task_context.trace_id.is_none() {
                        task_context.trace_id = stored_ctx.trace_id;
                    }
                    if task_context.task_id.is_none() {
                        task_context.task_id = stored_ctx.task_id;
                    }
                    if task_context.task_name.is_none() {
                        task_context.task_name = stored_ctx.task_name.clone();
                    }
                }
            }
        }

        // Only route events that have task context
        let Some(task_id) = task_context.task_id else {
            return;
        };

        // Extract event message and fields
        let mut visitor = EventVisitor::new();
        event.record(&mut visitor);

        let entry = LogEntry::new(
            *event.metadata().level(),
            task_context.trace_id.unwrap_or(0),
            task_id,
            task_context.task_name.unwrap_or_default(),
            visitor.message,
        )
        .with_fields(visitor.fields);

        // Publish to the task's broadcast channel
        self.registry.publish(task_id, entry);
    }
}
