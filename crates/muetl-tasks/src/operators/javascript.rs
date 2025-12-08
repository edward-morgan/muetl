//! JavaScript operator - executes user-provided JS snippets to transform JSON data.
//!
//! This operator accepts `serde_json::Value` inputs or common primitive types
//! (String, integers, floats, bools). Non-JSON inputs are automatically converted
//! to JSON before being passed to the script.

use std::{any::TypeId, cell::RefCell, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use muetl::{
    impl_config_template,
    messages::event::Event,
    registry::{SelfDescribing, TaskDefInfo, TaskInfo},
    task_defs::{operator::Operator, ConfigTemplate, MuetlContext, TaskConfig, TaskDef},
};
use rquickjs::{Context, Ctx, Runtime};
use serde_json::Value as JsonValue;

thread_local! {
    /// Thread-local QuickJS runtime for efficient reuse.
    static JS_RUNTIME: RefCell<Option<Runtime>> = const { RefCell::new(None) };
}

/// Get or create the thread-local JS runtime and execute a function with it.
fn with_js_context<T, F>(f: F) -> Result<T, String>
where
    F: for<'js> FnOnce(Ctx<'js>) -> Result<T, String>,
{
    JS_RUNTIME.with(|runtime_cell| {
        let mut runtime_opt = runtime_cell.borrow_mut();
        if runtime_opt.is_none() {
            *runtime_opt =
                Some(Runtime::new().map_err(|e| format!("failed to create JS runtime: {}", e))?);
        }
        let runtime = runtime_opt.as_ref().unwrap();
        let ctx =
            Context::full(runtime).map_err(|e| format!("failed to create JS context: {}", e))?;
        ctx.with(|ctx| f(ctx))
    })
}

/// JavaScript operator that transforms JSON input using user-provided JS code.
///
/// Configuration:
/// - `script` (required): JavaScript code that transforms the input. The script receives
///   the input as a global variable `input` (a JSON object) and should return the
///   transformed output (also a JSON object).
///
/// The script is executed for each incoming event. The input event's data must be
/// a `serde_json::Value`. The script should return a JSON-serializable value.
///
/// Example script:
/// ```javascript
/// // Double all numeric values in the input
/// let output = {};
/// for (let key in input) {
///     if (typeof input[key] === 'number') {
///         output[key] = input[key] * 2;
///     } else {
///         output[key] = input[key];
///     }
/// }
/// output;  // Return the result (last expression is returned)
/// ```
pub struct JavaScript {
    script: String,
}

impl JavaScript {
    pub fn new(config: &TaskConfig) -> Result<Box<dyn Operator>, String> {
        let script = config.require_str("script").to_string();

        // Validate the script by trying to evaluate it with dummy input
        let script_clone = script.clone();
        with_js_context(|ctx| {
            // Set up a dummy input and try to execute
            ctx.eval::<(), _>("var input = {};")
                .map_err(|e| format!("failed to set up input variable: {}", e))?;
            ctx.eval::<String, _>(format!(
                "JSON.stringify((function() {{ {} }})())",
                script_clone
            ))
            .map_err(|e| format!("invalid JavaScript: {}", e))
        })?;

        Ok(Box::new(JavaScript { script }))
    }

    fn execute(&self, input: &JsonValue) -> Result<JsonValue, String> {
        let script = &self.script;
        with_js_context(|ctx| {
            // Convert input JSON to JS string and parse it in JS context
            let input_json = serde_json::to_string(input).map_err(|e| e.to_string())?;
            let parse_input = format!("var input = {};", input_json);

            ctx.eval::<(), _>(parse_input)
                .map_err(|e| format!("failed to set input: {}", e))?;

            // Execute the user's script wrapped to capture the result
            let result: String = ctx
                .eval(format!("JSON.stringify((function() {{ {} }})())", script))
                .map_err(|e| format!("script execution failed: {}", e))?;

            // Parse the JSON result
            serde_json::from_str(&result).map_err(|e| format!("failed to parse result: {}", e))
        })
    }
}

impl TaskDef for JavaScript {}

impl SelfDescribing for JavaScript {
    fn task_info() -> TaskInfo {
        let mut inputs = HashMap::new();
        // JavaScript accepts JsonValue and common primitive types
        inputs.insert(
            "input".to_string(),
            vec![
                TypeId::of::<JsonValue>(),
                TypeId::of::<String>(),
                TypeId::of::<i64>(),
                TypeId::of::<i32>(),
                TypeId::of::<u64>(),
                TypeId::of::<u32>(),
                TypeId::of::<f64>(),
                TypeId::of::<bool>(),
                TypeId::of::<()>(),
            ],
        );

        let mut outputs = HashMap::new();
        outputs.insert("output".to_string(), vec![TypeId::of::<JsonValue>()]);

        TaskInfo {
            task_id: "javascript".to_string(),
            config_tpl: <Self as ConfigTemplate>::config_template(),
            info: TaskDefInfo::OperatorDef {
                inputs,
                outputs,
                build_operator: Self::new,
            },
        }
    }
}

#[async_trait]
impl Operator for JavaScript {
    async fn handle_event_for_conn(
        &mut self,
        ctx: &MuetlContext,
        conn_name: &String,
        ev: Arc<Event>,
    ) {
        if conn_name != "input" {
            return;
        }

        let data = ev.get_data();

        // Try to get input as JSON, either directly or by converting common types
        let input: JsonValue = if let Some(json) = data.downcast_ref::<JsonValue>() {
            // Already a JsonValue, use directly
            json.clone()
        } else if let Some(v) = try_serialize_common_types(&data) {
            // Try common serializable types
            v
        } else {
            tracing::error!(
                "JavaScript operator received non-serializable input. \
                 Input must be JsonValue or a common primitive type."
            );
            return;
        };

        match self.execute(&input) {
            Ok(output) => {
                let headers = ctx.event_headers.clone().unwrap_or_default();
                ctx.results
                    .send(Event::new(
                        ctx.event_name.clone().unwrap_or_default(),
                        "output".to_string(),
                        headers,
                        Arc::new(output),
                    ))
                    .await
                    .unwrap();
            }
            Err(e) => {
                tracing::error!(error = %e, "JavaScript execution failed");
            }
        }
    }
}

/// Try to convert common types to JSON.
fn try_serialize_common_types(data: &Arc<dyn std::any::Any + Send + Sync>) -> Option<JsonValue> {
    // Try primitive types
    if let Some(v) = data.downcast_ref::<String>() {
        return Some(JsonValue::String(v.clone()));
    }
    if let Some(v) = data.downcast_ref::<i64>() {
        return Some(JsonValue::Number((*v).into()));
    }
    if let Some(v) = data.downcast_ref::<i32>() {
        return Some(JsonValue::Number((*v as i64).into()));
    }
    if let Some(v) = data.downcast_ref::<u64>() {
        return Some(JsonValue::Number((*v).into()));
    }
    if let Some(v) = data.downcast_ref::<u32>() {
        return Some(JsonValue::Number((*v as u64).into()));
    }
    if let Some(v) = data.downcast_ref::<f64>() {
        return serde_json::Number::from_f64(*v).map(JsonValue::Number);
    }
    if let Some(v) = data.downcast_ref::<bool>() {
        return Some(JsonValue::Bool(*v));
    }
    // Unit type (e.g., from S3ListSource which passes metadata via headers)
    if data.downcast_ref::<()>().is_some() {
        return Some(JsonValue::Null);
    }

    None
}

impl_config_template!(
    JavaScript,
    script: Str!,
);
