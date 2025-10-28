pub mod daemon;
pub mod node;
pub mod sink;
pub mod source;
pub mod task;

use std::{any::TypeId, collections::HashMap, sync::Arc};

use crate::messages::{event::Event, Status};

/// A TaskDef represents any process that is executed by muetl.
pub trait TaskDef {
    /// TaskDefs may implement this to return the list of configuration options
    /// they may expect. By default, no configuration options are processed.
    fn task_config_tpl(&self) -> Option<TaskConfigTpl> {
        None
    }
    fn init(&mut self, config: TaskConfig) -> Result<(), String>;
    fn deinit(self) -> Result<(), String>;
}

pub trait HasOutputs: TaskDef {
    fn get_outputs(&self) -> HashMap<String, Vec<TypeId>>;

    /// After the underlying event handling has returned a set of Events, validate that each one's
    /// conn_name matches the data type. If any Events do not match the expected conn_name - type
    /// declared by the Node's implementation of Output<T>, then an error is returned.
    fn validate_output(&self, events: &Vec<Event>) -> Result<(), String> {
        let outputs = self.get_outputs();
        for event in events {
            if let Some(exp_types) = outputs.get(&event.conn_name) {
                if !exp_types.contains(&event.get_data().type_id()) {
                    return Err(
                        format!("output Event for conn named '{}' has invalid type {:?} (expected one of {:?})",
                            event.conn_name,
                            event.get_data().type_id(),
                            exp_types));
                }
            }
        }
        Ok(())
    }
}

/// A MuetlContext contains information about the runtime environment that a TaskDef might need
/// when running.
pub struct MuetlContext {
    /// For TaskDefs that produce outputs, the current mapping of output conn_names to the
    /// type IDs that have been requested by subscribers. Note that each mapping has already
    /// been validated against the full list of supported output types for the given
    /// conn_name; this represents the list of types that are expected.
    ///
    /// Producers should use this to limit how much work they do when producing messages to
    /// outputs that support multiple types. For example, take a Source that has an output
    /// named "output_1" with possible output types [String, i32, bool]. At runtime, two
    /// Sinks subscribe to the "output_1" connection:
    /// - Sink #1 requests types `[i32, bool]`.
    /// - Sink #2 requests type `[String]`.
    ///
    /// Prior to starting the TaskDefs, muetl will negotiate the acceptable types such that
    /// all subscribers agree on the type they'll receive. For the example above, the list
    /// of current subscribers would look like:
    /// ```
    /// {
    ///   output_1: [i32, String]
    /// }
    /// ```
    /// The Source should then use that information, stored in `current_subscribers`, to
    /// only produce the types that are needed for `output_1`, instead of naively producing
    /// messages of all its supported types ([String, i32, bool]).
    ///
    /// Note that `current_subscribers` may change between calls to a producer's run function
    /// as new Tasks subscribe or unsubscribe.
    pub current_subscribers: HashMap<String, Vec<TypeId>>,
}

pub trait HasInputs: TaskDef {
    fn get_inputs(&self) -> HashMap<String, Vec<TypeId>>;
}

/// Users should implement Input<Some Type> to declare that their Node or Sink is
/// capable of processing that type of message on the given connection named
/// conn_name.
pub trait Input<T> {
    const conn_name: &'static str;
    fn handle(&mut self, ctx: &MuetlContext, input: &T) -> TaskResult;
}

/// Users should implement Output<Some Type> to declare that their Daemon,
/// Source, or Node is capable of producing that type of message on the
/// given connection named conn_name. Note that the TaskResult produced
/// by any Input<T>::handle() is checked at runtime to ensure that
/// each Event's conn_name and type match exactly one Output<T>
/// impl for the TaskDef.
pub trait Output<T> {
    const conn_name: &'static str;
}

/// TaskDefs can implement Output<RegisteredType> to allow them to output any dynamic type
/// known by the type registry at runtime; this helps cut down on repeated `impl Output<T>`
/// statements for generic TaskDefs.
/// TODO: Do we actually need this? Maybe not yet?
pub struct RegisteredType {
    type_id: TypeId,
}

/// Any handler function should return this result, which breaks down into:
/// 1. Err(message) if something went wrong while handling the input.
/// 2. Ok((events, status)) otherwise. `events` gets type checked and produced
/// as output messages. If `status` is present, then it is separately produced
/// to the Monitoring service. Use `status` to report state changes like
/// % complete, active, or finished.
pub type TaskResult = Result<(Vec<Event>, Option<Status>), String>;

/// A particular configuration property that a TaskDef looks for. At runtime, the template
/// will be processed, and a `TaskConfig` will be returned.
pub struct TaskConfigTpl {
    /// Single configuration fields. `name` must be matched exactly for this template to be processed.
    pub fields: Vec<ConfigField>,
    /// If true, any fields that are found that don't match this template will be rejected, and config
    /// processing will return an error.
    pub disallow_unknown_fields: bool,
}
pub struct ConfigField {
    pub name: String,
    pub required: bool,
    pub default_value: Option<TaskConfigValue>,
}
impl ConfigField {
    pub fn required(name: &str) -> ConfigField {
        ConfigField {
            name: name.to_string(),
            required: true,
            default_value: None,
        }
    }
    pub fn optional_with_default(name: &str, default_value: TaskConfigValue) -> ConfigField {
        ConfigField {
            name: name.to_string(),
            required: false,
            default_value: Some(default_value),
        }
    }
    pub fn optional(name: &str) -> ConfigField {
        ConfigField {
            name: name.to_string(),
            required: false,
            default_value: None,
        }
    }
}

/// The runtime configuration passed to a TaskDef that represents a processed version
/// of the TaskConfigTpl it exposes.
pub type TaskConfig = HashMap<String, TaskConfigValue>;
// TODO: make TaskConfig a struct so we can override get() with a better version, to prevent having to double-unwrap()

pub enum TaskConfigValue {
    Str(String),
    Int(i64),
    Uint(u64),
    Bool(bool),
    Arr(Vec<TaskConfigValue>),
    Map(HashMap<String, TaskConfigValue>),
}

impl TaskConfigValue {
    pub fn try_into_str(&self) -> Option<String> {
        match &self {
            TaskConfigValue::Str(s) => Some(s.clone()),
            _ => None,
        }
    }
    // TODO: add other methods
}
