pub mod daemon;
pub mod node;
pub mod sink;
pub mod source;

use std::{
    any::{Any, TypeId},
    collections::HashMap,
    fmt::Debug,
    sync::Arc,
};

use daemon::Daemon;
use kameo::actor::ActorRef;
use sink::Sink;
use tokio::sync::mpsc::Sender;

use crate::{
    messages::{event::Event, Status},
    runtime::{
        connection::{IncomingConnections, OutgoingConnections},
        sink_actor::SinkActor,
    },
};

/// An OutputType represents the type of an output connection in a TaskDef.
///
/// The specific type may be a single type, a one-of sum type or an
/// any-of product type.
///
/// OutputTypes are used when negotiating the types of subscribers to this
/// TaskDef. In the simplest case (a Singleton), subscribers can only ask
/// to subscribe to that one type. The TaskDef is responsible for producing
/// Events with that single type.
///
/// For a Sum type, subscribers can ask for one of the types that are
/// available; however, they must *all* agree on which type that is for
/// the subscriptions to be valid. Like for Singleton OutputTypes, the
/// TaskDef is responsible for producing only Events with the single
/// 'negotiated' type, whatever that is at runtime.
///
/// For a Product type, subscribers can ask for any one of the available
/// options, where multiple types may be produced from a single output.
/// While the most flexible, this requires the TaskDef to produce an
/// Event of *each* 'negotiated' type as output.
pub enum OutputType {
    /// The output may only be of this single type.
    Singleton(TypeId),
    /// The output may be exactly one of the types listed.
    Sum(Vec<TypeId>),
    /// The output may be any one of the types listed.
    Product(Vec<TypeId>),
}
impl OutputType {
    /// Check the given type against the OutputType, which differs
    /// based on whether this is a sum or product type.
    pub fn check_type(&self, against: TypeId) -> bool {
        match self {
            Self::Singleton(tpe) => *tpe == against,
            Self::Sum(types) => types.contains(&against),
            Self::Product(types) => types.contains(&against),
        }
    }

    pub fn check_types(&self, against: Vec<TypeId>) -> bool {
        if against.len() == 0 {
            return false;
        }

        match self {
            Self::Singleton(tpe) => against.iter().all(|&a| a == *tpe),
            Self::Sum(types) => {
                let to_match = against[0].type_id();
                // Check that all types in against match, and that that single type
                // exists in the sum type.
                types.contains(&to_match) && against.iter().skip(1).all(|&a| a == to_match)
            }
            Self::Product(types) => against.iter().all(|a| types.contains(a)),
        }
    }

    /// Construct a new OutputType that can only be a single type.
    pub fn singleton(of: TypeId) -> Self {
        Self::Singleton(of)
    }

    pub fn singleton_of<T: 'static>() -> Self {
        Self::Singleton(TypeId::of::<T>())
    }

    /// Construct a new OutputType that can be a single type among a
    /// set of choices.
    pub fn one_of(choices: Vec<TypeId>) -> Self {
        Self::Sum(choices)
    }

    /// Construct a new OutputType that can be any one of types in a
    /// set of choices.
    pub fn any_of(choices: Vec<TypeId>) -> Self {
        Self::Product(choices)
    }
}
impl Debug for OutputType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Singleton(tpe) => f.debug_set().entry(tpe).finish(),
            Self::Product(types) => f.debug_set().entries(types).finish(),
            Self::Sum(types) => f.debug_set().entries(types).finish(),
        }
    }
}

/// A TaskDef represents any process that is executed by muetl.
pub trait TaskDef {
    /// TaskDefs may implement this to return the list of configuration options
    /// they may expect. By default, no configuration options are processed.
    fn task_config_tpl(&self) -> Option<TaskConfigTpl> {
        None
    }
    fn deinit(&mut self) -> Result<(), String> {
        Ok(())
    }
}

pub trait HasOutputs: TaskDef {
    fn get_outputs(&self) -> HashMap<String, OutputType>;
}

pub trait HasInputs: TaskDef {
    fn get_inputs(&self) -> HashMap<String, TypeId>;
}

/// Users should implement Input<Some Type> to declare that their Node is
/// capable of processing that type of message on the given connection named
/// conn_name.
pub trait Input<T> {
    const conn_name: &'static str;
    async fn handle(&mut self, ctx: &MuetlContext, input: &T);
}

/// Users should implement SinkInput<Some Type> to declare that their Sink is
/// capable of processing that type of message on the given connection named
/// conn_name.
pub trait SinkInput<T> {
    const conn_name: &'static str;
    async fn handle(&mut self, ctx: &MuetlSinkContext, input: &T);
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
//
// TODO: the usage semantics here aren't very good - is there a better way to do things? Can TaskDefs define their own configuration structs? Is there a crate that does this?

pub enum TaskConfigValue {
    Str(String),
    Int(i64),
    Uint(u64),
    Bool(bool),
    Arr(Vec<TaskConfigValue>),
    Map(HashMap<String, TaskConfigValue>),
}

impl TryFrom<&TaskConfigValue> for u64 {
    type Error = String;
    fn try_from(value: &TaskConfigValue) -> Result<Self, Self::Error> {
        if let TaskConfigValue::Uint(u) = value {
            Ok(*u)
        } else {
            Err(format!("cannot turn value into u64"))
        }
    }
}
impl TryFrom<&TaskConfigValue> for String {
    type Error = String;
    fn try_from(value: &TaskConfigValue) -> Result<Self, Self::Error> {
        if let TaskConfigValue::Str(s) = value {
            Ok(s.clone())
        } else {
            Err(format!("cannot turn value into String"))
        }
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
    /// The channel to send results back on, in the form of Events.
    pub results: Sender<Event>,
    /// The channel to send statuses back on.
    pub status: Sender<Status>,
}

/// A MuetlSinkContext contains the context a Sink should use when running. It's different from a `MuetlContext` in that it has
/// no output results channel, nor does it have a map of current subscribers.
pub struct MuetlSinkContext {
    /// The channel to send statuses back on.
    pub status: Sender<Status>,
}
