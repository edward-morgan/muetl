pub mod config;
pub mod operator;
pub mod sink;
pub mod source;

pub use config::*;

use std::{any::TypeId, collections::HashMap};

use tokio::sync::mpsc::Sender;

use crate::messages::{event::Event, Status};

/// Generates the `handle_event_for_conn` implementation for an Operator.
///
/// This macro reduces boilerplate by generating the connection name matching
/// and type downcasting logic. It calls the `Input<T>::handle` method for each
/// type registered with the macro.
///
/// # Multiple Types Per Input
///
/// You can specify multiple types for the same input connection using bracket syntax.
/// The macro will try to downcast to each type in order, calling the first handler that matches:
///
/// ```ignore
/// impl_operator_handler!(
///     MyOperator,
///     task_id = "my_op",
///     inputs(
///         "data" => [serde_json::Value, String],  // Try these types in order
///     ),
///     outputs("output" => String)
/// );
/// ```
///
/// # Example
///
/// ```ignore
/// struct Adder { addend: i64 }
///
/// impl Input<i64> for Adder {
///     const conn_name: &'static str = "input";
///     async fn handle(&mut self, ctx: &MuetlContext, value: &i64) {
///         let result = value + self.addend;
///         ctx.results.send(Event::new(
///             format!("{}-plus-{}", ctx.event_name.as_ref().unwrap(), self.addend),
///             "output".to_string(),
///             ctx.event_headers.clone().unwrap_or_default(),
///             Arc::new(result),
///         )).await.unwrap();
///     }
/// }
///
/// impl_operator_handler!(Adder, "input" => i64);
/// ```
///
/// With the new self-describing registration system, use:
/// ```ignore
/// impl_operator_handler!(Adder, task_id = "adder", inputs("input" => i64), outputs("output" => i64));
/// ```
#[macro_export]
macro_rules! impl_operator_handler {
    // New pattern with task_id for self-describing registration
    ($ty:ty, task_id = $task_id:literal, inputs($($inputs:tt)*), outputs($($outputs:tt)*) $(,)?) => {
        // Parse and expand inputs/outputs, then delegate to implementation
        impl_operator_handler!(@expand_and_impl $ty, $task_id, inputs($($inputs)*), outputs($($outputs)*));
    };

    // Expand inputs and outputs, then generate implementations
    (@expand_and_impl $ty:ty, $task_id:literal, inputs($($inputs:tt)*), outputs($($outputs:tt)*)) => {
        // First, expand the inputs and outputs syntax
        impl_operator_handler!(@impl_with_expanded $ty, $task_id,
            inputs_expanded[], inputs_to_parse[$($inputs)*],
            outputs_expanded[], outputs_to_parse[$($outputs)*]
        );
    };

    // Parse inputs: handle "conn" => [Type1, Type2, ...]
    (@impl_with_expanded $ty:ty, $task_id:literal,
        inputs_expanded[$($in_conn_exp:literal => $input_ty_exp:ty),*],
        inputs_to_parse[$conn:literal => [$($types:ty),+ $(,)?] $(, $($rest_in:tt)*)?],
        outputs_expanded[$($out_exp:tt)*], outputs_to_parse[$($rest_out:tt)*]
    ) => {
        impl_operator_handler!(@impl_with_expanded $ty, $task_id,
            inputs_expanded[$($in_conn_exp => $input_ty_exp,)* $($conn => $types),*],
            inputs_to_parse[$($($rest_in)*)?],
            outputs_expanded[$($out_exp)*], outputs_to_parse[$($rest_out)*]
        );
    };

    // Parse inputs: handle "conn" => Type
    (@impl_with_expanded $ty:ty, $task_id:literal,
        inputs_expanded[$($in_conn_exp:literal => $input_ty_exp:ty),*],
        inputs_to_parse[$conn:literal => $single_type:ty $(, $($rest_in:tt)*)?],
        outputs_expanded[$($out_exp:tt)*], outputs_to_parse[$($rest_out:tt)*]
    ) => {
        impl_operator_handler!(@impl_with_expanded $ty, $task_id,
            inputs_expanded[$($in_conn_exp => $input_ty_exp,)* $conn => $single_type],
            inputs_to_parse[$($($rest_in)*)?],
            outputs_expanded[$($out_exp)*], outputs_to_parse[$($rest_out)*]
        );
    };

    // Done parsing inputs, now parse outputs: handle "conn" => [Type1, Type2, ...]
    (@impl_with_expanded $ty:ty, $task_id:literal,
        inputs_expanded[$($in_conn_exp:literal => $input_ty_exp:ty),*],
        inputs_to_parse[],
        outputs_expanded[$($out_conn_exp:literal => $output_ty_exp:ty),*],
        outputs_to_parse[$conn:literal => [$($types:ty),+ $(,)?] $(, $($rest:tt)*)?]
    ) => {
        impl_operator_handler!(@impl_with_expanded $ty, $task_id,
            inputs_expanded[$($in_conn_exp => $input_ty_exp),*],
            inputs_to_parse[],
            outputs_expanded[$($out_conn_exp => $output_ty_exp,)* $($conn => $types),*],
            outputs_to_parse[$($($rest)*)?]
        );
    };

    // Done parsing inputs, now parse outputs: handle "conn" => Type
    (@impl_with_expanded $ty:ty, $task_id:literal,
        inputs_expanded[$($in_conn_exp:literal => $input_ty_exp:ty),*],
        inputs_to_parse[],
        outputs_expanded[$($out_conn_exp:literal => $output_ty_exp:ty),*],
        outputs_to_parse[$conn:literal => $single_type:ty $(, $($rest:tt)*)?]
    ) => {
        impl_operator_handler!(@impl_with_expanded $ty, $task_id,
            inputs_expanded[$($in_conn_exp => $input_ty_exp),*],
            inputs_to_parse[],
            outputs_expanded[$($out_conn_exp => $output_ty_exp,)* $conn => $single_type],
            outputs_to_parse[$($($rest)*)?]
        );
    };

    // All parsing complete, generate the implementations
    (@impl_with_expanded $ty:ty, $task_id:literal,
        inputs_expanded[$($in_conn:literal => $input_ty:ty),*],
        inputs_to_parse[],
        outputs_expanded[$($out_conn:literal => $output_ty:ty),*],
        outputs_to_parse[]
    ) => {
        // Generate the Operator trait implementation
        impl_operator_handler!(@impl_trait $ty, $($in_conn => $input_ty),*);

        // Generate the SelfDescribing implementation
        impl $crate::registry::SelfDescribing for $ty {
            fn task_info() -> $crate::registry::TaskInfo {
                let mut inputs = ::std::collections::HashMap::new();
                $(
                    inputs.entry($in_conn.to_string())
                        .or_insert_with(Vec::new)
                        .push(::std::any::TypeId::of::<$input_ty>());
                )*

                let mut outputs = ::std::collections::HashMap::new();
                $(
                    outputs.entry($out_conn.to_string())
                        .or_insert_with(Vec::new)
                        .push(::std::any::TypeId::of::<$output_ty>());
                )*

                $crate::registry::TaskInfo {
                    task_id: $task_id.to_string(),
                    config_tpl: <$ty as $crate::task_defs::ConfigTemplate>::config_template(),
                    info: $crate::registry::TaskDefInfo::OperatorDef {
                        inputs,
                        outputs,
                        build_operator: <$ty>::new,
                    },
                }
            }
        }
    };

    // Old pattern for backward compatibility
    ($ty:ty, $($conn:literal => $input_ty:ty),* $(,)?) => {
        impl_operator_handler!(@impl_trait $ty, $($conn => $input_ty),*);
    };

    // Internal rule to implement the Operator trait
    (@impl_trait $ty:ty, $($conn:literal => $input_ty:ty),*) => {
        #[async_trait::async_trait]
        impl $crate::task_defs::operator::Operator for $ty {
            async fn handle_event_for_conn(
                &mut self,
                ctx: &$crate::task_defs::MuetlContext,
                conn_name: &String,
                ev: std::sync::Arc<$crate::messages::event::Event>,
            ) {
                // Try each connection/type pair in order
                $(
                    if conn_name == $conn {
                        if let Some(data) = ev.get_data().downcast_ref::<$input_ty>() {
                            <Self as $crate::task_defs::Input<$input_ty>>::handle(self, ctx, data).await;
                            return;
                        }
                    }
                )*

                // If we get here, either the connection was unknown or all type downcasts failed
                // Collect all known connection names for better error reporting
                let known_connections: ::std::collections::HashSet<&str> = vec![$($conn),*].into_iter().collect();

                if known_connections.contains(conn_name.as_str()) {
                    tracing::warn!(
                        conn_name = %conn_name,
                        actual_type = ?ev.get_data().type_id(),
                        event_name = %ev.name,
                        "Type mismatch: failed to downcast event data to any expected type for this connection"
                    );
                } else {
                    tracing::warn!(
                        conn_name = %conn_name,
                        event_name = %ev.name,
                        known_connections = ?known_connections,
                        "Unknown connection name for operator"
                    );
                }
            }
        }
    };
}

/// Generates the `handle_event_for_conn` implementation for a Sink.
///
/// This macro reduces boilerplate by generating the connection name matching
/// and type downcasting logic. It calls the `SinkInput<T>::handle` method for each
/// type registered with the macro.
///
/// # Multiple Types Per Input
///
/// You can specify multiple types for the same input connection using bracket syntax.
/// The macro will try to downcast to each type in order, calling the first handler that matches:
///
/// ```ignore
/// impl_sink_handler!(
///     MySink,
///     task_id = "my_sink",
///     "input" => [serde_json::Value, String],  // Try these types in order
/// );
/// ```
///
/// # Example
///
/// ```ignore
/// struct MySink { count: u64 }
///
/// impl SinkInput<String> for MySink {
///     const conn_name: &'static str = "input";
///     async fn handle(&mut self, ctx: &MuetlSinkContext, value: &String) {
///         println!("[{}] {}", ctx.event_name, value);
///         self.count += 1;
///     }
/// }
///
/// impl_sink_handler!(MySink, "input" => String);
/// ```
///
/// With the new self-describing registration system, use:
/// ```ignore
/// impl_sink_handler!(MySink, task_id = "my_sink", "input" => String);
/// ```
#[macro_export]
macro_rules! impl_sink_handler {
    // New pattern with task_id for self-describing registration
    ($ty:ty, task_id = $task_id:literal, $($inputs:tt)*) => {
        // Parse and expand inputs, then delegate to implementation
        impl_sink_handler!(@expand_and_impl $ty, $task_id, inputs($($inputs)*));
    };

    // Expand inputs, then generate implementations
    (@expand_and_impl $ty:ty, $task_id:literal, inputs($($inputs:tt)*)) => {
        impl_sink_handler!(@impl_with_expanded $ty, $task_id,
            inputs_expanded[], inputs_to_parse[$($inputs)*]
        );
    };

    // Parse inputs: handle "conn" => [Type1, Type2, ...]
    (@impl_with_expanded $ty:ty, $task_id:literal,
        inputs_expanded[$($conn_exp:literal => $input_ty_exp:ty),*],
        inputs_to_parse[$conn:literal => [$($types:ty),+ $(,)?] $(, $($rest:tt)*)?]
    ) => {
        impl_sink_handler!(@impl_with_expanded $ty, $task_id,
            inputs_expanded[$($conn_exp => $input_ty_exp,)* $($conn => $types),*],
            inputs_to_parse[$($($rest)*)?]
        );
    };

    // Parse inputs: handle "conn" => Type
    (@impl_with_expanded $ty:ty, $task_id:literal,
        inputs_expanded[$($conn_exp:literal => $input_ty_exp:ty),*],
        inputs_to_parse[$conn:literal => $single_type:ty $(, $($rest:tt)*)?]
    ) => {
        impl_sink_handler!(@impl_with_expanded $ty, $task_id,
            inputs_expanded[$($conn_exp => $input_ty_exp,)* $conn => $single_type],
            inputs_to_parse[$($($rest)*)?]
        );
    };

    // All parsing complete, generate the implementations
    (@impl_with_expanded $ty:ty, $task_id:literal,
        inputs_expanded[$($conn:literal => $input_ty:ty),*],
        inputs_to_parse[]
    ) => {
        // Generate the Sink trait implementation
        impl_sink_handler!(@impl_trait $ty, $($conn => $input_ty),*);

        // Generate the SelfDescribing implementation
        impl $crate::registry::SelfDescribing for $ty {
            fn task_info() -> $crate::registry::TaskInfo {
                let mut inputs = ::std::collections::HashMap::new();
                $(
                    inputs.entry($conn.to_string())
                        .or_insert_with(Vec::new)
                        .push(::std::any::TypeId::of::<$input_ty>());
                )*

                $crate::registry::TaskInfo {
                    task_id: $task_id.to_string(),
                    config_tpl: <$ty as $crate::task_defs::ConfigTemplate>::config_template(),
                    info: $crate::registry::TaskDefInfo::SinkDef {
                        inputs,
                        build_sink: <$ty>::new,
                    },
                }
            }
        }
    };

    // Old pattern for backward compatibility
    ($ty:ty, $($conn:literal => $input_ty:ty),* $(,)?) => {
        impl_sink_handler!(@impl_trait $ty, $($conn => $input_ty),*);
    };

    // Internal rule to implement the Sink trait
    (@impl_trait $ty:ty, $($conn:literal => $input_ty:ty),*) => {
        #[async_trait::async_trait]
        impl $crate::task_defs::sink::Sink for $ty {
            async fn handle_event_for_conn(
                &mut self,
                ctx: &$crate::task_defs::MuetlSinkContext,
                conn_name: &String,
                ev: std::sync::Arc<$crate::messages::event::Event>,
            ) {
                // Try each connection/type pair in order
                $(
                    if conn_name == $conn {
                        if let Some(data) = ev.get_data().downcast_ref::<$input_ty>() {
                            <Self as $crate::task_defs::SinkInput<$input_ty>>::handle(self, ctx, data).await;
                            return;
                        }
                    }
                )*

                // If we get here, either the connection was unknown or all type downcasts failed
                // Collect all known connection names for better error reporting
                let known_connections: ::std::collections::HashSet<&str> = vec![$($conn),*].into_iter().collect();

                if known_connections.contains(conn_name.as_str()) {
                    tracing::warn!(
                        conn_name = %conn_name,
                        actual_type = ?ev.get_data().type_id(),
                        event_name = %ev.name,
                        "Type mismatch: failed to downcast event data to any expected type for this connection"
                    );
                } else {
                    tracing::warn!(
                        conn_name = %conn_name,
                        event_name = %ev.name,
                        "Unknown connection name for sink"
                    );
                }
            }
        }
    };
}

/// Generates the SelfDescribing trait implementation for a Source.
///
/// This macro generates the self-describing metadata for a Source TaskDef,
/// allowing it to be automatically registered with the registry using
/// `registry.register::<SourceType>()`.
///
/// # Example
///
/// ```ignore
/// struct Ticker {
///     t: u64,
///     period: Duration,
///     iterations: u64,
/// }
///
/// impl Output<u64> for Ticker {
///     const conn_name: &'static str = "tick";
/// }
///
/// impl_source_handler!(Ticker, task_id = "ticker", "tick" => u64);
/// impl_config_template!(
///     Ticker,
///     period_ms: Uint = 1000,
///     iterations: Uint = 10,
/// );
/// ```
#[macro_export]
macro_rules! impl_source_handler {
    ($ty:ty, task_id = $task_id:literal, $($conn:literal => $output_ty:ty),* $(,)?) => {
        impl $crate::registry::SelfDescribing for $ty {
            fn task_info() -> $crate::registry::TaskInfo {
                let mut outputs = ::std::collections::HashMap::new();
                $(
                    outputs.entry($conn.to_string())
                        .or_insert_with(Vec::new)
                        .push(::std::any::TypeId::of::<$output_ty>());
                )*

                $crate::registry::TaskInfo {
                    task_id: $task_id.to_string(),
                    config_tpl: <$ty as $crate::task_defs::ConfigTemplate>::config_template(),
                    info: $crate::registry::TaskDefInfo::SourceDef {
                        outputs,
                        build_source: <$ty>::new,
                    },
                }
            }
        }
    };
}

/// A TaskDef represents any process that is executed by muetl.
pub trait TaskDef {
    fn deinit(&mut self) -> Result<(), String> {
        Ok(())
    }
}

/// A trait for TaskDefs to optionally provide their configuration template.
/// This is used by the self-describing registration system.
pub trait ConfigTemplate {
    fn config_template() -> Option<TaskConfigTpl> {
        None
    }
}

/// Users should implement Input<Some Type> to declare that their Node is
/// capable of processing that type of message on the given connection named
/// conn_name.
pub trait Input<T> {
    #[allow(non_upper_case_globals)]
    const conn_name: &'static str;
    #[allow(async_fn_in_trait)]
    async fn handle(&mut self, ctx: &MuetlContext, input: &T);
}

/// Users should implement SinkInput<Some Type> to declare that their Sink is
/// capable of processing that type of message on the given connection named
/// conn_name.
pub trait SinkInput<T> {
    #[allow(non_upper_case_globals)]
    const conn_name: &'static str;
    #[allow(async_fn_in_trait)]
    async fn handle(&mut self, ctx: &MuetlSinkContext, input: &T);
}

/// Users should implement Output<Some Type> to declare that their Daemon,
/// Source, or Node is capable of producing that type of message on the
/// given connection named conn_name. Note that the TaskResult produced
/// by any Input<T>::handle() is checked at runtime to ensure that
/// each Event's conn_name and type match exactly one Output<T>
/// impl for the TaskDef.
pub trait Output<T> {
    #[allow(non_upper_case_globals)]
    const conn_name: &'static str;
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
    /// named "output_1" with possible output types `[String, i32, bool]`. At runtime, two
    /// Sinks subscribe to the "output_1" connection:
    /// - Sink #1 requests types `[i32, bool]`.
    /// - Sink #2 requests type `[String]`.
    ///
    /// Prior to starting the TaskDefs, muetl will negotiate the acceptable types such that
    /// all subscribers agree on the type they'll receive. For the example above, the list
    /// of current subscribers would look like:
    /// {
    ///   output_1: [i32, String]
    /// }
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
    /// The name of the current event being processed (if any).
    pub event_name: Option<String>,
    /// Headers from the current event being processed (if any).
    /// In the case of a Source, event_headers will be None, because it has no event to get them from.
    pub event_headers: Option<HashMap<String, String>>,
}

/// A MuetlSinkContext contains the context a Sink should use when running. It's different from a `MuetlContext` in that it has
/// no output results channel, nor does it have a map of current subscribers.
pub struct MuetlSinkContext {
    /// The channel to send statuses back on.
    pub status: Sender<Status>,
    /// The name of the current event being processed.
    pub event_name: String,
    /// Headers from the current event being processed.
    pub event_headers: HashMap<String, String>,
}
