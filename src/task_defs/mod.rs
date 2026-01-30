pub mod operator;
pub mod sink;
pub mod source;

use std::{
    any::TypeId,
    collections::{HashMap, HashSet},
    fmt::Debug,
};

use serde::{Deserialize, Serialize};
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

/// Generates a configuration template for a TaskDef.
///
/// This macro simplifies creating configuration templates by generating the
/// `ConfigTemplate` trait implementation with field definitions and defaults.
///
/// # Syntax
///
/// Fields can be declared in three forms:
/// - `field: Type = default` - Optional field with default value
/// - `field: Type!` - Required field (must be provided)
/// - `field: Type` - Optional field without default
///
/// Types can be:
/// - `Num` - Number (i64)
/// - `Str` - String
/// - `Bool` - Boolean
/// - `Arr[Type]` - Array of Type (e.g., `Arr[Str]` for array of strings)
///
/// # Example
///
/// ```ignore
/// impl_config_template!(
///     MyTask,
///     schedule: Str!,           // Required string field
///     count: Num = 10,          // Optional num with default
///     enabled: Bool,            // Optional bool without default
///     tags: Arr[Str] = vec!["default"], // Array of strings with default
///     ports: Arr[Num]!,         // Required array of numbers
/// );
/// ```
#[macro_export]
macro_rules! impl_config_template {
    // Main entry point
    ($ty:ty, $($fields:tt)*) => {
        impl $crate::task_defs::ConfigTemplate for $ty {
            fn config_template() -> Option<$crate::task_defs::TaskConfigTpl> {
                #[allow(unused_mut)]
                let mut fields = vec![];
                impl_config_template!(@parse_fields fields; $($fields)*);
                Some($crate::task_defs::TaskConfigTpl {
                    fields,
                    disallow_unknown_fields: true,
                })
            }
        }
    };

    // Parse array field with default value
    (@parse_fields $vec:ident; $field:ident: Arr[$ftype:ident] = $default:expr, $($rest:tt)*) => {
        $vec.push($crate::task_defs::ConfigField::with_default(
            stringify!($field),
            impl_config_template!(@array_value $ftype, $default)
        ));
        impl_config_template!(@parse_fields $vec; $($rest)*);
    };

    // Parse required array field
    (@parse_fields $vec:ident; $field:ident: Arr[$ftype:ident]!, $($rest:tt)*) => {
        $vec.push($crate::task_defs::ConfigField::required(
            stringify!($field),
            $crate::task_defs::ConfigType::Arr(Box::new(impl_config_template!(@type $ftype)))
        ));
        impl_config_template!(@parse_fields $vec; $($rest)*);
    };

    // Parse optional array field without default
    (@parse_fields $vec:ident; $field:ident: Arr[$ftype:ident], $($rest:tt)*) => {
        $vec.push($crate::task_defs::ConfigField::optional(
            stringify!($field),
            $crate::task_defs::ConfigType::Arr(Box::new(impl_config_template!(@type $ftype)))
        ));
        impl_config_template!(@parse_fields $vec; $($rest)*);
    };

    // Parse field with default value
    (@parse_fields $vec:ident; $field:ident: $ftype:ident = $default:expr, $($rest:tt)*) => {
        $vec.push($crate::task_defs::ConfigField::with_default(
            stringify!($field),
            impl_config_template!(@value $ftype, $default)
        ));
        impl_config_template!(@parse_fields $vec; $($rest)*);
    };

    // Parse required field
    (@parse_fields $vec:ident; $field:ident: $ftype:ident!, $($rest:tt)*) => {
        $vec.push($crate::task_defs::ConfigField::required(
            stringify!($field),
            impl_config_template!(@type $ftype)
        ));
        impl_config_template!(@parse_fields $vec; $($rest)*);
    };

    // Parse optional field without default
    (@parse_fields $vec:ident; $field:ident: $ftype:ident, $($rest:tt)*) => {
        $vec.push($crate::task_defs::ConfigField::optional(
            stringify!($field),
            impl_config_template!(@type $ftype)
        ));
        impl_config_template!(@parse_fields $vec; $($rest)*);
    };

    // Parse field with default value (last field, no comma)
    (@parse_fields $vec:ident; $field:ident: $ftype:ident = $default:expr) => {
        $vec.push($crate::task_defs::ConfigField::with_default(
            stringify!($field),
            impl_config_template!(@value $ftype, $default)
        ));
    };

    // Parse required field (last field, no comma)
    (@parse_fields $vec:ident; $field:ident: $ftype:ident!) => {
        $vec.push($crate::task_defs::ConfigField::required(
            stringify!($field),
            impl_config_template!(@type $ftype)
        ));
    };

    // Parse optional field without default (last field, no comma)
    (@parse_fields $vec:ident; $field:ident: $ftype:ident) => {
        $vec.push($crate::task_defs::ConfigField::optional(
            stringify!($field),
            impl_config_template!(@type $ftype)
        ));
    };

    // Parse array field with default value (last field, no comma)
    (@parse_fields $vec:ident; $field:ident: Arr[$ftype:ident] = $default:expr) => {
        $vec.push($crate::task_defs::ConfigField::with_default(
            stringify!($field),
            impl_config_template!(@array_value $ftype, $default)
        ));
    };

    // Parse required array field (last field, no comma)
    (@parse_fields $vec:ident; $field:ident: Arr[$ftype:ident]!) => {
        $vec.push($crate::task_defs::ConfigField::required(
            stringify!($field),
            $crate::task_defs::ConfigType::Arr(Box::new(impl_config_template!(@type $ftype)))
        ));
    };

    // Parse optional array field without default (last field, no comma)
    (@parse_fields $vec:ident; $field:ident: Arr[$ftype:ident]) => {
        $vec.push($crate::task_defs::ConfigField::optional(
            stringify!($field),
            $crate::task_defs::ConfigType::Arr(Box::new(impl_config_template!(@type $ftype)))
        ));
    };

    // Base case - no more fields
    (@parse_fields $vec:ident;) => {};

    // Helper rules to convert field types to ConfigType
    (@type Num) => { $crate::task_defs::ConfigType::Num };
    (@type Str) => { $crate::task_defs::ConfigType::Str };
    (@type Bool) => { $crate::task_defs::ConfigType::Bool };

    (@value Num, $val:expr) => {
        $crate::task_defs::ConfigValue::Num($val)
    };
    (@value Str, $val:expr) => {
        $crate::task_defs::ConfigValue::Str($val.to_string())
    };
    (@value Bool, $val:expr) => {
        $crate::task_defs::ConfigValue::Bool($val)
    };

    // Helper rules to convert array values
    (@array_value Num, $val:expr) => {
        $crate::task_defs::ConfigValue::Arr(
            $val.into_iter().map(|v| $crate::task_defs::ConfigValue::Num(v)).collect()
        )
    };
    (@array_value Str, $val:expr) => {
        $crate::task_defs::ConfigValue::Arr(
            $val.into_iter().map(|v| $crate::task_defs::ConfigValue::Str(v.to_string())).collect()
        )
    };
    (@array_value Bool, $val:expr) => {
        $crate::task_defs::ConfigValue::Arr(
            $val.into_iter().map(|v| $crate::task_defs::ConfigValue::Bool(v)).collect()
        )
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
// pub enum OutputType {
//     /// The output may only be of this single type.
//     Singleton(TypeId),
//     /// The output may be exactly one of the types listed.
//     Sum(Vec<TypeId>),
//     /// The output may be any one of the types listed.
//     Product(Vec<TypeId>),
// }
// impl OutputType {
//     /// Check the given type against the OutputType, which differs
//     /// based on whether this is a sum or product type.
//     pub fn check_type(&self, against: TypeId) -> bool {
//         match self {
//             Self::Singleton(tpe) => *tpe == against,
//             Self::Sum(types) => types.contains(&against),
//             Self::Product(types) => types.contains(&against),
//         }
//     }

//     pub fn check_types(&self, against: Vec<TypeId>) -> bool {
//         if against.len() == 0 {
//             return false;
//         }

//         match self {
//             Self::Singleton(tpe) => against.iter().all(|&a| a == *tpe),
//             Self::Sum(types) => {
//                 let to_match = against[0].type_id();
//                 // Check that all types in against match, and that that single type
//                 // exists in the sum type.
//                 types.contains(&to_match) && against.iter().skip(1).all(|&a| a == to_match)
//             }
//             Self::Product(types) => against.iter().all(|a| types.contains(a)),
//         }
//     }

//     /// Construct a new OutputType that can only be a single type.
//     pub fn singleton(of: TypeId) -> Self {
//         Self::Singleton(of)
//     }

//     pub fn singleton_of<T: 'static>() -> Self {
//         Self::Singleton(TypeId::of::<T>())
//     }

//     /// Construct a new OutputType that can be a single type among a
//     /// set of choices.
//     pub fn one_of(choices: Vec<TypeId>) -> Self {
//         Self::Sum(choices)
//     }

//     /// Construct a new OutputType that can be any one of types in a
//     /// set of choices.
//     pub fn any_of(choices: Vec<TypeId>) -> Self {
//         Self::Product(choices)
//     }
// }
// impl Debug for OutputType {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
//             Self::Singleton(tpe) => f.debug_set().entry(tpe).finish(),
//             Self::Product(types) => f.debug_set().entries(types).finish(),
//             Self::Sum(types) => f.debug_set().entries(types).finish(),
//         }
//     }
// }

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
#[derive(Debug, Serialize, Deserialize)]
pub struct TaskConfigTpl {
    /// Single configuration fields. `name` must be matched exactly for this template to be processed.
    pub fields: Vec<ConfigField>,
    /// If true, any fields that are found that don't match this template will be rejected, and config
    /// processing will return an error.
    pub disallow_unknown_fields: bool,
}

impl TaskConfigTpl {
    /// Validate raw config values against this template, returning a TaskConfig
    /// with defaults applied, or a list of validation errors.
    pub fn validate(&self, raw: HashMap<String, ConfigValue>) -> Result<TaskConfig, Vec<String>> {
        let mut errors = vec![];
        let mut values = HashMap::new();

        // Check each declared field
        for field in &self.fields {
            match raw.get(&field.name) {
                Some(value) => {
                    // Type check
                    if !field.field_type.matches(value) {
                        errors.push(format!(
                            "field '{}': expected {:?}, got {:?}",
                            field.name,
                            field.field_type,
                            value.config_type()
                        ));
                    } else {
                        values.insert(field.name.clone(), value.clone());
                    }
                }
                None if field.required => {
                    errors.push(format!("missing required field '{}'", field.name));
                }
                None => {
                    // Apply default if present
                    if let Some(default) = &field.default {
                        values.insert(field.name.clone(), default.clone());
                    }
                }
            }
        }

        // Check for unknown fields
        if self.disallow_unknown_fields {
            let known: HashSet<_> = self.fields.iter().map(|f| &f.name).collect();
            for key in raw.keys() {
                if !known.contains(key) {
                    errors.push(format!("unknown field '{}'", key));
                }
            }
        }

        if errors.is_empty() {
            Ok(TaskConfig::new(values))
        } else {
            Err(errors)
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConfigField {
    pub name: String,
    pub field_type: ConfigType,
    pub required: bool,
    pub default: Option<ConfigValue>,
}

impl ConfigField {
    pub fn required(name: &str, field_type: ConfigType) -> Self {
        ConfigField {
            name: name.to_string(),
            field_type,
            required: true,
            default: None,
        }
    }

    pub fn optional(name: &str, field_type: ConfigType) -> Self {
        ConfigField {
            name: name.to_string(),
            field_type,
            required: false,
            default: None,
        }
    }

    pub fn with_default(name: &str, default: ConfigValue) -> Self {
        ConfigField {
            name: name.to_string(),
            field_type: default.config_type(),
            required: false,
            default: Some(default),
        }
    }
}

/// The expected type of a configuration field
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConfigType {
    Str,
    Num,
    Bool,
    Arr(Box<ConfigType>),
    Map(Box<ConfigType>),
    Any,
}

impl ConfigType {
    /// Check if a value matches this type
    pub fn matches(&self, value: &ConfigValue) -> bool {
        match (self, value) {
            (ConfigType::Any, _) => true,
            (ConfigType::Str, ConfigValue::Str(_)) => true,
            (ConfigType::Num, ConfigValue::Num(_)) => true,
            (ConfigType::Bool, ConfigValue::Bool(_)) => true,
            (ConfigType::Arr(inner), ConfigValue::Arr(items)) => {
                items.iter().all(|item| inner.matches(item))
            }
            (ConfigType::Map(inner), ConfigValue::Map(map)) => {
                map.values().all(|v| inner.matches(v))
            }
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConfigValue {
    Str(String),
    Num(i64),
    Bool(bool),
    Arr(Vec<ConfigValue>),
    Map(HashMap<String, ConfigValue>),
}

impl ConfigValue {
    /// Returns the ConfigType that matches this value
    pub fn config_type(&self) -> ConfigType {
        match self {
            Self::Str(_) => ConfigType::Str,
            Self::Num(_) => ConfigType::Num,
            Self::Bool(_) => ConfigType::Bool,
            Self::Arr(v) => {
                let inner = v
                    .first()
                    .map(|e| e.config_type())
                    .unwrap_or(ConfigType::Any);
                ConfigType::Arr(Box::new(inner))
            }
            Self::Map(m) => {
                let inner = m
                    .values()
                    .next()
                    .map(|e| e.config_type())
                    .unwrap_or(ConfigType::Any);
                ConfigType::Map(Box::new(inner))
            }
        }
    }

    /// Try to get as a string reference
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Str(s) => Some(s.as_str()),
            _ => None,
        }
    }
}

/// Resolved configuration for a TaskDef instance
#[derive(Debug)]
pub struct TaskConfig {
    values: HashMap<String, ConfigValue>,
}

impl TaskConfig {
    pub fn new(values: HashMap<String, ConfigValue>) -> Self {
        Self { values }
    }

    pub fn empty() -> Self {
        Self {
            values: HashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<&ConfigValue> {
        self.values.get(key)
    }

    pub fn get_str(&self, key: &str) -> Option<&str> {
        match self.values.get(key)? {
            ConfigValue::Str(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn get_i64(&self, key: &str) -> Option<i64> {
        match self.values.get(key)? {
            ConfigValue::Num(n) => Some(*n),
            _ => None,
        }
    }

    pub fn get_bool(&self, key: &str) -> Option<bool> {
        match self.values.get(key)? {
            ConfigValue::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn get_arr(&self, key: &str) -> Option<&Vec<ConfigValue>> {
        match self.values.get(key)? {
            ConfigValue::Arr(arr) => Some(arr),
            _ => None,
        }
    }

    pub fn require_str(&self, key: &str) -> &str {
        self.get_str(key)
            .unwrap_or_else(|| panic!("missing or invalid config key: {}", key))
    }

    pub fn require_i64(&self, key: &str) -> i64 {
        self.get_i64(key)
            .unwrap_or_else(|| panic!("missing or invalid config key: {}", key))
    }

    pub fn require_bool(&self, key: &str) -> bool {
        self.get_bool(key)
            .unwrap_or_else(|| panic!("missing or invalid config key: {}", key))
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
