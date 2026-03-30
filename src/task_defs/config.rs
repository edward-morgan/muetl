use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
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
/// - `UtcDateTime` - UTC DateTime (RFC3339 string like "2024-01-01T00:00:00Z")
/// - `Arr[Type]` - Array of Type (e.g., `Arr[Str]` for array of strings)
/// - `Map[Type]` - Map from String to Type (e.g., `Map[Str]` for map of strings)
/// - `Enum["a", "b", ...]` - One of the listed string values
/// - `Enum[Type: val, val, ...]` - Typed enum (e.g., `Enum[Num: 1, 2, 3]`)
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
///     settings: Map[Str],       // Optional map of strings
///     limits: Map[Num]!,        // Required map of numbers
///     format: Enum["json", "csv"]!,        // Required string enum
///     mode: Enum["fast", "slow"] = "fast", // String enum with default
///     level: Enum["info", "warn"],         // Optional string enum
///     retries: Enum[Num: 0, 1, 3, 5]!,    // Required typed enum
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

    // Parse map field with default value (simple type)
    (@parse_fields $vec:ident; $field:ident: Map[$ftype:ident] = $default:expr, $($rest:tt)*) => {
        $vec.push($crate::task_defs::ConfigField::with_default(
            stringify!($field),
            impl_config_template!(@map_value $ftype, $default)
        ));
        impl_config_template!(@parse_fields $vec; $($rest)*);
    };

    // Parse map field with default value (array type)
    (@parse_fields $vec:ident; $field:ident: Map[Arr[$inner_type:ident]] = $default:expr, $($rest:tt)*) => {
        $vec.push($crate::task_defs::ConfigField::with_default(
            stringify!($field),
            impl_config_template!(@map_arr_value $inner_type, $default)
        ));
        impl_config_template!(@parse_fields $vec; $($rest)*);
    };

    // Parse required map field (simple type)
    (@parse_fields $vec:ident; $field:ident: Map[$ftype:ident]!, $($rest:tt)*) => {
        $vec.push($crate::task_defs::ConfigField::required(
            stringify!($field),
            $crate::task_defs::ConfigType::Map(Box::new(impl_config_template!(@type $ftype)))
        ));
        impl_config_template!(@parse_fields $vec; $($rest)*);
    };

    // Parse required map field (array type)
    (@parse_fields $vec:ident; $field:ident: Map[Arr[$inner_type:ident]]!, $($rest:tt)*) => {
        $vec.push($crate::task_defs::ConfigField::required(
            stringify!($field),
            $crate::task_defs::ConfigType::Map(Box::new(
                $crate::task_defs::ConfigType::Arr(Box::new(impl_config_template!(@type $inner_type)))
            ))
        ));
        impl_config_template!(@parse_fields $vec; $($rest)*);
    };

    // Parse optional map field without default (simple type)
    (@parse_fields $vec:ident; $field:ident: Map[$ftype:ident], $($rest:tt)*) => {
        $vec.push($crate::task_defs::ConfigField::optional(
            stringify!($field),
            $crate::task_defs::ConfigType::Map(Box::new(impl_config_template!(@type $ftype)))
        ));
        impl_config_template!(@parse_fields $vec; $($rest)*);
    };

    // Parse optional map field without default (array type)
    (@parse_fields $vec:ident; $field:ident: Map[Arr[$inner_type:ident]], $($rest:tt)*) => {
        $vec.push($crate::task_defs::ConfigField::optional(
            stringify!($field),
            $crate::task_defs::ConfigType::Map(Box::new(
                $crate::task_defs::ConfigType::Arr(Box::new(impl_config_template!(@type $inner_type)))
            ))
        ));
        impl_config_template!(@parse_fields $vec; $($rest)*);
    };

    // Parse typed enum field with default value
    (@parse_fields $vec:ident; $field:ident: Enum[$ftype:ident: $($variant:expr),+ $(,)?] = $default:expr, $($rest:tt)*) => {
        $vec.push($crate::task_defs::ConfigField::enum_with_default(
            stringify!($field),
            vec![$(impl_config_template!(@value $ftype, $variant)),+],
            impl_config_template!(@value $ftype, $default),
        ));
        impl_config_template!(@parse_fields $vec; $($rest)*);
    };

    // Parse required typed enum field
    (@parse_fields $vec:ident; $field:ident: Enum[$ftype:ident: $($variant:expr),+ $(,)?]!, $($rest:tt)*) => {
        $vec.push($crate::task_defs::ConfigField::required(
            stringify!($field),
            $crate::task_defs::ConfigType::Enum(vec![$(impl_config_template!(@value $ftype, $variant)),+])
        ));
        impl_config_template!(@parse_fields $vec; $($rest)*);
    };

    // Parse optional typed enum field without default
    (@parse_fields $vec:ident; $field:ident: Enum[$ftype:ident: $($variant:expr),+ $(,)?], $($rest:tt)*) => {
        $vec.push($crate::task_defs::ConfigField::optional(
            stringify!($field),
            $crate::task_defs::ConfigType::Enum(vec![$(impl_config_template!(@value $ftype, $variant)),+])
        ));
        impl_config_template!(@parse_fields $vec; $($rest)*);
    };

    // Parse string enum field with default value
    (@parse_fields $vec:ident; $field:ident: Enum[$($variant:literal),+ $(,)?] = $default:expr, $($rest:tt)*) => {
        $vec.push($crate::task_defs::ConfigField::enum_with_default(
            stringify!($field),
            vec![$($crate::task_defs::ConfigValue::Str($variant.to_string())),+],
            $crate::task_defs::ConfigValue::Str($default.to_string()),
        ));
        impl_config_template!(@parse_fields $vec; $($rest)*);
    };

    // Parse required enum field
    (@parse_fields $vec:ident; $field:ident: Enum[$($variant:literal),+ $(,)?]!, $($rest:tt)*) => {
        $vec.push($crate::task_defs::ConfigField::required(
            stringify!($field),
            $crate::task_defs::ConfigType::Enum(vec![$($crate::task_defs::ConfigValue::Str($variant.to_string())),+])
        ));
        impl_config_template!(@parse_fields $vec; $($rest)*);
    };

    // Parse optional enum field without default
    (@parse_fields $vec:ident; $field:ident: Enum[$($variant:literal),+ $(,)?], $($rest:tt)*) => {
        $vec.push($crate::task_defs::ConfigField::optional(
            stringify!($field),
            $crate::task_defs::ConfigType::Enum(vec![$($crate::task_defs::ConfigValue::Str($variant.to_string())),+])
        ));
        impl_config_template!(@parse_fields $vec; $($rest)*);
    };

    // Parse UtcDateTime field with default value
    (@parse_fields $vec:ident; $field:ident: UtcDateTime = $default:expr, $($rest:tt)*) => {
        $vec.push($crate::task_defs::ConfigField::datetime_with_default(
            stringify!($field),
            $crate::task_defs::ConfigValue::Str($default.to_string())
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

    // Parse UtcDateTime field with default value (last field, no comma)
    (@parse_fields $vec:ident; $field:ident: UtcDateTime = $default:expr) => {
        $vec.push($crate::task_defs::ConfigField::datetime_with_default(
            stringify!($field),
            $crate::task_defs::ConfigValue::Str($default.to_string())
        ));
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

    // Parse map field with default value (simple type, last field, no comma)
    (@parse_fields $vec:ident; $field:ident: Map[$ftype:ident] = $default:expr) => {
        $vec.push($crate::task_defs::ConfigField::with_default(
            stringify!($field),
            impl_config_template!(@map_value $ftype, $default)
        ));
    };

    // Parse map field with default value (array type, last field, no comma)
    (@parse_fields $vec:ident; $field:ident: Map[Arr[$inner_type:ident]] = $default:expr) => {
        $vec.push($crate::task_defs::ConfigField::with_default(
            stringify!($field),
            impl_config_template!(@map_arr_value $inner_type, $default)
        ));
    };

    // Parse required map field (simple type, last field, no comma)
    (@parse_fields $vec:ident; $field:ident: Map[$ftype:ident]!) => {
        $vec.push($crate::task_defs::ConfigField::required(
            stringify!($field),
            $crate::task_defs::ConfigType::Map(Box::new(impl_config_template!(@type $ftype)))
        ));
    };

    // Parse required map field (array type, last field, no comma)
    (@parse_fields $vec:ident; $field:ident: Map[Arr[$inner_type:ident]]!) => {
        $vec.push($crate::task_defs::ConfigField::required(
            stringify!($field),
            $crate::task_defs::ConfigType::Map(Box::new(
                $crate::task_defs::ConfigType::Arr(Box::new(impl_config_template!(@type $inner_type)))
            ))
        ));
    };

    // Parse optional map field without default (simple type, last field, no comma)
    (@parse_fields $vec:ident; $field:ident: Map[$ftype:ident]) => {
        $vec.push($crate::task_defs::ConfigField::optional(
            stringify!($field),
            $crate::task_defs::ConfigType::Map(Box::new(impl_config_template!(@type $ftype)))
        ));
    };

    // Parse optional map field without default (array type, last field, no comma)
    (@parse_fields $vec:ident; $field:ident: Map[Arr[$inner_type:ident]]) => {
        $vec.push($crate::task_defs::ConfigField::optional(
            stringify!($field),
            $crate::task_defs::ConfigType::Map(Box::new(
                $crate::task_defs::ConfigType::Arr(Box::new(impl_config_template!(@type $inner_type)))
            ))
        ));
    };

    // Parse typed enum field with default value (last field, no comma)
    (@parse_fields $vec:ident; $field:ident: Enum[$ftype:ident: $($variant:expr),+ $(,)?] = $default:expr) => {
        $vec.push($crate::task_defs::ConfigField::enum_with_default(
            stringify!($field),
            vec![$(impl_config_template!(@value $ftype, $variant)),+],
            impl_config_template!(@value $ftype, $default),
        ));
    };

    // Parse required typed enum field (last field, no comma)
    (@parse_fields $vec:ident; $field:ident: Enum[$ftype:ident: $($variant:expr),+ $(,)?]!) => {
        $vec.push($crate::task_defs::ConfigField::required(
            stringify!($field),
            $crate::task_defs::ConfigType::Enum(vec![$(impl_config_template!(@value $ftype, $variant)),+])
        ));
    };

    // Parse optional typed enum field without default (last field, no comma)
    (@parse_fields $vec:ident; $field:ident: Enum[$ftype:ident: $($variant:expr),+ $(,)?]) => {
        $vec.push($crate::task_defs::ConfigField::optional(
            stringify!($field),
            $crate::task_defs::ConfigType::Enum(vec![$(impl_config_template!(@value $ftype, $variant)),+])
        ));
    };

    // Parse string enum field with default value (last field, no comma)
    (@parse_fields $vec:ident; $field:ident: Enum[$($variant:literal),+ $(,)?] = $default:expr) => {
        $vec.push($crate::task_defs::ConfigField::enum_with_default(
            stringify!($field),
            vec![$($crate::task_defs::ConfigValue::Str($variant.to_string())),+],
            $crate::task_defs::ConfigValue::Str($default.to_string()),
        ));
    };

    // Parse required enum field (last field, no comma)
    (@parse_fields $vec:ident; $field:ident: Enum[$($variant:literal),+ $(,)?]!) => {
        $vec.push($crate::task_defs::ConfigField::required(
            stringify!($field),
            $crate::task_defs::ConfigType::Enum(vec![$($crate::task_defs::ConfigValue::Str($variant.to_string())),+])
        ));
    };

    // Parse optional enum field without default (last field, no comma)
    (@parse_fields $vec:ident; $field:ident: Enum[$($variant:literal),+ $(,)?]) => {
        $vec.push($crate::task_defs::ConfigField::optional(
            stringify!($field),
            $crate::task_defs::ConfigType::Enum(vec![$($crate::task_defs::ConfigValue::Str($variant.to_string())),+])
        ));
    };

    // Base case - no more fields
    (@parse_fields $vec:ident;) => {};

    // Helper rules to convert field types to ConfigType
    (@type Num) => { $crate::task_defs::ConfigType::Num };
    (@type Str) => { $crate::task_defs::ConfigType::Str };
    (@type Bool) => { $crate::task_defs::ConfigType::Bool };
    (@type UtcDateTime) => { $crate::task_defs::ConfigType::UtcDateTime };

    (@value Num, $val:expr) => {
        $crate::task_defs::ConfigValue::Num($val)
    };
    (@value Str, $val:expr) => {
        $crate::task_defs::ConfigValue::Str($val.to_string())
    };
    (@value Bool, $val:expr) => {
        $crate::task_defs::ConfigValue::Bool($val)
    };
    (@value UtcDateTime, $val:expr) => {
        $crate::task_defs::ConfigValue::Str($val.to_string())
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
    (@array_value UtcDateTime, $val:expr) => {
        $crate::task_defs::ConfigValue::Arr(
            $val.into_iter().map(|v| $crate::task_defs::ConfigValue::Str(v.to_string())).collect()
        )
    };

    // Helper rules to convert map values (simple types)
    (@map_value Num, $val:expr) => {
        $crate::task_defs::ConfigValue::Map(
            $val.into_iter().map(|(k, v)| (k.to_string(), $crate::task_defs::ConfigValue::Num(v))).collect()
        )
    };
    (@map_value Str, $val:expr) => {
        $crate::task_defs::ConfigValue::Map(
            $val.into_iter().map(|(k, v)| (k.to_string(), $crate::task_defs::ConfigValue::Str(v.to_string()))).collect()
        )
    };
    (@map_value Bool, $val:expr) => {
        $crate::task_defs::ConfigValue::Map(
            $val.into_iter().map(|(k, v)| (k.to_string(), $crate::task_defs::ConfigValue::Bool(v))).collect()
        )
    };
    (@map_value UtcDateTime, $val:expr) => {
        $crate::task_defs::ConfigValue::Map(
            $val.into_iter().map(|(k, v)| (k.to_string(), $crate::task_defs::ConfigValue::Str(v.to_string()))).collect()
        )
    };

    // Helper rules to convert map values (array types)
    (@map_arr_value Num, $val:expr) => {
        $crate::task_defs::ConfigValue::Map(
            $val.into_iter().map(|(k, v)| (
                k.to_string(),
                $crate::task_defs::ConfigValue::Arr(
                    v.into_iter().map(|i| $crate::task_defs::ConfigValue::Num(i)).collect()
                )
            )).collect()
        )
    };
    (@map_arr_value Str, $val:expr) => {
        $crate::task_defs::ConfigValue::Map(
            $val.into_iter().map(|(k, v)| (
                k.to_string(),
                $crate::task_defs::ConfigValue::Arr(
                    v.into_iter().map(|i| $crate::task_defs::ConfigValue::Str(i.to_string())).collect()
                )
            )).collect()
        )
    };
    (@map_arr_value Bool, $val:expr) => {
        $crate::task_defs::ConfigValue::Map(
            $val.into_iter().map(|(k, v)| (
                k.to_string(),
                $crate::task_defs::ConfigValue::Arr(
                    v.into_iter().map(|i| $crate::task_defs::ConfigValue::Bool(i)).collect()
                )
            )).collect()
        )
    };
    (@map_arr_value UtcDateTime, $val:expr) => {
        $crate::task_defs::ConfigValue::Map(
            $val.into_iter().map(|(k, v)| (
                k.to_string(),
                $crate::task_defs::ConfigValue::Arr(
                    v.into_iter().map(|i| $crate::task_defs::ConfigValue::Str(i.to_string())).collect()
                )
            )).collect()
        )
    };
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
                    // Special handling for UtcDateTime - convert from string if needed
                    let converted_value = if matches!(field.field_type, ConfigType::UtcDateTime) {
                        match value {
                            ConfigValue::Str(s) => match DateTime::parse_from_rfc3339(s) {
                                Ok(dt) => ConfigValue::UtcDateTime(dt.with_timezone(&Utc)),
                                Err(e) => {
                                    errors.push(format!(
                                        "field '{}': invalid datetime format '{}': {}",
                                        field.name, s, e
                                    ));
                                    value.clone()
                                }
                            },
                            v @ ConfigValue::UtcDateTime(_) => v.clone(),
                            _ => value.clone(),
                        }
                    } else {
                        value.clone()
                    };

                    // Type check
                    if !field.field_type.matches(&converted_value) {
                        errors.push(format!(
                            "field '{}': expected {:?}, got {:?}",
                            field.name,
                            field.field_type,
                            converted_value.config_type()
                        ));
                    } else {
                        values.insert(field.name.clone(), converted_value);
                    }
                }
                None if field.required => {
                    errors.push(format!("missing required field '{}'", field.name));
                }
                None => {
                    // Apply default if present
                    if let Some(default) = &field.default {
                        // Convert default value if it's a UtcDateTime field
                        let converted_default = if matches!(
                            field.field_type,
                            ConfigType::UtcDateTime
                        ) {
                            match default {
                                ConfigValue::Str(s) => match DateTime::parse_from_rfc3339(s) {
                                    Ok(dt) => ConfigValue::UtcDateTime(dt.with_timezone(&Utc)),
                                    Err(e) => {
                                        errors.push(format!(
                                            "field '{}': invalid default datetime format '{}': {}",
                                            field.name, s, e
                                        ));
                                        default.clone()
                                    }
                                },
                                v @ ConfigValue::UtcDateTime(_) => v.clone(),
                                _ => default.clone(),
                            }
                        } else {
                            default.clone()
                        };
                        values.insert(field.name.clone(), converted_default);
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

    pub fn enum_with_default(name: &str, choices: Vec<ConfigValue>, default: ConfigValue) -> Self {
        ConfigField {
            name: name.to_string(),
            field_type: ConfigType::Enum(choices),
            required: false,
            default: Some(default),
        }
    }

    pub fn datetime_with_default(name: &str, default: ConfigValue) -> Self {
        ConfigField {
            name: name.to_string(),
            field_type: ConfigType::UtcDateTime,
            required: false,
            default: Some(default),
        }
    }
}

/// The expected type of a configuration field
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfigType {
    Str,
    Num,
    Bool,
    UtcDateTime,
    Arr(Box<ConfigType>),
    Map(Box<ConfigType>),
    Any,
    Enum(Vec<ConfigValue>),
}

impl ConfigType {
    /// Check if a value matches this type
    pub fn matches(&self, value: &ConfigValue) -> bool {
        match (self, value) {
            (ConfigType::Any, _) => true,
            (ConfigType::Str, ConfigValue::Str(_)) => true,
            (ConfigType::Num, ConfigValue::Num(_)) => true,
            (ConfigType::Bool, ConfigValue::Bool(_)) => true,
            (ConfigType::UtcDateTime, ConfigValue::UtcDateTime(_)) => true,
            (ConfigType::Arr(inner), ConfigValue::Arr(items)) => {
                items.iter().all(|item| inner.matches(item))
            }
            (ConfigType::Map(inner), ConfigValue::Map(map)) => {
                map.values().all(|v| inner.matches(v))
            }
            (ConfigType::Enum(choices), value) => {
                for choice in choices {
                    if *choice == *value {
                        return true;
                    }
                }
                return false;
            }
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum ConfigValue {
    Str(String),
    Num(i64),
    Bool(bool),
    UtcDateTime(DateTime<Utc>),
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
            Self::UtcDateTime(_) => ConfigType::UtcDateTime,
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

    pub fn get_map(&self, key: &str) -> Option<&HashMap<String, ConfigValue>> {
        match self.values.get(key)? {
            ConfigValue::Map(map) => Some(map),
            _ => None,
        }
    }

    pub fn get_utc_datetime(&self, key: &str) -> Option<DateTime<Utc>> {
        match self.values.get(key)? {
            ConfigValue::UtcDateTime(dt) => Some(*dt),
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

    pub fn require_utc_datetime(&self, key: &str) -> DateTime<Utc> {
        self.get_utc_datetime(key)
            .unwrap_or_else(|| panic!("missing or invalid config key: {}", key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- ConfigType::matches ---

    #[test]
    fn matches_str() {
        assert!(ConfigType::Str.matches(&ConfigValue::Str("hello".into())));
        assert!(!ConfigType::Str.matches(&ConfigValue::Num(1)));
        assert!(!ConfigType::Str.matches(&ConfigValue::Bool(true)));
    }

    #[test]
    fn matches_num() {
        assert!(ConfigType::Num.matches(&ConfigValue::Num(42)));
        assert!(!ConfigType::Num.matches(&ConfigValue::Str("42".into())));
        assert!(!ConfigType::Num.matches(&ConfigValue::Bool(false)));
    }

    #[test]
    fn matches_bool() {
        assert!(ConfigType::Bool.matches(&ConfigValue::Bool(true)));
        assert!(ConfigType::Bool.matches(&ConfigValue::Bool(false)));
        assert!(!ConfigType::Bool.matches(&ConfigValue::Num(0)));
        assert!(!ConfigType::Bool.matches(&ConfigValue::Str("true".into())));
    }

    #[test]
    fn matches_any() {
        assert!(ConfigType::Any.matches(&ConfigValue::Str("x".into())));
        assert!(ConfigType::Any.matches(&ConfigValue::Num(1)));
        assert!(ConfigType::Any.matches(&ConfigValue::Bool(true)));
        assert!(ConfigType::Any.matches(&ConfigValue::Arr(vec![])));
        assert!(ConfigType::Any.matches(&ConfigValue::Map(HashMap::new())));
    }

    #[test]
    fn matches_arr_homogeneous() {
        let ty = ConfigType::Arr(Box::new(ConfigType::Str));
        assert!(ty.matches(&ConfigValue::Arr(vec![
            ConfigValue::Str("a".into()),
            ConfigValue::Str("b".into()),
        ])));
    }

    #[test]
    fn matches_arr_empty() {
        let ty = ConfigType::Arr(Box::new(ConfigType::Num));
        assert!(ty.matches(&ConfigValue::Arr(vec![])));
    }

    #[test]
    fn matches_arr_rejects_wrong_element() {
        let ty = ConfigType::Arr(Box::new(ConfigType::Str));
        assert!(!ty.matches(&ConfigValue::Arr(vec![
            ConfigValue::Str("ok".into()),
            ConfigValue::Num(99),
        ])));
    }

    #[test]
    fn matches_arr_rejects_non_arr_value() {
        let ty = ConfigType::Arr(Box::new(ConfigType::Str));
        assert!(!ty.matches(&ConfigValue::Str("not an array".into())));
    }

    #[test]
    fn matches_map_homogeneous() {
        let ty = ConfigType::Map(Box::new(ConfigType::Num));
        let mut m = HashMap::new();
        m.insert("a".into(), ConfigValue::Num(1));
        m.insert("b".into(), ConfigValue::Num(2));
        assert!(ty.matches(&ConfigValue::Map(m)));
    }

    #[test]
    fn matches_map_empty() {
        let ty = ConfigType::Map(Box::new(ConfigType::Bool));
        assert!(ty.matches(&ConfigValue::Map(HashMap::new())));
    }

    #[test]
    fn matches_map_rejects_wrong_value_type() {
        let ty = ConfigType::Map(Box::new(ConfigType::Bool));
        let mut m = HashMap::new();
        m.insert("ok".into(), ConfigValue::Bool(true));
        m.insert("bad".into(), ConfigValue::Str("nope".into()));
        assert!(!ty.matches(&ConfigValue::Map(m)));
    }

    #[test]
    fn matches_map_rejects_non_map_value() {
        let ty = ConfigType::Map(Box::new(ConfigType::Str));
        assert!(!ty.matches(&ConfigValue::Num(5)));
    }

    #[test]
    fn matches_nested_arr_of_arr() {
        let ty = ConfigType::Arr(Box::new(ConfigType::Arr(Box::new(ConfigType::Num))));
        assert!(ty.matches(&ConfigValue::Arr(vec![
            ConfigValue::Arr(vec![ConfigValue::Num(1), ConfigValue::Num(2)]),
            ConfigValue::Arr(vec![ConfigValue::Num(3)]),
        ])));
        // Inner element wrong type
        assert!(!ty.matches(&ConfigValue::Arr(vec![ConfigValue::Arr(vec![
            ConfigValue::Str("bad".into()),
        ])])));
    }

    #[test]
    fn matches_enum_accepts_member() {
        let ty = ConfigType::Enum(vec![
            ConfigValue::Str("json".into()),
            ConfigValue::Str("csv".into()),
        ]);
        assert!(ty.matches(&ConfigValue::Str("json".into())));
        assert!(ty.matches(&ConfigValue::Str("csv".into())));
    }

    #[test]
    fn matches_enum_rejects_non_member() {
        let ty = ConfigType::Enum(vec![
            ConfigValue::Str("json".into()),
            ConfigValue::Str("csv".into()),
        ]);
        assert!(!ty.matches(&ConfigValue::Str("xml".into())));
    }

    #[test]
    fn matches_enum_mixed_types() {
        let ty = ConfigType::Enum(vec![
            ConfigValue::Str("auto".into()),
            ConfigValue::Num(0),
            ConfigValue::Bool(false),
        ]);
        assert!(ty.matches(&ConfigValue::Str("auto".into())));
        assert!(ty.matches(&ConfigValue::Num(0)));
        assert!(ty.matches(&ConfigValue::Bool(false)));
        assert!(!ty.matches(&ConfigValue::Num(1)));
        assert!(!ty.matches(&ConfigValue::Str("manual".into())));
    }

    #[test]
    fn matches_enum_empty_rejects_all() {
        let ty = ConfigType::Enum(vec![]);
        assert!(!ty.matches(&ConfigValue::Str("anything".into())));
        assert!(!ty.matches(&ConfigValue::Num(0)));
    }
}
