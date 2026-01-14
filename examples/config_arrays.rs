//! Example demonstrating array support in impl_config_template! macro
//!
//! This example shows how to use the Arr[Type] syntax for array configuration fields.

use std::collections::HashMap;
use muetl::{
    impl_config_template,
    task_defs::{ConfigTemplate, ConfigValue},
};

struct MultiArrayConfig;

impl_config_template!(
    MultiArrayConfig,
    // Required array of strings - must be provided
    required_tags: Arr[Str]!,

    // Optional array of numbers - can be omitted
    optional_ports: Arr[Num],

    // Array with default values
    default_flags: Arr[Bool] = vec![true, false, true],

    // Mix of regular and array fields
    name: Str!,
    count: Num = 10,
    enabled_features: Arr[Str] = vec!["feature1", "feature2"],
);

fn main() {
    println!("Array Configuration Template Example\n");

    // Get the template
    let template = MultiArrayConfig::config_template().expect("Should have template");

    println!("Configuration fields:");
    for field in &template.fields {
        println!("  - {}: {:?} (required: {})",
            field.name,
            field.field_type,
            field.required
        );
        if let Some(default) = &field.default {
            println!("    Default: {:?}", default);
        }
    }

    println!("\n--- Example 1: Valid configuration with all fields ---");
    let mut config = HashMap::new();
    config.insert("name".to_string(), ConfigValue::Str("MyApp".to_string()));
    config.insert(
        "required_tags".to_string(),
        ConfigValue::Arr(vec![
            ConfigValue::Str("production".to_string()),
            ConfigValue::Str("api".to_string()),
            ConfigValue::Str("v2".to_string()),
        ])
    );
    config.insert(
        "optional_ports".to_string(),
        ConfigValue::Arr(vec![
            ConfigValue::Num(8080),
            ConfigValue::Num(8443),
        ])
    );

    match template.validate(config) {
        Ok(validated) => {
            println!("✓ Configuration validated successfully");
            println!("  name: {:?}", validated.get_str("name"));
            println!("  count: {:?}", validated.get_i64("count"));
            println!("  required_tags: {:?}", validated.get_arr("required_tags"));
            println!("  optional_ports: {:?}", validated.get_arr("optional_ports"));
            println!("  enabled_features: {:?}", validated.get_arr("enabled_features"));
            println!("  default_flags: {:?}", validated.get_arr("default_flags"));
        }
        Err(errors) => {
            println!("✗ Validation failed:");
            for error in errors {
                println!("  - {}", error);
            }
        }
    }

    println!("\n--- Example 2: Missing required array field ---");
    let mut config = HashMap::new();
    config.insert("name".to_string(), ConfigValue::Str("MyApp".to_string()));
    // Missing required_tags!

    match template.validate(config) {
        Ok(_) => println!("✓ Validation passed (unexpected)"),
        Err(errors) => {
            println!("✗ Validation failed as expected:");
            for error in errors {
                println!("  - {}", error);
            }
        }
    }

    println!("\n--- Example 3: Type mismatch in array ---");
    let mut config = HashMap::new();
    config.insert("name".to_string(), ConfigValue::Str("MyApp".to_string()));
    config.insert(
        "required_tags".to_string(),
        ConfigValue::Arr(vec![
            ConfigValue::Str("valid".to_string()),
            ConfigValue::Num(123), // Wrong type - should be string!
        ])
    );

    match template.validate(config) {
        Ok(_) => println!("✓ Validation passed (unexpected)"),
        Err(errors) => {
            println!("✗ Validation failed as expected:");
            for error in errors {
                println!("  - {}", error);
            }
        }
    }

    println!("\n--- Example 4: Minimal valid configuration (using defaults) ---");
    let mut config = HashMap::new();
    config.insert("name".to_string(), ConfigValue::Str("MinimalApp".to_string()));
    config.insert(
        "required_tags".to_string(),
        ConfigValue::Arr(vec![
            ConfigValue::Str("minimal".to_string()),
        ])
    );

    match template.validate(config) {
        Ok(validated) => {
            println!("✓ Configuration validated successfully");
            println!("  name: {:?}", validated.get_str("name"));
            println!("  count: {:?} (from default)", validated.get_i64("count"));
            println!("  required_tags: {:?}", validated.get_arr("required_tags"));
            println!("  enabled_features: {:?} (from default)", validated.get_arr("enabled_features"));
            println!("  default_flags: {:?} (from default)", validated.get_arr("default_flags"));
        }
        Err(errors) => {
            println!("✗ Validation failed:");
            for error in errors {
                println!("  - {}", error);
            }
        }
    }
}
