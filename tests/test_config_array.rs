/// Test that the impl_config_template! macro supports array types

use std::collections::HashMap;
use muetl::{
    impl_config_template,
    task_defs::{ConfigTemplate, ConfigType, ConfigValue},
};

struct TestTask;

impl_config_template!(
    TestTask,
    required_array: Arr[Str]!,
    optional_array: Arr[Num],
    array_with_default: Arr[Bool] = vec![true, false],
);

#[test]
fn test_array_config_template() {
    let template = TestTask::config_template().expect("Should have template");

    // Check that we have 3 fields
    assert_eq!(template.fields.len(), 3);

    // Check required_array field
    let required_field = &template.fields[0];
    assert_eq!(required_field.name, "required_array");
    assert!(required_field.required);
    assert!(matches!(
        required_field.field_type,
        ConfigType::Arr(ref inner) if matches!(**inner, ConfigType::Str)
    ));

    // Check optional_array field
    let optional_field = &template.fields[1];
    assert_eq!(optional_field.name, "optional_array");
    assert!(!optional_field.required);
    assert!(matches!(
        optional_field.field_type,
        ConfigType::Arr(ref inner) if matches!(**inner, ConfigType::Num)
    ));

    // Check array_with_default field
    let default_field = &template.fields[2];
    assert_eq!(default_field.name, "array_with_default");
    assert!(!default_field.required);
    assert!(matches!(
        default_field.field_type,
        ConfigType::Arr(ref inner) if matches!(**inner, ConfigType::Bool)
    ));

    // Check the default value
    if let Some(ConfigValue::Arr(arr)) = &default_field.default {
        assert_eq!(arr.len(), 2);
        assert!(matches!(arr[0], ConfigValue::Bool(true)));
        assert!(matches!(arr[1], ConfigValue::Bool(false)));
    } else {
        panic!("Expected array default value");
    }
}

#[test]
fn test_array_validation() {
    let template = TestTask::config_template().expect("Should have template");

    // Test valid config with arrays
    let mut config = HashMap::new();
    config.insert(
        "required_array".to_string(),
        ConfigValue::Arr(vec![
            ConfigValue::Str("hello".to_string()),
            ConfigValue::Str("world".to_string()),
        ])
    );
    config.insert(
        "optional_array".to_string(),
        ConfigValue::Arr(vec![
            ConfigValue::Num(1),
            ConfigValue::Num(2),
            ConfigValue::Num(3),
        ])
    );

    let result = template.validate(config);
    assert!(result.is_ok(), "Valid config should pass: {:?}", result);

    // Test missing required array field
    let mut config = HashMap::new();
    config.insert(
        "optional_array".to_string(),
        ConfigValue::Arr(vec![ConfigValue::Num(42)])
    );

    let result = template.validate(config);
    assert!(result.is_err(), "Missing required field should fail");
    if let Err(errors) = result {
        assert!(errors.iter().any(|e| e.contains("required_array")));
    }

    // Test type mismatch in array
    let mut config = HashMap::new();
    config.insert(
        "required_array".to_string(),
        ConfigValue::Arr(vec![
            ConfigValue::Str("valid".to_string()),
            ConfigValue::Num(123), // Wrong type!
        ])
    );

    let result = template.validate(config);
    assert!(result.is_err(), "Type mismatch in array should fail");
}
