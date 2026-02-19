/// Test that the impl_config_template! macro supports enum types

use std::collections::HashMap;
use muetl::{
    impl_config_template,
    task_defs::{ConfigTemplate, ConfigType, ConfigValue},
};

struct TestTask;

impl_config_template!(
    TestTask,
    format: Enum["json", "csv"]!,
    mode: Enum["fast", "slow"] = "fast",
    level: Enum[Num: 0, 1, 2],
);

#[test]
fn test_enum_config_template() {
    let template = TestTask::config_template().expect("Should have template");
    assert_eq!(template.fields.len(), 3);

    // Check required enum field
    let format_field = &template.fields[0];
    assert_eq!(format_field.name, "format");
    assert!(format_field.required);
    assert!(format_field.default.is_none());
    match &format_field.field_type {
        ConfigType::Enum(choices) => {
            assert_eq!(choices.len(), 2);
            assert_eq!(choices[0], ConfigValue::Str("json".to_string()));
            assert_eq!(choices[1], ConfigValue::Str("csv".to_string()));
        }
        other => panic!("Expected Enum, got {:?}", other),
    }

    // Check enum with default
    let mode_field = &template.fields[1];
    assert_eq!(mode_field.name, "mode");
    assert!(!mode_field.required);
    assert_eq!(
        mode_field.default,
        Some(ConfigValue::Str("fast".to_string()))
    );
    match &mode_field.field_type {
        ConfigType::Enum(choices) => {
            assert_eq!(choices.len(), 2);
            assert_eq!(choices[0], ConfigValue::Str("fast".to_string()));
            assert_eq!(choices[1], ConfigValue::Str("slow".to_string()));
        }
        other => panic!("Expected Enum, got {:?}", other),
    }

    // Check optional typed enum without default
    let level_field = &template.fields[2];
    assert_eq!(level_field.name, "level");
    assert!(!level_field.required);
    assert!(level_field.default.is_none());
    match &level_field.field_type {
        ConfigType::Enum(choices) => {
            assert_eq!(choices.len(), 3);
            assert_eq!(choices[0], ConfigValue::Num(0));
            assert_eq!(choices[1], ConfigValue::Num(1));
            assert_eq!(choices[2], ConfigValue::Num(2));
        }
        other => panic!("Expected Enum, got {:?}", other),
    }
}

#[test]
fn test_enum_validation_accepts_valid() {
    let template = TestTask::config_template().unwrap();

    let mut config = HashMap::new();
    config.insert("format".to_string(), ConfigValue::Str("json".to_string()));

    let result = template.validate(config);
    assert!(result.is_ok(), "Valid enum value should pass: {:?}", result);

    let tc = result.unwrap();
    // default should be applied for mode
    assert_eq!(tc.get_str("mode"), Some("fast"));
}

#[test]
fn test_enum_validation_rejects_invalid_variant() {
    let template = TestTask::config_template().unwrap();

    let mut config = HashMap::new();
    config.insert("format".to_string(), ConfigValue::Str("xml".to_string()));

    let result = template.validate(config);
    assert!(result.is_err(), "Invalid enum value should fail");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.contains("format")));
}

#[test]
fn test_enum_validation_rejects_wrong_type() {
    let template = TestTask::config_template().unwrap();

    let mut config = HashMap::new();
    config.insert("format".to_string(), ConfigValue::Num(42));

    let result = template.validate(config);
    assert!(result.is_err(), "Wrong type for enum field should fail");
}

#[test]
fn test_enum_missing_required() {
    let template = TestTask::config_template().unwrap();

    let config = HashMap::new();
    let result = template.validate(config);
    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.contains("format")));
}

#[test]
fn test_typed_enum_validation_accepts_valid() {
    let template = TestTask::config_template().unwrap();

    let mut config = HashMap::new();
    config.insert("format".to_string(), ConfigValue::Str("json".to_string()));
    config.insert("level".to_string(), ConfigValue::Num(1));

    let result = template.validate(config);
    assert!(result.is_ok(), "Valid typed enum value should pass: {:?}", result);
}

#[test]
fn test_typed_enum_validation_rejects_invalid_variant() {
    let template = TestTask::config_template().unwrap();

    let mut config = HashMap::new();
    config.insert("format".to_string(), ConfigValue::Str("json".to_string()));
    config.insert("level".to_string(), ConfigValue::Num(99));

    let result = template.validate(config);
    assert!(result.is_err(), "Invalid typed enum variant should fail");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.contains("level")));
}

#[test]
fn test_typed_enum_validation_rejects_wrong_type() {
    let template = TestTask::config_template().unwrap();

    let mut config = HashMap::new();
    config.insert("format".to_string(), ConfigValue::Str("json".to_string()));
    config.insert("level".to_string(), ConfigValue::Str("high".to_string()));

    let result = template.validate(config);
    assert!(result.is_err(), "String value for Num enum should fail");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.contains("level")));
}

// Test enum as last field without trailing comma
struct LastFieldTask;

impl_config_template!(
    LastFieldTask,
    name: Str!,
    color: Enum["red", "blue"]
);

#[test]
fn test_enum_last_field_no_comma() {
    let template = LastFieldTask::config_template().unwrap();
    assert_eq!(template.fields.len(), 2);

    let color_field = &template.fields[1];
    assert_eq!(color_field.name, "color");
    assert!(!color_field.required);
    match &color_field.field_type {
        ConfigType::Enum(choices) => assert_eq!(choices.len(), 2),
        other => panic!("Expected Enum, got {:?}", other),
    }
}

// Test required enum as last field without trailing comma
struct RequiredLastTask;

impl_config_template!(
    RequiredLastTask,
    kind: Enum["a", "b"]!
);

#[test]
fn test_required_enum_last_field_no_comma() {
    let template = RequiredLastTask::config_template().unwrap();
    assert_eq!(template.fields.len(), 1);
    assert!(template.fields[0].required);
}

// Test enum with default as last field without trailing comma
struct DefaultLastTask;

impl_config_template!(
    DefaultLastTask,
    kind: Enum["x", "y"] = "x"
);

#[test]
fn test_default_enum_last_field_no_comma() {
    let template = DefaultLastTask::config_template().unwrap();
    assert_eq!(template.fields.len(), 1);
    assert!(!template.fields[0].required);
    assert_eq!(
        template.fields[0].default,
        Some(ConfigValue::Str("x".to_string()))
    );
}

// Test typed enum as last field without trailing comma
struct TypedLastFieldTask;

impl_config_template!(
    TypedLastFieldTask,
    name: Str!,
    priority: Enum[Num: 1, 2, 3]
);

#[test]
fn test_typed_enum_last_field_no_comma() {
    let template = TypedLastFieldTask::config_template().unwrap();
    assert_eq!(template.fields.len(), 2);

    let priority_field = &template.fields[1];
    assert_eq!(priority_field.name, "priority");
    assert!(!priority_field.required);
    match &priority_field.field_type {
        ConfigType::Enum(choices) => {
            assert_eq!(choices.len(), 3);
            assert_eq!(choices[0], ConfigValue::Num(1));
            assert_eq!(choices[1], ConfigValue::Num(2));
            assert_eq!(choices[2], ConfigValue::Num(3));
        }
        other => panic!("Expected Enum, got {:?}", other),
    }
}

// Test required typed enum as last field without trailing comma
struct TypedRequiredLastTask;

impl_config_template!(
    TypedRequiredLastTask,
    count: Enum[Num: 10, 20]!
);

#[test]
fn test_typed_required_enum_last_field_no_comma() {
    let template = TypedRequiredLastTask::config_template().unwrap();
    assert_eq!(template.fields.len(), 1);
    assert!(template.fields[0].required);
    match &template.fields[0].field_type {
        ConfigType::Enum(choices) => {
            assert_eq!(choices[0], ConfigValue::Num(10));
            assert_eq!(choices[1], ConfigValue::Num(20));
        }
        other => panic!("Expected Enum, got {:?}", other),
    }
}

// Test typed enum with default as last field without trailing comma
struct TypedDefaultLastTask;

impl_config_template!(
    TypedDefaultLastTask,
    retries: Enum[Num: 0, 1, 3] = 1
);

#[test]
fn test_typed_default_enum_last_field_no_comma() {
    let template = TypedDefaultLastTask::config_template().unwrap();
    assert_eq!(template.fields.len(), 1);
    assert!(!template.fields[0].required);
    assert_eq!(template.fields[0].default, Some(ConfigValue::Num(1)));
    match &template.fields[0].field_type {
        ConfigType::Enum(choices) => {
            assert_eq!(choices.len(), 3);
            assert_eq!(choices[0], ConfigValue::Num(0));
        }
        other => panic!("Expected Enum, got {:?}", other),
    }
}
