/// Test that the impl_config_template! macro supports UtcDateTime type

use std::collections::HashMap;
use chrono::{Datelike, Utc};
use muetl::{
    impl_config_template,
    task_defs::{ConfigTemplate, ConfigType, ConfigValue},
};

struct TestTask;

impl_config_template!(
    TestTask,
    start_time: UtcDateTime!,
    end_time: UtcDateTime = "2024-12-31T23:59:59Z",
    created_at: UtcDateTime,
);

#[test]
fn test_datetime_config_template() {
    let template = TestTask::config_template().expect("Should have template");
    assert_eq!(template.fields.len(), 3);

    // Check required datetime field
    let start_field = &template.fields[0];
    assert_eq!(start_field.name, "start_time");
    assert!(start_field.required);
    assert!(start_field.default.is_none());
    assert!(matches!(start_field.field_type, ConfigType::UtcDateTime));

    // Check datetime with default
    let end_field = &template.fields[1];
    assert_eq!(end_field.name, "end_time");
    assert!(!end_field.required);
    assert!(end_field.default.is_some());
    assert!(matches!(end_field.field_type, ConfigType::UtcDateTime));

    // Check optional datetime without default
    let created_field = &template.fields[2];
    assert_eq!(created_field.name, "created_at");
    assert!(!created_field.required);
    assert!(created_field.default.is_none());
    assert!(matches!(created_field.field_type, ConfigType::UtcDateTime));
}

#[test]
fn test_datetime_validation_accepts_valid() {
    let template = TestTask::config_template().unwrap();

    let mut config = HashMap::new();
    config.insert("start_time".to_string(), ConfigValue::Str("2024-01-01T00:00:00Z".to_string()));

    let result = template.validate(config);
    assert!(result.is_ok(), "Valid datetime string should pass: {:?}", result);

    let tc = result.unwrap();
    // Check that the datetime was parsed and stored correctly
    let start_time = tc.get_utc_datetime("start_time").expect("start_time should be present");
    assert_eq!(start_time.to_rfc3339(), "2024-01-01T00:00:00+00:00");

    // Check default was applied and converted
    let end_time = tc.get_utc_datetime("end_time").expect("end_time default should be present");
    assert_eq!(end_time.to_rfc3339(), "2024-12-31T23:59:59+00:00");
}

#[test]
fn test_datetime_validation_rejects_invalid_format() {
    let template = TestTask::config_template().unwrap();

    let mut config = HashMap::new();
    config.insert("start_time".to_string(), ConfigValue::Str("not-a-datetime".to_string()));

    let result = template.validate(config);
    assert!(result.is_err(), "Invalid datetime format should fail");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.contains("start_time") && e.contains("invalid datetime format")));
}

#[test]
fn test_datetime_validation_rejects_wrong_type() {
    let template = TestTask::config_template().unwrap();

    let mut config = HashMap::new();
    config.insert("start_time".to_string(), ConfigValue::Num(42));

    let result = template.validate(config);
    assert!(result.is_err(), "Wrong type for datetime field should fail");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.contains("start_time")));
}

#[test]
fn test_datetime_missing_required() {
    let template = TestTask::config_template().unwrap();

    let config = HashMap::new();
    let result = template.validate(config);
    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.contains("start_time")));
}

#[test]
fn test_datetime_getter_methods() {
    let template = TestTask::config_template().unwrap();

    let mut config = HashMap::new();
    config.insert("start_time".to_string(), ConfigValue::Str("2024-06-15T12:30:45Z".to_string()));
    config.insert("created_at".to_string(), ConfigValue::Str("2024-03-17T10:00:00Z".to_string()));

    let tc = template.validate(config).unwrap();

    // Test get_utc_datetime
    let start = tc.get_utc_datetime("start_time").unwrap();
    assert_eq!(start.year(), 2024);
    assert_eq!(start.month(), 6);
    assert_eq!(start.day(), 15);

    // Test require_utc_datetime
    let created = tc.require_utc_datetime("created_at");
    assert_eq!(created.year(), 2024);
    assert_eq!(created.month(), 3);

    // Test None for missing optional field
    assert!(tc.get_utc_datetime("nonexistent").is_none());
}

#[test]
fn test_datetime_various_rfc3339_formats() {
    let template = TestTask::config_template().unwrap();

    // Test with timezone offset
    let mut config = HashMap::new();
    config.insert("start_time".to_string(), ConfigValue::Str("2024-01-01T12:00:00+05:30".to_string()));

    let result = template.validate(config);
    assert!(result.is_ok(), "RFC3339 with timezone offset should work");

    let tc = result.unwrap();
    let dt = tc.get_utc_datetime("start_time").unwrap();
    // Should be converted to UTC
    assert_eq!(dt.timezone(), Utc);
}

// Test datetime as last field without trailing comma
struct LastFieldTask;

impl_config_template!(
    LastFieldTask,
    name: Str!,
    timestamp: UtcDateTime
);

#[test]
fn test_datetime_last_field_no_comma() {
    let template = LastFieldTask::config_template().unwrap();
    assert_eq!(template.fields.len(), 2);

    let timestamp_field = &template.fields[1];
    assert_eq!(timestamp_field.name, "timestamp");
    assert!(!timestamp_field.required);
    assert!(matches!(timestamp_field.field_type, ConfigType::UtcDateTime));
}

// Test required datetime as last field without trailing comma
struct RequiredLastTask;

impl_config_template!(
    RequiredLastTask,
    scheduled_at: UtcDateTime!
);

#[test]
fn test_required_datetime_last_field_no_comma() {
    let template = RequiredLastTask::config_template().unwrap();
    assert_eq!(template.fields.len(), 1);
    assert!(template.fields[0].required);
    assert!(matches!(template.fields[0].field_type, ConfigType::UtcDateTime));
}

// Test datetime with default as last field without trailing comma
struct DefaultLastTask;

impl_config_template!(
    DefaultLastTask,
    expires_at: UtcDateTime = "2025-01-01T00:00:00Z"
);

#[test]
fn test_default_datetime_last_field_no_comma() {
    let template = DefaultLastTask::config_template().unwrap();
    assert_eq!(template.fields.len(), 1);
    assert!(!template.fields[0].required);
    assert!(template.fields[0].default.is_some());
}
