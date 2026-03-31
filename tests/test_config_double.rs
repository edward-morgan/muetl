/// Test that the impl_config_template! macro supports Double type

use std::collections::HashMap;
use muetl::{
    impl_config_template,
    task_defs::{ConfigTemplate, ConfigType, ConfigValue},
};

struct TestTask;

impl_config_template!(
    TestTask,
    threshold: Double!,
    alpha: Double = 0.05,
    beta: Double,
    weights: Arr[Double] = vec![0.1, 0.2, 0.3],
    scores: Map[Double],
    matrix: Map[Arr[Double]]!,
);

#[test]
fn test_double_config_template() {
    let template = TestTask::config_template().expect("Should have template");
    assert_eq!(template.fields.len(), 6);

    // Check required double field
    let threshold_field = &template.fields[0];
    assert_eq!(threshold_field.name, "threshold");
    assert!(threshold_field.required);
    assert!(threshold_field.default.is_none());
    assert!(matches!(threshold_field.field_type, ConfigType::Double));

    // Check double with default
    let alpha_field = &template.fields[1];
    assert_eq!(alpha_field.name, "alpha");
    assert!(!alpha_field.required);
    assert!(alpha_field.default.is_some());
    if let Some(ConfigValue::Double(val)) = &alpha_field.default {
        assert_eq!(*val, 0.05);
    } else {
        panic!("Expected default to be Double(0.05)");
    }
    assert!(matches!(alpha_field.field_type, ConfigType::Double));

    // Check optional double without default
    let beta_field = &template.fields[2];
    assert_eq!(beta_field.name, "beta");
    assert!(!beta_field.required);
    assert!(beta_field.default.is_none());
    assert!(matches!(beta_field.field_type, ConfigType::Double));

    // Check array of doubles with default
    let weights_field = &template.fields[3];
    assert_eq!(weights_field.name, "weights");
    assert!(!weights_field.required);
    assert!(weights_field.default.is_some());
    if let Some(ConfigValue::Arr(arr)) = &weights_field.default {
        assert_eq!(arr.len(), 3);
        assert!(matches!(arr[0], ConfigValue::Double(_)));
    } else {
        panic!("Expected default to be Arr");
    }

    // Check optional map of doubles
    let scores_field = &template.fields[4];
    assert_eq!(scores_field.name, "scores");
    assert!(!scores_field.required);
    assert!(scores_field.default.is_none());
    if let ConfigType::Map(inner) = &scores_field.field_type {
        assert!(matches!(**inner, ConfigType::Double));
    } else {
        panic!("Expected Map type");
    }

    // Check required map of arrays of doubles
    let matrix_field = &template.fields[5];
    assert_eq!(matrix_field.name, "matrix");
    assert!(matrix_field.required);
    assert!(matrix_field.default.is_none());
    if let ConfigType::Map(inner) = &matrix_field.field_type {
        if let ConfigType::Arr(inner2) = inner.as_ref() {
            assert!(matches!(**inner2, ConfigType::Double));
        } else {
            panic!("Expected Arr inside Map");
        }
    } else {
        panic!("Expected Map type");
    }
}

#[test]
fn test_double_validation_accepts_valid() {
    let template = TestTask::config_template().unwrap();

    let mut config = HashMap::new();
    config.insert("threshold".to_string(), ConfigValue::Double(0.75));

    let mut matrix = HashMap::new();
    matrix.insert(
        "layer1".to_string(),
        ConfigValue::Arr(vec![
            ConfigValue::Double(1.0),
            ConfigValue::Double(2.0),
        ]),
    );
    config.insert("matrix".to_string(), ConfigValue::Map(matrix));

    let result = template.validate(config);
    assert!(result.is_ok(), "Valid double values should pass: {:?}", result);

    let tc = result.unwrap();

    // Check that the double was stored correctly
    let threshold = tc.get_f64("threshold").expect("threshold should be present");
    assert_eq!(threshold, 0.75);

    // Check default was applied
    let alpha = tc.get_f64("alpha").expect("alpha default should be present");
    assert_eq!(alpha, 0.05);

    // Check array default was applied
    let weights = tc.get_f64_vec("weights").expect("weights default should be present");
    assert_eq!(weights, vec![0.1, 0.2, 0.3]);

    // Check matrix was stored correctly
    let matrix = tc.get_f64_vec_map("matrix").expect("matrix should be present");
    assert_eq!(matrix.get("layer1"), Some(&vec![1.0, 2.0]));
}

#[test]
fn test_double_validation_rejects_wrong_type() {
    let template = TestTask::config_template().unwrap();

    let mut config = HashMap::new();
    config.insert("threshold".to_string(), ConfigValue::Num(42)); // Wrong type - should be Double
    config.insert("matrix".to_string(), ConfigValue::Map(HashMap::new()));

    let result = template.validate(config);
    assert!(result.is_err(), "Wrong type for double field should fail");
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.contains("threshold")));
}

#[test]
fn test_double_missing_required() {
    let template = TestTask::config_template().unwrap();

    let mut config = HashMap::new();
    config.insert("matrix".to_string(), ConfigValue::Map(HashMap::new()));

    let result = template.validate(config);
    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.contains("threshold")));
}

#[test]
fn test_double_getter_methods() {
    let template = TestTask::config_template().unwrap();

    let mut config = HashMap::new();
    config.insert("threshold".to_string(), ConfigValue::Double(0.95));
    config.insert("beta".to_string(), ConfigValue::Double(0.001));

    let mut matrix = HashMap::new();
    matrix.insert(
        "layer1".to_string(),
        ConfigValue::Arr(vec![ConfigValue::Double(1.5), ConfigValue::Double(2.5)]),
    );
    config.insert("matrix".to_string(), ConfigValue::Map(matrix));

    let tc = template.validate(config).unwrap();

    // Test get_f64
    let threshold = tc.get_f64("threshold").unwrap();
    assert_eq!(threshold, 0.95);

    // Test require_f64
    let beta = tc.require_f64("beta");
    assert_eq!(beta, 0.001);

    // Test get_f64_vec (from default)
    let weights = tc.get_f64_vec("weights").unwrap();
    assert_eq!(weights.len(), 3);

    // Test get_f64_vec_map
    let matrix = tc.get_f64_vec_map("matrix").unwrap();
    assert_eq!(matrix.get("layer1"), Some(&vec![1.5, 2.5]));

    // Test None for missing optional field
    assert!(tc.get_f64("nonexistent").is_none());
}

#[test]
fn test_double_array_validation() {
    let template = TestTask::config_template().unwrap();

    let mut config = HashMap::new();
    config.insert("threshold".to_string(), ConfigValue::Double(0.5));
    config.insert(
        "weights".to_string(),
        ConfigValue::Arr(vec![
            ConfigValue::Double(0.1),
            ConfigValue::Double(0.2),
            ConfigValue::Double(0.3),
            ConfigValue::Double(0.4),
        ]),
    );
    config.insert("matrix".to_string(), ConfigValue::Map(HashMap::new()));

    let result = template.validate(config);
    assert!(result.is_ok());

    let tc = result.unwrap();
    let weights = tc.get_f64_vec("weights").unwrap();
    assert_eq!(weights.len(), 4);
    assert_eq!(weights[3], 0.4);
}

#[test]
fn test_double_array_rejects_mixed_types() {
    let template = TestTask::config_template().unwrap();

    let mut config = HashMap::new();
    config.insert("threshold".to_string(), ConfigValue::Double(0.5));
    config.insert(
        "weights".to_string(),
        ConfigValue::Arr(vec![
            ConfigValue::Double(0.1),
            ConfigValue::Num(42), // Wrong type in array
        ]),
    );
    config.insert("matrix".to_string(), ConfigValue::Map(HashMap::new()));

    let result = template.validate(config);
    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.contains("weights")));
}

#[test]
fn test_double_map_validation() {
    let template = TestTask::config_template().unwrap();

    let mut config = HashMap::new();
    config.insert("threshold".to_string(), ConfigValue::Double(0.5));

    let mut scores = HashMap::new();
    scores.insert("accuracy".to_string(), ConfigValue::Double(0.95));
    scores.insert("precision".to_string(), ConfigValue::Double(0.87));
    config.insert("scores".to_string(), ConfigValue::Map(scores));

    config.insert("matrix".to_string(), ConfigValue::Map(HashMap::new()));

    let result = template.validate(config);
    assert!(result.is_ok());

    let tc = result.unwrap();
    let scores = tc.get_f64_map("scores").unwrap();
    assert_eq!(scores.get("accuracy"), Some(&0.95));
    assert_eq!(scores.get("precision"), Some(&0.87));
}

#[test]
fn test_double_map_rejects_wrong_value_type() {
    let template = TestTask::config_template().unwrap();

    let mut config = HashMap::new();
    config.insert("threshold".to_string(), ConfigValue::Double(0.5));

    let mut scores = HashMap::new();
    scores.insert("accuracy".to_string(), ConfigValue::Double(0.95));
    scores.insert("count".to_string(), ConfigValue::Num(42)); // Wrong type in map
    config.insert("scores".to_string(), ConfigValue::Map(scores));

    config.insert("matrix".to_string(), ConfigValue::Map(HashMap::new()));

    let result = template.validate(config);
    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.contains("scores")));
}

#[test]
fn test_double_special_values() {
    let template = TestTask::config_template().unwrap();

    let mut config = HashMap::new();
    config.insert("threshold".to_string(), ConfigValue::Double(0.0)); // Zero
    config.insert("beta".to_string(), ConfigValue::Double(-1.5)); // Negative
    config.insert("matrix".to_string(), ConfigValue::Map(HashMap::new()));

    let result = template.validate(config);
    assert!(result.is_ok(), "Zero and negative doubles should be valid");

    let tc = result.unwrap();
    assert_eq!(tc.get_f64("threshold").unwrap(), 0.0);
    assert_eq!(tc.get_f64("beta").unwrap(), -1.5);
}

// Test double as last field without trailing comma
struct LastFieldTask;

impl_config_template!(
    LastFieldTask,
    name: Str!,
    value: Double
);

#[test]
fn test_double_last_field_no_comma() {
    let template = LastFieldTask::config_template().unwrap();
    assert_eq!(template.fields.len(), 2);

    let value_field = &template.fields[1];
    assert_eq!(value_field.name, "value");
    assert!(!value_field.required);
    assert!(matches!(value_field.field_type, ConfigType::Double));
}

// Test required double as last field without trailing comma
struct RequiredLastTask;

impl_config_template!(
    RequiredLastTask,
    temperature: Double!
);

#[test]
fn test_required_double_last_field_no_comma() {
    let template = RequiredLastTask::config_template().unwrap();
    assert_eq!(template.fields.len(), 1);
    assert!(template.fields[0].required);
    assert!(matches!(template.fields[0].field_type, ConfigType::Double));
}

// Test double with default as last field without trailing comma
struct DefaultLastTask;

impl_config_template!(
    DefaultLastTask,
    rate: Double = 0.001
);

#[test]
fn test_default_double_last_field_no_comma() {
    let template = DefaultLastTask::config_template().unwrap();
    assert_eq!(template.fields.len(), 1);
    assert!(!template.fields[0].required);
    assert!(template.fields[0].default.is_some());
    if let Some(ConfigValue::Double(val)) = &template.fields[0].default {
        assert_eq!(*val, 0.001);
    } else {
        panic!("Expected default to be Double(0.001)");
    }
}

// Test array of doubles as last field
struct ArrayLastTask;

impl_config_template!(
    ArrayLastTask,
    coefficients: Arr[Double] = vec![1.0, 2.0, 3.0]
);

#[test]
fn test_array_double_last_field_no_comma() {
    let template = ArrayLastTask::config_template().unwrap();
    assert_eq!(template.fields.len(), 1);

    let field = &template.fields[0];
    assert_eq!(field.name, "coefficients");
    assert!(!field.required);
    assert!(field.default.is_some());

    if let ConfigType::Arr(inner) = &field.field_type {
        assert!(matches!(**inner, ConfigType::Double));
    } else {
        panic!("Expected Arr type");
    }
}

// Test map of doubles as last field
struct MapLastTask;

impl_config_template!(
    MapLastTask,
    metrics: Map[Double]
);

#[test]
fn test_map_double_last_field_no_comma() {
    let template = MapLastTask::config_template().unwrap();
    assert_eq!(template.fields.len(), 1);

    let field = &template.fields[0];
    assert_eq!(field.name, "metrics");
    assert!(!field.required);

    if let ConfigType::Map(inner) = &field.field_type {
        assert!(matches!(**inner, ConfigType::Double));
    } else {
        panic!("Expected Map type");
    }
}
