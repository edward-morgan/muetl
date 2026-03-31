use muetl::{
    impl_config_template,
    task_defs::{ConfigTemplate, ConfigType, ConfigValue},
};
/// Test that the impl_config_template! macro supports map types
use std::collections::HashMap;

struct TestTask;

impl_config_template!(
    TestTask,
    optional_map: Map[Str],
    required_map: Map[Num]!,
    map_with_default: Map[Bool] = HashMap::from([("one", false), ("two", true)]),
    required_map_arr: Map[Arr[Str]]!,
    map_arr_with_default: Map[Arr[Num]] = HashMap::from([("one", vec![1, 2, 3]), ("two", vec![4])]),
);

#[test]
fn test_map_config_template() {
    let template = TestTask::config_template().expect("Should have template");

    // Check that we have 5 fields
    assert_eq!(template.fields.len(), 5);

    // Check optional_map field (Map[Str])
    let optional_field = &template.fields[0];
    assert_eq!(optional_field.name, "optional_map");
    assert!(!optional_field.required);
    assert!(matches!(
        optional_field.field_type,
        ConfigType::Map(ref inner) if matches!(**inner, ConfigType::Str)
    ));

    // Check required_map field (Map[Num]!)
    let required_field = &template.fields[1];
    assert_eq!(required_field.name, "required_map");
    assert!(required_field.required);
    assert!(matches!(
        required_field.field_type,
        ConfigType::Map(ref inner) if matches!(**inner, ConfigType::Num)
    ));

    // Check map_with_default field (Map[Bool] with default)
    let default_field = &template.fields[2];
    assert_eq!(default_field.name, "map_with_default");
    assert!(!default_field.required);
    assert!(matches!(
        default_field.field_type,
        ConfigType::Map(ref inner) if matches!(**inner, ConfigType::Bool)
    ));

    // Check the default value
    if let Some(ConfigValue::Map(map)) = &default_field.default {
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("one"), Some(&ConfigValue::Bool(false)));
        assert_eq!(map.get("two"), Some(&ConfigValue::Bool(true)));
    } else {
        panic!("Expected map default value");
    }

    // Check required_map_arr field (Map[Arr[Str]]!)
    let arr_field = &template.fields[3];
    assert_eq!(arr_field.name, "required_map_arr");
    assert!(arr_field.required);
    assert!(matches!(
        arr_field.field_type,
        ConfigType::Map(ref inner) if matches!(
            **inner,
            ConfigType::Arr(ref arr_inner) if matches!(**arr_inner, ConfigType::Str)
        )
    ));

    // Check map_arr_with_default field (Map[Arr[Num]] with default)
    let arr_default_field = &template.fields[4];
    assert_eq!(arr_default_field.name, "map_arr_with_default");
    assert!(!arr_default_field.required);
    assert!(matches!(
        arr_default_field.field_type,
        ConfigType::Map(ref inner) if matches!(
            **inner,
            ConfigType::Arr(ref arr_inner) if matches!(**arr_inner, ConfigType::Num)
        )
    ));

    // Check the default value
    if let Some(ConfigValue::Map(map)) = &arr_default_field.default {
        assert_eq!(map.len(), 2);
        // Check "one" -> [1, 2, 3]
        if let Some(ConfigValue::Arr(arr)) = map.get("one") {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], ConfigValue::Num(1));
            assert_eq!(arr[1], ConfigValue::Num(2));
            assert_eq!(arr[2], ConfigValue::Num(3));
        } else {
            panic!("Expected array value for 'one'");
        }
        // Check "two" -> [4]
        if let Some(ConfigValue::Arr(arr)) = map.get("two") {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], ConfigValue::Num(4));
        } else {
            panic!("Expected array value for 'two'");
        }
    } else {
        panic!("Expected map array default value");
    }
}

#[test]
fn test_map_validation() {
    let template = TestTask::config_template().expect("Should have template");

    // Test valid config with maps
    let mut config = HashMap::new();

    // Required Map[Num]!
    let mut num_map = HashMap::new();
    num_map.insert("count".to_string(), ConfigValue::Num(42));
    num_map.insert("limit".to_string(), ConfigValue::Num(100));
    config.insert("required_map".to_string(), ConfigValue::Map(num_map));

    // Optional Map[Str]
    let mut str_map = HashMap::new();
    str_map.insert("key1".to_string(), ConfigValue::Str("value1".to_string()));
    str_map.insert("key2".to_string(), ConfigValue::Str("value2".to_string()));
    config.insert("optional_map".to_string(), ConfigValue::Map(str_map));

    // Required Map[Arr[Str]]!
    let mut arr_map = HashMap::new();
    arr_map.insert(
        "tags".to_string(),
        ConfigValue::Arr(vec![
            ConfigValue::Str("tag1".to_string()),
            ConfigValue::Str("tag2".to_string()),
        ]),
    );
    arr_map.insert(
        "labels".to_string(),
        ConfigValue::Arr(vec![ConfigValue::Str("label1".to_string())]),
    );
    config.insert("required_map_arr".to_string(), ConfigValue::Map(arr_map));

    let result = template.validate(config);
    assert!(result.is_ok(), "Valid config should pass: {:?}", result);

    if let Ok(task_config) = result {
        // Verify we can retrieve the map values
        let num_map = task_config
            .get_map("required_map")
            .expect("Should have required_map");
        assert_eq!(num_map.get("count"), Some(&ConfigValue::Num(42)));

        // Verify default was applied
        let default_map = task_config
            .get_map("map_with_default")
            .expect("Should have default");
        assert_eq!(default_map.get("one"), Some(&ConfigValue::Bool(false)));
        assert_eq!(default_map.get("two"), Some(&ConfigValue::Bool(true)));
    }
}

#[test]
fn test_map_missing_required_field() {
    let template = TestTask::config_template().expect("Should have template");

    // Test missing required map field (required_map)
    let mut config = HashMap::new();

    // Provide required_map_arr but not required_map
    let mut arr_map = HashMap::new();
    arr_map.insert(
        "tags".to_string(),
        ConfigValue::Arr(vec![ConfigValue::Str("tag1".to_string())]),
    );
    config.insert("required_map_arr".to_string(), ConfigValue::Map(arr_map));

    let result = template.validate(config);
    assert!(result.is_err(), "Missing required field should fail");
    if let Err(errors) = result {
        assert!(errors.iter().any(|e| e.contains("required_map")));
    }
}

#[test]
fn test_map_type_mismatch() {
    let template = TestTask::config_template().expect("Should have template");

    // Test type mismatch in map values
    let mut config = HashMap::new();

    // required_map should contain numbers, but we provide strings
    let mut bad_map = HashMap::new();
    bad_map.insert(
        "count".to_string(),
        ConfigValue::Str("not_a_number".to_string()),
    );
    config.insert("required_map".to_string(), ConfigValue::Map(bad_map));

    // Provide required_map_arr correctly
    let mut arr_map = HashMap::new();
    arr_map.insert(
        "tags".to_string(),
        ConfigValue::Arr(vec![ConfigValue::Str("tag1".to_string())]),
    );
    config.insert("required_map_arr".to_string(), ConfigValue::Map(arr_map));

    let result = template.validate(config);
    assert!(result.is_err(), "Type mismatch in map should fail");
}

#[test]
fn test_map_arr_type_mismatch() {
    let template = TestTask::config_template().expect("Should have template");

    // Test type mismatch in map with array values
    let mut config = HashMap::new();

    // Provide required_map correctly
    let mut num_map = HashMap::new();
    num_map.insert("count".to_string(), ConfigValue::Num(42));
    config.insert("required_map".to_string(), ConfigValue::Map(num_map));

    // required_map_arr should contain Arr[Str], but we provide Arr[Num]
    let mut bad_arr_map = HashMap::new();
    bad_arr_map.insert(
        "tags".to_string(),
        ConfigValue::Arr(vec![ConfigValue::Num(123)]), // Wrong type in array!
    );
    config.insert(
        "required_map_arr".to_string(),
        ConfigValue::Map(bad_arr_map),
    );

    let result = template.validate(config);
    assert!(
        result.is_err(),
        "Type mismatch in map array values should fail"
    );
}

#[test]
fn test_map_default_value_applied() {
    let template = TestTask::config_template().expect("Should have template");

    // Provide only required fields, let defaults apply
    let mut config = HashMap::new();

    let mut num_map = HashMap::new();
    num_map.insert("count".to_string(), ConfigValue::Num(42));
    config.insert("required_map".to_string(), ConfigValue::Map(num_map));

    let mut arr_map = HashMap::new();
    arr_map.insert(
        "tags".to_string(),
        ConfigValue::Arr(vec![ConfigValue::Str("tag1".to_string())]),
    );
    config.insert("required_map_arr".to_string(), ConfigValue::Map(arr_map));

    let result = template.validate(config);
    assert!(result.is_ok(), "Should apply default values");

    if let Ok(task_config) = result {
        // Verify default was applied
        let default_map = task_config
            .get_map("map_with_default")
            .expect("Should have default");
        assert_eq!(default_map.len(), 2);
        assert_eq!(default_map.get("one"), Some(&ConfigValue::Bool(false)));
        assert_eq!(default_map.get("two"), Some(&ConfigValue::Bool(true)));
    }
}

#[test]
fn test_map_empty_map_is_valid() {
    let template = TestTask::config_template().expect("Should have template");

    // Test that empty maps are valid
    let mut config = HashMap::new();

    // Provide empty map for required fields
    config.insert("required_map".to_string(), ConfigValue::Map(HashMap::new()));
    config.insert(
        "required_map_arr".to_string(),
        ConfigValue::Map(HashMap::new()),
    );

    let result = template.validate(config);
    assert!(result.is_ok(), "Empty maps should be valid: {:?}", result);
}

#[test]
fn test_map_arr_with_default_applied() {
    let template = TestTask::config_template().expect("Should have template");

    // Provide only required fields, let map_arr_with_default apply
    let mut config = HashMap::new();

    let mut num_map = HashMap::new();
    num_map.insert("count".to_string(), ConfigValue::Num(42));
    config.insert("required_map".to_string(), ConfigValue::Map(num_map));

    let mut arr_map = HashMap::new();
    arr_map.insert(
        "tags".to_string(),
        ConfigValue::Arr(vec![ConfigValue::Str("tag1".to_string())]),
    );
    config.insert("required_map_arr".to_string(), ConfigValue::Map(arr_map));

    let result = template.validate(config);
    assert!(result.is_ok(), "Should apply default values");

    if let Ok(task_config) = result {
        // Verify map_arr_with_default was applied
        let arr_default_map = task_config
            .get_map("map_arr_with_default")
            .expect("Should have map_arr_with_default");
        assert_eq!(arr_default_map.len(), 2);

        // Check "one" -> [1, 2, 3]
        if let Some(ConfigValue::Arr(arr)) = arr_default_map.get("one") {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], ConfigValue::Num(1));
            assert_eq!(arr[1], ConfigValue::Num(2));
            assert_eq!(arr[2], ConfigValue::Num(3));
        } else {
            panic!("Expected array value for 'one'");
        }

        // Check "two" -> [4]
        if let Some(ConfigValue::Arr(arr)) = arr_default_map.get("two") {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], ConfigValue::Num(4));
        } else {
            panic!("Expected array value for 'two'");
        }
    }
}
