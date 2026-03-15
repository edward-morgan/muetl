use std::{any::TypeId, collections::HashMap, sync::Arc};

use crate::task_defs::{TaskConfig, TaskConfigTpl};

use crate::task_defs::{operator::Operator, sink::Sink, source::Source};

/// A trait for TaskDefs that can describe themselves for automatic registration.
/// Implementors provide their TaskInfo, including task_id, config template, and I/O types.
pub trait SelfDescribing {
    fn task_info() -> TaskInfo;
}

#[derive(Debug)]
pub struct TaskInfo {
    pub task_id: String,
    pub config_tpl: Option<TaskConfigTpl>,
    pub info: TaskDefInfo,
}
#[derive(Debug)]
pub enum TaskDefInfo {
    SourceDef {
        outputs: HashMap<String, Vec<TypeId>>,
        build_source: fn(&TaskConfig) -> Result<Box<dyn Source>, String>,
    },
    SinkDef {
        inputs: HashMap<String, Vec<TypeId>>,
        build_sink: fn(&TaskConfig) -> Result<Box<dyn Sink>, String>,
    },
    OperatorDef {
        inputs: HashMap<String, Vec<TypeId>>,
        outputs: HashMap<String, Vec<TypeId>>,
        build_operator: fn(&TaskConfig) -> Result<Box<dyn Operator>, String>,
    },
}

/// A registry of TaskDefs that is used by the muetl runtime when instantiating Tasks from a Flow.
///
/// Note that this is **not** the same thing as a runtime registry; Registry is a storefront for
/// advertising the kinds of Tasks that may be started; it doesn't keep any information about
/// what is running at any given time.
pub struct Registry {
    defs: Vec<Arc<TaskInfo>>,
}

impl Registry {
    pub fn new() -> Self {
        Self { defs: vec![] }
    }

    pub fn add_def(&mut self, def: TaskInfo) {
        self.defs.push(Arc::new(def));
    }

    /// Register a SelfDescribing TaskDef with the registry.
    /// This is a convenience method that calls T::task_info() and adds it to the registry.
    pub fn register<T: SelfDescribing>(&mut self) {
        self.add_def(T::task_info());
    }

    pub fn def_for(&self, task_id: &String) -> Option<Arc<TaskInfo>> {
        for def in &self.defs {
            if def.task_id == *task_id {
                return Some(def.clone());
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_source_info(task_id: &str) -> TaskInfo {
        TaskInfo {
            task_id: task_id.to_string(),
            config_tpl: None,
            info: TaskDefInfo::SourceDef {
                outputs: HashMap::new(),
                build_source: |_| Err("stub".to_string()),
            },
        }
    }

    fn make_sink_info(task_id: &str) -> TaskInfo {
        TaskInfo {
            task_id: task_id.to_string(),
            config_tpl: None,
            info: TaskDefInfo::SinkDef {
                inputs: HashMap::new(),
                build_sink: |_| Err("stub".to_string()),
            },
        }
    }

    #[test]
    fn new_registry_is_empty() {
        let reg = Registry::new();
        assert!(reg.defs.is_empty());
    }

    #[test]
    fn add_def_stores_task_info() {
        let mut reg = Registry::new();
        reg.add_def(make_source_info("my_source"));

        assert_eq!(reg.defs.len(), 1);
        assert_eq!(reg.defs[0].task_id, "my_source");
    }

    #[test]
    fn add_def_multiple() {
        let mut reg = Registry::new();
        reg.add_def(make_source_info("source_a"));
        reg.add_def(make_sink_info("sink_b"));
        reg.add_def(make_source_info("source_c"));

        assert_eq!(reg.defs.len(), 3);
        assert_eq!(reg.defs[0].task_id, "source_a");
        assert_eq!(reg.defs[1].task_id, "sink_b");
        assert_eq!(reg.defs[2].task_id, "source_c");
    }

    #[test]
    fn def_for_returns_matching_def() {
        let mut reg = Registry::new();
        reg.add_def(make_source_info("alpha"));
        reg.add_def(make_sink_info("beta"));

        let result = reg.def_for(&"beta".to_string());
        assert!(result.is_some());
        assert_eq!(result.unwrap().task_id, "beta");
    }

    #[test]
    fn def_for_returns_none_when_not_found() {
        let mut reg = Registry::new();
        reg.add_def(make_source_info("alpha"));

        let result = reg.def_for(&"nonexistent".to_string());
        assert!(result.is_none());
    }

    #[test]
    fn def_for_on_empty_registry() {
        let reg = Registry::new();
        assert!(reg.def_for(&"anything".to_string()).is_none());
    }

    #[test]
    fn def_for_returns_first_match_on_duplicate_ids() {
        let mut reg = Registry::new();
        reg.add_def(make_source_info("dup"));
        reg.add_def(make_sink_info("dup"));

        let result = reg.def_for(&"dup".to_string()).unwrap();
        assert!(matches!(result.info, TaskDefInfo::SourceDef { .. }));
    }

    #[test]
    fn def_for_returns_arc() {
        let mut reg = Registry::new();
        reg.add_def(make_source_info("shared"));

        let a = reg.def_for(&"shared".to_string()).unwrap();
        let b = reg.def_for(&"shared".to_string()).unwrap();
        assert!(Arc::ptr_eq(&a, &b));
    }

    struct FakeOperator;

    impl SelfDescribing for FakeOperator {
        fn task_info() -> TaskInfo {
            TaskInfo {
                task_id: "fake_op".to_string(),
                config_tpl: None,
                info: TaskDefInfo::OperatorDef {
                    inputs: HashMap::new(),
                    outputs: HashMap::new(),
                    build_operator: |_| Err("stub".to_string()),
                },
            }
        }
    }

    #[test]
    fn register_uses_self_describing() {
        let mut reg = Registry::new();
        reg.register::<FakeOperator>();

        assert_eq!(reg.defs.len(), 1);
        let def = reg.def_for(&"fake_op".to_string()).unwrap();
        assert_eq!(def.task_id, "fake_op");
        assert!(matches!(def.info, TaskDefInfo::OperatorDef { .. }));
    }
}
