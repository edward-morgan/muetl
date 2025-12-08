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

pub struct Registry {
    defs: Vec<Arc<TaskInfo>>,
}

impl Registry {
    pub fn new() -> Self {
        Self { defs: vec![] }
    }

    pub fn add_def(&mut self, def: TaskInfo) {
        // TODO: Validate incoming TaskInfo
        self.defs.push(Arc::new(def));
    }

    /// Register a SelfDescribing TaskDef with the registry.
    /// This is a convenience method that calls T::task_info() and adds it to the registry.
    pub fn register<T: SelfDescribing>(&mut self) {
        self.add_def(T::task_info());
    }

    pub fn def_for(&self, name: &String) -> Option<Arc<TaskInfo>> {
        for def in &self.defs {
            if def.task_id == *name {
                return Some(def.clone());
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test() {}
}
