use std::{any::TypeId, collections::HashMap, sync::Arc};

use crate::{
    runtime::connection::IncomingConnections,
    task_defs::{TaskConfig, TaskConfigTpl},
};

use crate::task_defs::{operator::Operator, sink::Sink, source::Source};

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

impl TaskDefInfo {
    pub fn validate(
        &self,
        inputs: &IncomingConnections,
        config: &TaskConfig,
    ) -> Result<(), Vec<String>> {
        let errors = vec![];

        if errors.len() > 0 {
            Err(errors)
        } else {
            Ok(())
        }
    }
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

    pub fn def_for(&self, name: &String) -> Option<Arc<TaskInfo>> {
        for def in &self.defs {
            if def.task_id == *name {
                return Some(def.clone());
            }
        }
        None
    }

    // Finds a sink with the given name; if it isn't found, Ok(None) is returned. If it is found,
    // attempt to build it; if that errors, return Err; otherwise, return Ok(Some(sink)).
    // pub fn build_def_for(
    //     &self,
    //     name: &String,
    //     config_tpl: &TaskConfig,
    // ) -> Result<Option<Box<dyn Sink>>, String> {
    //     for def in &self.defs {
    //         if def.name == *name {
    //             let builder = match def.info {
    //                 TaskDefInfo::DaemonDef { build_daemon, .. } => match build_daemon(config_tpl) {
    //                     Ok(s) => Ok(Some(s)),
    //                     Err(e) => Err(format!("failed to construct sink named {}: {}", name, e)),
    //                 },
    //                 TaskDefInfo::SinkDef { build_sink, .. } => match build_sink(config_tpl) {
    //                     Ok(s) => Ok(Some(s)),
    //                     Err(e) => Err(format!("failed to construct sink named {}: {}", name, e)),
    //                 },
    //             };
    //         }
    //     }
    //     Ok(None)
    // }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test() {}
}
