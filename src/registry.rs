use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::Arc,
};

use crate::{
    runtime::connection::{IncomingConnections, OutgoingConnections},
    task_defs::{HasInputs, HasOutputs, TaskConfig, TaskConfigTpl, TaskDef},
};

use crate::task_defs::{daemon::Daemon, sink::Sink, MuetlSinkContext, OutputType};

pub struct DaemonDef {
    pub name: String,
    pub inputs: Option<HashMap<String, Vec<TypeId>>>,
    pub config_tpl: Option<TaskConfigTpl>,
    pub build_daemon: fn(&TaskConfig) -> Result<Box<dyn Sink>, String>,
}

pub struct SinkDef {
    pub name: String,
    pub inputs: Option<HashMap<String, Vec<TypeId>>>,
    pub config_tpl: Option<TaskConfigTpl>,
    pub build_sink: fn(&TaskConfig) -> Result<Box<dyn Sink>, String>,
}

impl SinkDef {
    pub fn validate(
        &self,
        inputs: &IncomingConnections,
        config: &TaskConfig,
    ) -> Result<(), Vec<String>> {
        let mut errors = vec![];

        if errors.len() > 0 {
            Err(errors)
        } else {
            Ok(())
        }
    }
}

pub struct Registry {
    daemons: Vec<DaemonDef>,
    sinks: Vec<SinkDef>,
}

impl Registry {
    pub fn add_sink(&mut self, sink_def: SinkDef) {
        self.sinks.push(sink_def);
    }
    pub fn add_daemon(&mut self, daemon_def: DaemonDef) {
        self.daemons.push(daemon_def);
    }

    /// Finds a sink with the given name; if it isn't found, Ok(None) is returned. If it is found,
    /// attempt to build it; if that errors, return Err; otherwise, return Ok(Some(sink)).
    pub fn sink_for(
        &self,
        name: &String,
        config_tpl: &TaskConfig,
    ) -> Result<Option<Box<dyn Sink>>, String> {
        for def in &self.sinks {
            if def.name == *name {
                return match (def.build_sink)(config_tpl) {
                    Ok(s) => Ok(Some(s)),
                    Err(e) => Err(format!("failed to construct sink named {}: {}", name, e)),
                };
            }
        }
        Ok(None)
    }
    pub fn daemon_for(
        &self,
        name: &String,
        config_tpl: &TaskConfig,
    ) -> Result<Option<Box<dyn Sink>>, String> {
        for def in &self.daemons {
            if def.name == *name {
                return match (def.build_daemon)(config_tpl) {
                    Ok(s) => Ok(Some(s)),
                    Err(e) => Err(format!("failed to construct sink named {}: {}", name, e)),
                };
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use crate::sinks::log_sink::LogSink;

    use super::*;

    #[test]
    fn test() {
        let sd = SinkDef {
            name: format!("LogSink"),
            inputs: None, // TODO: Wrong
            config_tpl: None,
            build_sink: LogSink::new,
        };
    }
}
