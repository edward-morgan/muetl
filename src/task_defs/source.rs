use std::{any::TypeId, collections::HashMap};

use crate::task_defs::*;

pub trait Source: TaskDef + Send + Sync {
    // fn get_inputs(&self) -> HashMap<String, Vec<TypeId>>;
    fn get_outputs(&self) -> HashMap<String, Vec<TypeId>>;
}
