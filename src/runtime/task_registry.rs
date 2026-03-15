use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// The runtime registry that keeps track of instantiated Tasks across
/// all Roots on this instance.
///
/// Note that this is different from a `Registry` in that `TaskRegistry`
/// contains the runtime information for instantiated Tasks, rather than
/// the storefront for available TaskDefs.
///
/// Any components that service requests for running muetl Flows should
/// utilize `TaskRegistry` to uniquely identify Tasks that are running.
pub struct TaskRegistry {
    tasks: Arc<Mutex<HashMap<u64, RuntimeTaskInfo>>>,
}

impl Default for TaskRegistry {
    fn default() -> Self {
        Self {
            tasks: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl TaskRegistry {
    pub fn register_task(&mut self, rti: RuntimeTaskInfo) -> Result<(), String> {
        let mut t = self.tasks.lock().unwrap();
        if t.contains_key(&rti.id) {
            Err(format!(
                "TaskRegistry already contains runtime information for a task with id '{}'",
                rti.id
            ))
        } else {
            t.insert(rti.id, rti);
            Ok(())
        }
    }

    pub fn task_for(&self, id: u64) -> Option<RuntimeTaskInfo> {
        if let Some(rti) = self.tasks.lock().unwrap().get(&id) {
            Some(rti.clone())
        } else {
            None
        }
    }
}

/// Runtime information about a Task.
#[derive(Clone)]
pub struct RuntimeTaskInfo {
    /// The ID of this Task when it was instantiated by a Root.
    ///
    /// Note that multiple instances of the same TaskDef will have different
    /// ids.
    id: u64,
    /// The ID of the TaskDef that was used when instantiating
    /// this Task.
    ///
    /// Note that multiple instances of the same TaskDef will have the same
    /// task_id.
    task_id: String,
    /// A human-readable name for this Task. Note that this may or may not
    /// be unique across multiple instances of the same TaskDef and should
    /// not be used to uniquely identify a Task.
    task_name: String,
}
