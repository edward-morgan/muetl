use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::Arc,
};

/// Events control data movement in muetl and are produced and consumed by Tasks.
#[derive(Debug, Clone)]
pub struct Event {
    /// The identifier of this event.
    pub name: String,
    /// What output connection name this event is being produced to on the downstream task.
    /// For TaskDefs that accept Events from upstream sources, conn_name will not be
    /// meaningful, as it refers to a conn_name for an upstream TaskDef's output.
    pub conn_name: String,
    /// A key/value map of header metadata associated with this event.
    pub headers: HashMap<String, String>,
    /// The payload contained in this Event. Its type should be pointed to by tpe.
    data: Arc<dyn Any + Send + Sync>,
}

impl Event {
    pub fn new(
        name: String,
        conn_name: String,
        headers: HashMap<String, String>,
        data: Arc<dyn Any + Send + Sync>,
    ) -> Event {
        Event {
            name,
            conn_name,
            headers,
            data: data.clone(),
        }
    }
}

impl Event {
    pub fn get_data(&self) -> Arc<dyn Any + Send + Sync> {
        self.data.clone()
    }
}
