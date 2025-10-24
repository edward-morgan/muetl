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
    /// What input connection name this event is being produced to on the downstream task.
    pub conn_name: String,
    /// A key/value map of header metadata associated with this event.
    pub headers: HashMap<String, String>,
    /// The type of data in this Event. Should be automatically set upon Event creation and
    /// used to downcast the data field.
    tpe: TypeId,
    /// The payload contained in this Event. Its type should be pointed to by tpe.
    data: Arc<dyn Any + Send + Sync>,
}

impl Event {
    fn new(
        name: String,
        conn_name: String,
        headers: HashMap<String, String>,
        data: Arc<dyn Any + Send + Sync>,
    ) -> Event {
        Event {
            name,
            conn_name,
            headers,
            tpe: data.type_id(),
            data: data.clone(),
        }
    }
}

impl Event {
    pub fn get_data(&self) -> Arc<dyn Any + Send + Sync> {
        self.data.clone()
    }
    pub fn get_data_type(&self) -> TypeId {
        self.tpe
    }
}
