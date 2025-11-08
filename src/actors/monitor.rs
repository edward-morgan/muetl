use std::{
    collections::HashMap,
    sync::Mutex,
};

use kameo::{prelude::Message, Actor};

use crate::messages::{RetrieveStatus, Status, StatusUpdate};

#[derive(Actor)]
pub struct Monitor {
    statuses: Mutex<HashMap<u32, Status>>,
}

impl Message<StatusUpdate> for Monitor {
    type Reply = ();
    async fn handle(
        &mut self,
        msg: StatusUpdate,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.statuses.lock().unwrap().insert(msg.id, msg.status);
    }
}

impl Message<RetrieveStatus> for Monitor {
    type Reply = Option<Status>;
    async fn handle(
        &mut self,
        msg: RetrieveStatus,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.statuses
            .lock()
            .unwrap()
            .get(&msg.id)
            .map(|s| s.clone())
    }
}
