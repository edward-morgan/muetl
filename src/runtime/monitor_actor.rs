use std::{collections::HashMap, sync::Mutex};

use kameo::{prelude::Message, Actor};

use crate::messages::{RetrieveStatus, Status, StatusUpdate};

#[derive(Actor)]
pub struct Monitor {
    statuses: Mutex<HashMap<u64, Vec<Status>>>,
}

impl Message<StatusUpdate> for Monitor {
    type Reply = ();
    async fn handle(
        &mut self,
        msg: StatusUpdate,
        _: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if let Some(statuses) = self.statuses.lock().unwrap().get_mut(&msg.id) {
            statuses.push(msg.status);
        } else {
            self.statuses
                .lock()
                .unwrap()
                .insert(msg.id, vec![msg.status]);
        }
    }
}

impl Message<RetrieveStatus> for Monitor {
    type Reply = Option<Status>;
    async fn handle(
        &mut self,
        msg: RetrieveStatus,
        _: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if let Some(statuses) = self.statuses.lock().unwrap().get_mut(&msg.id) {
            match statuses.last() {
                Some(l) => Some(l.clone()),
                None => None,
            }
        } else {
            None
        }
    }
}
