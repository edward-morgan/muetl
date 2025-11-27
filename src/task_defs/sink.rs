use std::{future::Future, sync::Arc};

use kameo::actor::ActorRef;
use kameo_actors::pubsub::PubSub;

use crate::{
    messages::{event::Event, StatusUpdate},
    runtime::{connection::IncomingConnections, sink_actor::SinkActor},
};
use async_trait::async_trait;

use super::{HasInputs, MuetlSinkContext, TaskDef};

#[async_trait]
pub trait Sink: TaskDef + HasInputs + Send + Sync {
    /// Handle an Event sent to conn_name. The handler should disregard the conn_name
    /// inside the Event; that will be the name of the output conn from the source.
    async fn handle_event_for_conn(
        &mut self,
        ctx: &MuetlSinkContext,
        conn_name: &String,
        ev: Arc<Event>,
    );
}
