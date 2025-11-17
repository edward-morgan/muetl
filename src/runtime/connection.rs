use kameo::prelude::*;
use std::{
    any::{Any, TypeId},
    collections::{HashMap, HashSet},
    future::Future,
    os::unix::net::Incoming,
    sync::Arc,
};
use tokio::sync::mpsc::error::TrySendError;

use kameo_actors::{
    pubsub::{PubSub, Publish, Subscribe},
    DeliveryStrategy,
};

use crate::runtime::{event::InternalEvent, EventMessage, NegotiatedType};
use crate::{messages::event::Event, task_defs::OutputType};

type ChannelImpl = Arc<ActorRef<PubSub<EventMessage>>>;
/*
 * There are three stages to connecting two Tasks together:
 * 1. Multiplexing a single Event with conn_name from an internal Task handler
 * to the right PubSub channel.
 * 2. Routing the publisher and subscriber ends of a PubSub to the right actors,
 * which is done by the runtime.
 * 3. Multiplexing a single InternalEvent received by a Sink or Node to route it
 * to the correct handler implementation
 *
 * InternalEvents contain a sender_id field that should uniquely identify a combination of:
 * 1. The Task that produced the Event
 * 2. The output connection that produced the Event
 *
 * At runtime, sending Tasks (Daemons, Sources, and Nodes) should receive:
 * 1. A list of PubSub channels to produce, along with their negotiated types.
 * 2. The mapping of output conn_names to sender_ids that are used to construct InternalEvents.
 * Likewise, receiving Tasks (Nodes and Sinks) should receive:
 * 1. A list of PubSub channels to subscribe to, along with their negotiated types.
 * 2. The mapping of sender_ids to input conn_names that are used to route incoming InternalEvents.
 */

/// The runtime representation of a connection between two Tasks, which includes
/// two components:
/// 1. The type reference that has been negotiated for this connection.
/// 2. A reference to the actor that will handle the producer/consumer relationship
/// between the Tasks, which preserves loose coupling.
///
/// TODO: Ideally chan_ref shouldn't be `pub`, since callers could use it to stop or
/// otherwise affect the PubSub, when really they *only* need the ability to publish
/// to it.
pub struct Connection {
    pub chan_type: Arc<NegotiatedType>,
    pub chan_ref: ChannelImpl,
}
impl Connection {
    pub fn new(tpe: NegotiatedType, chan_ref: Arc<ActorRef<PubSub<EventMessage>>>) -> Self {
        Self {
            chan_ref: chan_ref.clone(),
            chan_type: Arc::new(tpe),
        }
    }
    pub async fn publish(&self, msg: EventMessage) -> Result<(), SendError<Publish<EventMessage>>> {
        self.chan_ref.tell(Publish(msg)).await
    }
}

/// The side of a `Connection` that is provided to consumer Tasks (Sinks and Nodes).
pub struct IncomingConnection {
    pub chan_type: Arc<NegotiatedType>,
    pub chan_ref: ChannelImpl,
    pub receiver_conn_name: String, // Descriptive only
    sender_id: u64,
}
impl IncomingConnection {
    pub fn from(c: &Connection, sender_id: u64, receiver_conn_name: String) -> Self {
        Self {
            chan_ref: c.chan_ref.clone(),
            chan_type: c.chan_type.clone(),
            receiver_conn_name,
            sender_id,
        }
    }

    // TODO: implement pub fn subscribe_to(&self, ...)
}

/// The side of a `Connection` that is provided to producer Tasks (Daemons, Sources, and Nodes).
pub struct OutgoingConnection {
    pub chan_type: Arc<NegotiatedType>,
    pub chan_ref: ChannelImpl,
    pub sender_conn_name: String, // Descriptive only
    sender_id: u64,
}

impl OutgoingConnection {
    pub fn from(c: &Connection, sender_id: u64, sender_conn_name: String) -> Self {
        Self {
            chan_ref: c.chan_ref.clone(),
            chan_type: c.chan_type.clone(),
            sender_conn_name,
            sender_id,
        }
    }

    // TODO: return a result type
    pub async fn publish_to(&self, ev: Arc<Event>) {
        self.chan_ref
            .tell(Publish(Arc::new(InternalEvent {
                sender_id: self.sender_id,
                event: ev.clone(),
            })))
            .await
            .unwrap();
    }
}
