use kameo::prelude::*;
use std::{collections::HashMap, sync::Arc};

use kameo_actors::pubsub::{PubSub, Publish, Subscribe};

use crate::messages::event::Event;
use crate::{
    runtime::{event::InternalEvent, EventMessage, NegotiatedType},
    util::new_id,
};

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
    /// The negotiated type of this connection.
    pub chan_type: Arc<NegotiatedType>,
    /// A channel to publish messages on.
    pub chan_ref: ChannelImpl,
    /// A generated ID that the send side of this Connection should use when creating InternalEvents.
    sender_id: u64,
    /// The conn_name of the sending Task.
    sender_conn_name: String,
    /// The conn_name of the receiving Task.
    receiver_conn_name: String,
}
impl Connection {
    pub fn new(tpe: NegotiatedType, sender_conn_name: String, receiver_conn_name: String) -> Self {
        Self {
            chan_ref: Arc::new(PubSub::<EventMessage>::spawn(PubSub::new(
                kameo_actors::DeliveryStrategy::Guaranteed,
            ))),
            chan_type: Arc::new(tpe),
            sender_id: new_id(),
            sender_conn_name,
            receiver_conn_name,
        }
    }
}

pub struct IncomingConnections {
    conns: HashMap<u64, Arc<IncomingConnection>>,
}

impl From<&Vec<&Connection>> for IncomingConnections {
    fn from(value: &Vec<&Connection>) -> Self {
        let mut conns = HashMap::new();
        value.iter().for_each(|c| {
            conns.insert(c.sender_id, Arc::new(IncomingConnection::from(&c)));
        });
        Self { conns }
    }
}

impl IncomingConnections {
    pub fn conn_name_for(&self, ie: Arc<InternalEvent>) -> Result<String, String> {
        if let Some(ic) = self.conns.get(&ie.sender_id) {
            Ok(ic.receiver_conn_name.clone())
        } else {
            Err(format!(
                "cannot find input conn_name for sender_id {}",
                ie.sender_id
            ))
        }
    }

    pub async fn subscribe_to_all<T: Actor + Message<Arc<InternalEvent>>>(
        &self,
        subscriber_ref: ActorRef<T>,
    ) -> Result<(), String> {
        for (_, v) in &self.conns {
            match v.chan_ref.tell(Subscribe(subscriber_ref.clone())).await {
                Ok(_) => {}
                Err(e) => {
                    println!("Error from subscriber: {:?}", e);
                    return Err(format!("failed to subscribe"));
                }
            }
        }
        Ok(())
    }
}

/// The side of a `Connection` that is provided to consumer Tasks (Sinks and Nodes).
pub struct IncomingConnection {
    pub chan_type: Arc<NegotiatedType>,
    pub chan_ref: ChannelImpl,
    sender_id: u64,
    pub receiver_conn_name: String, // Descriptive only
}
impl IncomingConnection {
    pub fn from(c: &Connection) -> Self {
        Self {
            chan_ref: c.chan_ref.clone(),
            chan_type: c.chan_type.clone(),
            sender_id: c.sender_id,
            receiver_conn_name: c.receiver_conn_name.clone(),
        }
    }
}

pub struct OutgoingConnections {
    /// The mapping from output conn_name to OutgoingConnection for this Task.
    conns: HashMap<String, Arc<OutgoingConnection>>,
}
impl From<&Vec<&Connection>> for OutgoingConnections {
    fn from(value: &Vec<&Connection>) -> Self {
        let mut conns = HashMap::new();
        value.iter().for_each(|c| {
            conns.insert(
                c.sender_conn_name.clone(),
                Arc::new(OutgoingConnection::from(&c)),
            );
        });
        Self { conns }
    }
}

impl OutgoingConnections {
    /// Given a raw event from a Tasks' internal handler, do the following steps:
    /// 1. Attempt to find the `OutgoingConnection` for that conn_name.
    /// 2. If it exists, validate that the underlying type of the `Event` matches the `NegotiatedType` of the `OutgoingConnection`.
    /// 3. If that passes, create a new `InternalEvent` using the `sender_id` configured for the `OutgoingConnection` and publish it to the channel.
    ///
    /// If an error occurs in any of those steps, return it as a String..
    pub async fn publish_to(&self, ev: Arc<Event>) -> Result<(), String> {
        // We're going to get a conn_name in the event.
        // That needs to be mapped to a sender_id in the outgoing connection map
        // Then, an InternalEvent needs to be published to the right outgoing connection's
        // sender ref.
        if let Some(outgoing_conn) = self.conns.get(&ev.conn_name) {
            match outgoing_conn.chan_type.validate_types(vec![&ev]) {
                Ok(()) => {
                    outgoing_conn.publish(ev).await;
                    Ok(())
                }
                Err(e) => Err(e),
            }
        } else {
            Err(format!(
                "no outgoing connection exists for conn_name {}",
                ev.conn_name
            ))
        }
    }
}

/// The side of a `Connection` that is provided to producer Tasks (Daemons, Sources, and Nodes).
pub struct OutgoingConnection {
    pub chan_type: Arc<NegotiatedType>,
    pub chan_ref: ChannelImpl,
    pub sender_conn_name: String, // Descriptive only
    sender_id: u64,
}

impl OutgoingConnection {
    pub fn from(c: &Connection) -> Self {
        Self {
            chan_ref: c.chan_ref.clone(),
            chan_type: c.chan_type.clone(),
            sender_conn_name: c.sender_conn_name.clone(),
            sender_id: c.sender_id,
        }
    }

    // TODO: return a result type
    pub async fn publish(&self, ev: Arc<Event>) {
        self.chan_ref
            .tell(Publish(Arc::new(InternalEvent {
                sender_id: self.sender_id,
                event: ev.clone(),
            })))
            .await
            .unwrap();
    }
}
