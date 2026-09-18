use kameo::prelude::*;
use std::any::TypeId;
use std::collections::HashSet;
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

use kameo_actors::pubsub::{PubSub, Publish, Subscribe};

use crate::messages::event::Event;
use crate::runtime::error::RuntimeError;
use crate::runtime::{event::InternalEvent, EventMessage, NegotiatedType};
use crate::task_defs::Subscription;

use super::event::Payload;

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
 * At runtime, sending Tasks (Sources and Operators) should receive:
 * 1. A list of PubSub channels to produce, along with their negotiated types.
 * 2. The mapping of output conn_names to sender_ids that are used to construct InternalEvents.
 * Likewise, receiving Tasks (Operators and Sinks) should receive:
 * 1. A list of PubSub channels to subscribe to, along with their negotiated types.
 * 2. The mapping of sender_ids to input conn_names that are used to route incoming InternalEvents.
 */

/// The runtime representation of a connection between two Tasks, which includes
/// two components:
/// 1. The type reference that has been negotiated for this connection.
/// 2. A reference to the actor that will handle the producer/consumer relationship
/// between the Tasks, which preserves loose coupling.
#[derive(Clone)]
pub struct Connection {
    /// An ID that uniquely represents this connection. Senders should use this when creating InternalEvents.
    connection_key: ConnectionKey,
    /// The negotiated type of this connection.
    pub chan_type: Arc<NegotiatedType>,
    /// A channel to publish messages on.
    pub chan_ref: ChannelImpl,
    /// The conn_name of the sending Task.
    sender_conn_name: String,
    /// The conn_name of the receiving Task.
    receiver_conn_name: String,
}

pub(crate) type ConnectionKey = Uuid;

impl Connection {
    pub fn new(tpe: NegotiatedType, sender_conn_name: String, receiver_conn_name: String) -> Self {
        Self {
            chan_ref: Arc::new(PubSub::<EventMessage>::spawn(PubSub::new(
                kameo_actors::DeliveryStrategy::Guaranteed,
            ))),
            chan_type: Arc::new(tpe),
            connection_key: Uuid::new_v4(),
            sender_conn_name,
            receiver_conn_name,
        }
    }

    pub fn get_sender_conn_name(&self) -> String {
        self.sender_conn_name.clone()
    }
    pub fn get_receiver_conn_name(&self) -> String {
        self.receiver_conn_name.clone()
    }
}

pub struct IncomingConnections {
    conns: HashMap<ConnectionKey, Arc<Connection>>,
    active: HashSet<ConnectionKey>,
}

impl From<&Vec<Arc<Connection>>> for IncomingConnections {
    fn from(value: &Vec<Arc<Connection>>) -> Self {
        let mut conns: HashMap<ConnectionKey, Arc<Connection>> = HashMap::new();

        for conn in value {
            conns.insert(conn.connection_key, conn.clone());
        }

        Self {
            conns,
            active: value.iter().map(|c| c.connection_key.clone()).collect(),
        }
    }
}

impl IncomingConnections {
    pub fn conn_name_for(&self, ie: Arc<InternalEvent>) -> Result<String, RuntimeError> {
        if let Some(incoming_for_conn) = self.conns.get(&ie.sender_id) {
            Ok(incoming_for_conn.receiver_conn_name.clone())
        } else {
            Err(RuntimeError::UnknownIncomingConnection { id: ie.sender_id })
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
                    tracing::error!(error = ?e, "Failed to subscribe to channel");
                    return Err(format!("failed to subscribe"));
                }
            }
        }
        Ok(())
    }

    /// Returns the list of sender IDs that are in this IncomingConnections set.
    pub fn incoming_sender_ids(&self) -> HashSet<ConnectionKey> {
        self.conns.iter().map(|(&id, _)| id).collect()
    }

    /// Returns the set of subscriptions for this `IncomingConnections` object, sorted by the
    /// receiver_conn_name.
    pub fn subscriptions(&self) -> HashMap<String, Vec<Subscription>> {
        let mut hm: HashMap<String, Vec<Subscription>> = HashMap::new();
        for (conn_key, conn) in &self.conns {
            let sub = Subscription {
                publisher_conn_name: conn.get_sender_conn_name(),
                subscriber_conn_name: conn.get_receiver_conn_name(),
                subscription_type: conn.chan_type.clone(),
                active: self.active.contains(conn_key),
            };
            if let Some(incoming_for_conn_name) = hm.get_mut(&conn.receiver_conn_name) {
                incoming_for_conn_name.push(sub);
            } else {
                hm.insert(conn.receiver_conn_name.clone(), vec![sub]);
            }
        }

        hm
    }

    /// Returns whether there are any active Connections in this IncomingConnections object.
    pub fn alive(&self) -> bool {
        !self.active.is_empty()
    }

    /// Mark that an incoming connection has become inactive.
    pub fn mark_inactive(&mut self, key: ConnectionKey) {
        self.active.remove(&key);
    }
}

/// The side of a `Connection` that is provided to consumer Tasks (Sinks and Operators).
pub struct IncomingConnection {
    pub connection_key: ConnectionKey,
    pub chan_type: Arc<NegotiatedType>,
    pub chan_ref: ChannelImpl,
    pub receiver_conn_name: String, // Descriptive only
    /// Whether or not this connection is active, which corresponds to whether or not a sender has sent the
    /// `Payload::Stopped` event type.
    pub is_active: bool,
}
impl IncomingConnection {
    pub fn from(c: &Connection) -> Self {
        Self {
            connection_key: c.connection_key.clone(),
            chan_ref: c.chan_ref.clone(),
            chan_type: c.chan_type.clone(),
            receiver_conn_name: c.receiver_conn_name.clone(),
            is_active: true,
        }
    }
}

/// The side of a `Connection` that is provided to producer Tasks (Sources and Operators).
pub struct OutgoingConnections {
    /// The mapping from output conn_name to OutgoingConnection for this Task.
    conns: HashMap<String, Arc<OutgoingConnection>>,
}
impl From<&Vec<Arc<Connection>>> for OutgoingConnections {
    fn from(value: &Vec<Arc<Connection>>) -> Self {
        let mut conns = HashMap::new();
        value.iter().for_each(|c| {
            conns.insert(
                c.sender_conn_name.clone(),
                Arc::new(OutgoingConnection::from(c.clone())),
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
    pub async fn publish_to(&self, ev: Arc<Event>) -> Result<(), RuntimeError> {
        // We're going to get a conn_name in the event.
        // That needs to be mapped to a sender_id in the outgoing connection map
        // Then, an InternalEvent needs to be published to the right outgoing connection's
        // sender ref.
        if let Some(outgoing_conn) = self.conns.get(&ev.conn_name) {
            outgoing_conn.chan_type.validate_types(vec![&ev])?;
            outgoing_conn.publish(ev).await?;
            Ok(())
        } else {
            Err(RuntimeError::UnknownOutgoingConnection {
                conn_name: ev.conn_name.clone(),
            })
        }
    }

    /// Send a sentinel shutdown message to **all** outgoing connections.
    pub async fn broadcast_shutdown(&self) {
        for (_id, conn) in &self.conns {
            conn.shutdown().await;
        }
    }

    /// Retrieve the mapping of connection names to the type ID(s) that have been negotiated
    /// for that connection.
    pub fn get_connection_types(&self) -> HashMap<String, Vec<TypeId>> {
        let mut hm = HashMap::with_capacity(self.conns.len());
        for (conn_name, outgoing_conn) in &self.conns {
            match outgoing_conn.chan_type.as_ref() {
                NegotiatedType::AllOf(types) => hm.insert(conn_name.clone(), types.clone()),
                NegotiatedType::Singleton(tpe) => hm.insert(conn_name.clone(), vec![tpe.clone()]),
            };
        }
        hm
    }
}

/// The side of a `Connection` that is provided to producer Tasks (Sinks, Sources, and Operators).
pub struct OutgoingConnection {
    pub connection_key: ConnectionKey,
    pub chan_type: Arc<NegotiatedType>,
    pub chan_ref: ChannelImpl,
    pub sender_conn_name: String, // Descriptive only
}

impl OutgoingConnection {
    pub fn from(c: Arc<Connection>) -> Self {
        Self {
            chan_ref: c.chan_ref.clone(),
            chan_type: c.chan_type.clone(),
            sender_conn_name: c.sender_conn_name.clone(),
            connection_key: c.connection_key,
        }
    }

    pub async fn publish(&self, ev: Arc<Event>) -> Result<(), RuntimeError> {
        match self
            .chan_ref
            .tell(Publish(Arc::new(InternalEvent {
                sender_id: self.connection_key,
                event: Payload::Data(ev.clone()),
            })))
            .await
        {
            Ok(()) => Ok(()),
            Err(e) => Err(RuntimeError::PublishError(e.to_string())),
        }
    }

    /// Shut down this connection from the perspective of the sender, which involves
    /// sending a Payload::Stopped event to all subscribers.
    pub async fn shutdown(&self) {
        self.chan_ref
            .tell(Publish(Arc::new(InternalEvent {
                sender_id: self.connection_key,
                event: Payload::Stopped,
            })))
            .await
            .unwrap();
        // After publishing Payload::Stopped, shut the PubSub channel down so that the chan_ref is marked inactive.
        self.chan_ref.stop_gracefully().await.unwrap();
    }
}
