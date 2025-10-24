use crate::messages::StatusUpdate;
use crate::util::new_id;
use kameo::actor::ActorRef;
use kameo::message::{Context, Message};
use kameo::Actor;
use kameo_actors::message_bus::MessageBus;
use kameo_actors::pubsub::PubSub;
use std::any::TypeId;
use std::collections::HashMap;
use std::panic;
use std::sync::Arc;

use crate::messages::event::Event;
use crate::task_defs::{node::Node, Input};

#[derive(Actor)]
pub struct NodeActor<T: 'static>
where
    T: Node,
{
    id: u32,
    node: Box<T>,
    inputs: HashMap<String, Vec<TypeId>>,
    outputs: HashMap<String, Vec<TypeId>>,
    subscriber_chans: HashMap<String, PubSub<Arc<Event>>>,
    /// A mapping of senders to this node, from source (output) conn_name to input conn_name
    input_conn_name_mapping: HashMap<String, String>,
    monitor_bus: PubSub<StatusUpdate>,
}

impl<T: Node> NodeActor<T> {
    fn new(node: Box<T>, monitor_chan: PubSub<StatusUpdate>) -> NodeActor<T> {
        let inputs = node.get_inputs();
        let outputs = node.get_outputs();
        let mut chans = HashMap::<String, PubSub<Arc<Event>>>::new();
        for (name, _) in &outputs {
            chans.insert(
                name.clone(),
                PubSub::<Arc<Event>>::new(kameo_actors::DeliveryStrategy::BestEffort),
            );
        }
        NodeActor {
            id: new_id(),
            node,
            inputs,
            outputs,
            subscriber_chans: chans,
            input_conn_name_mapping: HashMap::new(),
            monitor_bus: monitor_chan,
        }
    }

    fn set_source_conn_mapping(&mut self, from_conn_name: String, to_conn_name: String) {
        self.input_conn_name_mapping
            .insert(from_conn_name, to_conn_name);
    }

    fn subscribe_to<Subscriber: Actor + Message<Arc<Event>>>(
        &mut self,
        conn_name: String,
        r: ActorRef<Subscriber>,
    ) -> Result<(), String> {
        if !self.outputs.contains_key(&conn_name) {
            return Err(format!(
                "cannot subscribe to conn named '{}' (expected one of {:?})",
                conn_name,
                self.outputs.keys()
            ));
        }

        if let Some(chan) = self.subscriber_chans.get_mut(&conn_name) {
            chan.subscribe(r);
            Ok(())
        } else {
            Err(format!(
                "invalid state: failed to find pubsub for conn named '{}'",
                conn_name
            ))
        }
    }

    async fn produce_outputs(&mut self, events: Vec<Event>) {
        for event in events {
            if let Some(chan) = self.subscriber_chans.get_mut(&event.conn_name) {
                chan.publish(Arc::new(event)).await;
            } else {
                panic!(
                    "Produced event has conn_name {} not found for node (expected one of {:?})",
                    event.conn_name,
                    self.subscriber_chans.keys()
                );
            }
        }
    }
}

impl<T: Node> Message<Arc<Event>> for NodeActor<T> {
    type Reply = ();

    async fn handle(
        &mut self,
        ev: Arc<Event>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Before sending the event to the Node, remap it's conn_name from the output of the sender
        // to the input of this Node.
        if let Some(input_conn_name) = self.input_conn_name_mapping.get(&ev.conn_name) {
            match self.node.handle_event(input_conn_name, ev.clone()) {
                Ok((events, status)) => {
                    self.produce_outputs(events).await;
                    if let Some(stat) = status {
                        self.monitor_bus
                            .publish(StatusUpdate {
                                id: self.id,
                                status: stat,
                            })
                            .await
                    }
                }
                Err(reason) => println!("Error handling event named {}: {}", ev.name, reason),
            }
        }
    }
}
