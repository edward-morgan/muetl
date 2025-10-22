use std::any::TypeId;
use std::collections::HashMap;

use kameo::actor::ActorRef;
use kameo::message::{Context, Message};
use kameo::Actor;
use kameo_actors::pubsub::PubSub;

use crate::event::Event;
use crate::task::{Input, Node};

#[derive(Actor)]
pub struct NodeActor<T: 'static>
where
    T: Node,
{
    node: Box<T>,
    inputs: HashMap<String, Vec<TypeId>>,
    outputs: HashMap<String, Vec<TypeId>>,
    subscriber_chans: HashMap<String, PubSub<Event>>,
}

impl<T: Node> NodeActor<T> {
    fn new(node: Box<T>) -> NodeActor<T> {
        let inputs = node.get_inputs();
        let outputs = node.get_outputs();
        let mut chans = HashMap::<String, PubSub<Event>>::new();
        for (name, _) in &outputs {
            chans.insert(
                name.clone(),
                PubSub::<Event>::new(kameo_actors::DeliveryStrategy::BestEffort),
            );
        }
        NodeActor {
            node,
            inputs,
            outputs,
            subscriber_chans: chans,
        }
    }

    fn subscribe_to<Subscriber: Actor + Message<Event>>(
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
}

impl<T: Node> Message<Event> for NodeActor<T> {
    type Reply = ();

    async fn handle(&mut self, ev: Event, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        match self.node.handle_event(&ev) {
            Ok(events) => {
                // TODO: what needs to happen now is:
                // For each event produced by the Node, we know its output conn/type match what is required
                // For each subscriber to this conn, remap the event conn_name to the matching input conn_name
                // and send it out.
            }
            Err(reason) => println!("Error handling event named {}: {}", ev.name, reason),
        }
    }
}
