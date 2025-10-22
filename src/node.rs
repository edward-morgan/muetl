use std::any::TypeId;
use std::collections::HashMap;

use kameo::message::{Context, Message};
use kameo::Actor;

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
}

impl<T: Node> NodeActor<T> {
    fn new(node: Box<T>) -> NodeActor<T> {
        let inputs = node.get_inputs();
        let outputs = node.get_outputs();
        NodeActor {
            node,
            inputs,
            outputs,
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
