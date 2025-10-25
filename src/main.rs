use std::{any::TypeId, collections::HashMap};
mod messages;
#[macro_use]
mod task_defs;
mod actors;
mod daemons;
mod util;

use crate::task_defs::node::Node;
use kameo::{
    prelude::{Context, Message},
    Actor,
};
use kameo_actors::pubsub::PubSub;
use messages::event::Event;

fn main() {
    println!("Hello, world!");

    let node = MyNode {};
    let mut pubsub = PubSub::new(kameo_actors::DeliveryStrategy::BestEffort);
    let actor_ref = MyActor::spawn(MyActor);
    // Use PubSub as a standalone object
    pubsub.subscribe(actor_ref.clone());
    // pubsub.publish("Hello, World!").await;
}
#[derive(Actor)]
struct MyActor;

impl Message<&'static str> for MyActor {
    type Reply = ();
    async fn handle(
        &mut self,
        msg: &'static str,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
    }
}

struct MyNode {}

// Define handler methods
impl MyNode {
    fn handle_string(&mut self, input: String) -> HashMap<String, Vec<Event>> {
        println!("Handling string: {}", input);
        HashMap::new()
    }
}

fn print_inputs_outputs_for(node: &dyn Node) {
    let inputs = node.get_inputs();
    let outputs = node.get_outputs();

    println!("Inputs:");
    for (name, type_ids) in &inputs {
        println!("  {} -> {:?}", name, type_ids);
    }

    println!("Outputs:");
    for (name, type_id) in &outputs {
        println!("  {} -> {:?}", name, type_id);
    }
}
