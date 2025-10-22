use std::{any::TypeId, collections::HashMap};
mod event;
#[macro_use]
mod task;
mod node;

use event::Event;
use task::Node;

fn main() {
    println!("Hello, world!");

    let node = MyNode {};
    // print_inputs_outputs_for(&node);
}

struct MyNode {}

// Define handler methods
impl MyNode {
    fn handle_string(&mut self, input: String) -> HashMap<String, Vec<Event>> {
        println!("Handling string: {}", input);
        HashMap::new()
    }
}

// // Use the macro to automatically implement everything!
// impl_task! {
//     MyNode,
//     inputs: { String => "Input #1" => handle_string },
//     outputs: { i32 => "Output #1" }
// }

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
