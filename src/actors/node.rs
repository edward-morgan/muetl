use crate::actors::Subscription;
use crate::messages::{Status, StatusUpdate};
use crate::task_defs::{MuetlContext, OutputType, TaskResult};
use crate::util::new_id;
use kameo::actor::ActorRef;
use kameo::message::{Context, Message};
use kameo::Actor;
use kameo_actors::pubsub::PubSub;
use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::messages::event::Event;
use crate::task_defs::{node::Node, Input};

use super::EventMessage;

// TODO: Node is unfinished.

#[derive(Actor)]
pub struct NodeActor<T: 'static>
where
    T: Node,
{
    id: u32,
    node: Box<T>,
    /// The inputs of the wrapped node, retrieved via its get_inputs() function.
    inputs: HashMap<String, TypeId>,
    /// The outputs of the wrapped node, retrieved via its get_outputs() function.
    outputs: HashMap<String, OutputType>,
    /// For each output conn_name, keep a mapping of negotiated types to the PubSub channels results will be sent on.
    subscriber_chans: HashMap<String, Subscription>,
    /// A mapping of senders to this node, from source (output) conn_name to input conn_name
    input_conn_name_mapping: HashMap<String, String>,
    monitor_chan: PubSub<StatusUpdate>,
    current_context: MuetlContext,
    status: (mpsc::Sender<Status>, mpsc::Receiver<Status>),
}

impl<T: Node> NodeActor<T> {
    /// Update the current MuetlContext with the given TypeId for the given output conn_name.
    /// This updates the context that is passed to the wrapped node and should be called
    /// whenever a new subscription request is validated against this Node.
    fn add_subscriber_to_context(&mut self, conn_name: String, tpe: TypeId) {
        // Allow panics here because the input should have already been validated.
        let subs = self
            .current_context
            .current_subscribers
            .get_mut(&conn_name)
            .unwrap();

        if !subs.contains(&tpe) {
            subs.push(tpe);
        }
    }

    fn remove_subscriber_from_context(&mut self, conn_name: String, tpe: TypeId) {
        if let Some(subs_for_conn) = self.current_context.current_subscribers.get_mut(&conn_name) {
            subs_for_conn.extract_if(.., |t| tpe == *t);
        }
    }
}

impl<T: Node> NodeActor<T> {
    fn new(node: Box<T>, monitor_chan: PubSub<StatusUpdate>) -> NodeActor<T> {
        let inputs = node.get_inputs();
        let outputs = node.get_outputs();
        let subscriber_chans = HashMap::<String, Subscription>::new();

        let (results_tx, _) = mpsc::channel(1);
        let (status_tx, _) = mpsc::channel(1);

        NodeActor {
            id: new_id(),
            node,
            inputs,
            outputs,
            subscriber_chans,
            input_conn_name_mapping: HashMap::new(),
            monitor_chan,
            current_context: MuetlContext {
                results: results_tx,
                status: status_tx,
                current_subscribers: HashMap::new(),
            },
            status: mpsc::channel(100),
        }
    }

    fn set_source_conn_mapping(&mut self, from_conn_name: String, to_conn_name: String) {
        self.input_conn_name_mapping
            .insert(from_conn_name, to_conn_name);
    }
}

impl<T: Node> Message<Arc<Event>> for NodeActor<T> {
    type Reply = ();

    async fn handle(
        &mut self,
        ev: Arc<Event>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Before sending the event to the Node, remap its conn_name from the output of the sender
        // to the input of this Node.
        // if let Some(input_conn_name) = self.input_conn_name_mapping.get(&ev.conn_name) {
        //     match self
        //         .node
        //         .handle_event(
        //             &self.current_context,
        //             input_conn_name,
        //             ev.clone(),
        //             &mut self.status.0,
        //         )
        //         .await
        //     // TODO: handle status chan
        //     {
        //         TaskResult::Pass => {}
        //         // TaskResult::Status(status) => {
        //         //     self.monitor_chan
        //         //         .publish(StatusUpdate {
        //         //             id: self.id,
        //         //             status,
        //         //         })
        //         //         .await
        //         // }
        //         TaskResult::Events(events) => match self.produce_outputs(events).await {
        //             Ok(()) => {}
        //             Err(reason) => println!("failed to produce events: {}", reason),
        //         },
        //         TaskResult::Error(reason) => {
        //             println!("Error handling event named {}: {}", ev.name, reason)
        //         }
        //     }
        // }
    }
}
