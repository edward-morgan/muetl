use std::{any::TypeId, collections::HashMap, ops::Index, sync::Arc};

use kameo::{actor::ActorRef, error::Infallible, prelude::Message, Actor};
use kameo_actors::pubsub::PubSub;
use tokio::{
    select,
    sync::mpsc::{self},
};
use tokio_stream::StreamExt;

use crate::{
    actors::{HasSubscriptions, NegotiatedType, Subscription},
    messages::{event::Event, Status, StatusUpdate},
    system::util::new_id,
    task_defs::{daemon::Daemon, MuetlContext, OutputType},
};

use super::EventMessage;

pub type OwnedDaemon<T> = Option<Box<T>>;

pub struct DaemonActor<T: 'static>
where
    T: Daemon + Send,
{
    id: u64,
    daemon: OwnedDaemon<T>,
    /// For each output conn_name, keep a mapping of negotiated types to the PubSub channels results will be sent on.
    subscriber_chans: HashMap<String, Subscription>,
    monitor_chan: PubSub<StatusUpdate>,
    current_context: MuetlContext,
    /// A mapping of output conn_names to internal sender IDs.
    sender_ids: HashMap<String, u64>,
}

impl<T: Daemon> DaemonActor<T> {
    pub fn new(
        daemon: OwnedDaemon<T>,
        monitor_chan: PubSub<StatusUpdate>,
        sender_ids: HashMap<String, u64>,
        subscriber_chans: HashMap<String, Subscription>,
    ) -> Self {
        // Throwaway
        let (results_tx, _) = mpsc::channel(1);
        let (status_tx, _) = mpsc::channel(1);

        DaemonActor {
            id: new_id(),
            daemon,
            subscriber_chans,
            monitor_chan,
            current_context: MuetlContext {
                current_subscribers: HashMap::new(),
                results: results_tx,
                status: status_tx,
            },
            sender_ids,
        }
    }
}

impl<T: Daemon> HasSubscriptions for DaemonActor<T> {
    fn get_outputs(&mut self) -> HashMap<String, OutputType> {
        self.daemon.as_ref().unwrap().get_outputs()
    }

    fn get_subscriber_channel(&mut self, conn_name: &String) -> Result<&mut Subscription, String> {
        if let Some(ch) = self.subscriber_chans.get_mut(conn_name) {
            Ok(ch)
        } else {
            Err(format!(
                "cannot find subscriber for conn_name {}",
                conn_name
            ))
        }
    }

    fn get_sender_id_for(&self, conn_name: &String) -> Result<u64, String> {
        if let Some(id) = self.sender_ids.get(conn_name) {
            Ok(*id)
        } else {
            Err(format!("cannot find sender_id for conn_name {}", conn_name))
        }
    }

    // /// Update the current MuetlContext with the given TypeId for the given output conn_name.
    // /// This updates the context that is passed to the wrapped node and should be called
    // /// whenever a new subscription request is validated against this Node.
    // fn add_subscriber(&mut self, conn_name: String, tpe: TypeId) {
    //     // Allow panics here because the input should have already been validated.
    //     let subs = self
    //         .current_context
    //         .current_subscribers
    //         .get_mut(&conn_name)
    //         .unwrap();

    // //     if !subs.contains(&tpe) {
    // //         subs.push(tpe);
    // //     }
    // // }
    // fn remove_subscriber(&mut self, conn_name: String, tpe: TypeId) {
    //     if let Some(subs_for_conn) = self.current_context.current_subscribers.get_mut(&conn_name) {
    //         for i in 0..subs_for_conn.len() {
    //             if tpe == subs_for_conn[i] {
    //                 subs_for_conn.remove(i);
    //             }
    //         }
    //     }
    // }
}

impl<T: Daemon> Message<()> for DaemonActor<T> {
    type Reply = ();
    async fn handle(
        &mut self,
        _: (),
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let (result_tx, mut result_rx) = mpsc::channel(100);
        let (status_tx, mut status_rx) = mpsc::channel(100);

        // Create a context for the daemon to own
        let daemon_context = MuetlContext {
            current_subscribers: self.current_context.current_subscribers.clone(),
            results: result_tx,
            status: status_tx,
        };
        let mut daemon = self.daemon.take().unwrap();

        let fut = tokio::spawn(async move {
            daemon.run(&daemon_context).await;
            daemon
        });

        loop {
            select! {
                res = result_rx.recv() => {
                    if let Some(result) = res {
                        println!("Received result {:?}", result);
                        match self.produce_output(result).await {
                            Ok(()) => break,
                             Err(reason) => println!("failed to produce events: {}", reason),
                        }
                    } else if status_rx.is_closed() {
                        break;
                    }
                },
                res = status_rx.recv() => {
                    if let Some(status) = res {
                        println!("Received status {:?}", status);
                        let update = StatusUpdate{status: status, id: self.id};
                        self.monitor_chan.publish(update).await;
                    } else if result_rx.is_closed() {
                        break;
                    }
                },
            }
        }

        match fut.await {
            Ok(daemon) => {
                println!("Run finished");
                // Replace the daemon
                self.daemon = Some(daemon);
                // Enqueue another iteration
                ctx.actor_ref().tell(()).await.unwrap();
            }
            // Don't enqueue another iteration
            Err(e) => {
                println!("Daemon task panicked: {:?}", e);
                // Send a failure message to the monitor
                self.monitor_chan
                    .publish(StatusUpdate {
                        id: self.id,
                        status: Status::Failed(e.to_string()),
                    })
                    .await;
                // Stop the current task
                ctx.stop();
            }
        }
    }
}

impl<T: Daemon> Actor for DaemonActor<T> {
    type Args = Self;
    type Error = Infallible;
    async fn on_start(args: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        // Send a trigger message to kick off the daemon.
        actor_ref.tell(()).await.unwrap();
        Ok(args)
    }
}
