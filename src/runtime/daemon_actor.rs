use std::{collections::HashMap, rc::Rc, sync::Arc};

use kameo::{actor::ActorRef, error::Infallible, prelude::Message, Actor};
use kameo_actors::pubsub::{PubSub, Publish};
use tokio::{
    select,
    sync::mpsc::{self},
};
use tokio_stream::StreamExt;

use crate::{
    messages::{Status, StatusUpdate},
    runtime::connection::OutgoingConnections,
    system::util::new_id,
    task_defs::{daemon::Daemon, MuetlContext, OutputType},
};

pub type OwnedDaemon<T> = Option<Box<T>>;

pub struct DaemonActor<T: 'static>
where
    T: Daemon + Send,
{
    id: u64,
    daemon: OwnedDaemon<T>,
    monitor_chan: ActorRef<PubSub<StatusUpdate>>,
    current_context: MuetlContext,
    /// A mapping of output conn_names to internal sender IDs.
    outgoing_connections: OutgoingConnections,
}

impl<T: Daemon> DaemonActor<T> {
    pub fn new(
        daemon: OwnedDaemon<T>,
        monitor_chan: ActorRef<PubSub<StatusUpdate>>,
        outgoing_connections: OutgoingConnections,
    ) -> Self {
        // Throwaway
        let (results_tx, _) = mpsc::channel(1);
        let (status_tx, _) = mpsc::channel(1);

        DaemonActor {
            id: new_id(),
            daemon,
            monitor_chan,
            current_context: MuetlContext {
                current_subscribers: HashMap::new(),
                results: results_tx,
                status: status_tx,
            },
            outgoing_connections,
        }
    }
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
                        match self.outgoing_connections.publish_to(Arc::new(result)).await {
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
                        let update = StatusUpdate{status: status.clone(), id: self.id};
                        self.monitor_chan.tell(Publish(update)).await.unwrap();
                        match &status {
                            Status::Finished => {
                                println!("Daemon is finished; exiting...");
                                ctx.stop();
                            }
                            _ => {},
                        }
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
                    .tell(Publish(StatusUpdate {
                        id: self.id,
                        status: Status::Failed(e.to_string()),
                    }))
                    .await
                    .unwrap();
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
