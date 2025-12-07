use std::{collections::HashMap, sync::Arc};

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
    task_defs::{daemon::Daemon, MuetlContext},
};

pub struct DaemonActor {
    id: u64,
    daemon: Option<Box<dyn Daemon>>,
    monitor_chan: ActorRef<PubSub<StatusUpdate>>,
    current_context: MuetlContext,
    /// A mapping of output conn_names to internal sender IDs.
    outgoing_connections: OutgoingConnections,
}

impl DaemonActor {
    pub fn new(
        daemon: Option<Box<dyn Daemon>>,
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

impl Message<()> for DaemonActor {
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

        let mut should_stop = false;
        let mut results_closed = false;
        let mut status_closed = false;

        loop {
            select! {
                res = result_rx.recv(), if !results_closed => {
                    match res {
                        Some(result) => {
                            match self.outgoing_connections.publish_to(Arc::new(result)).await {
                                Ok(()) => {},
                                Err(reason) => println!("failed to produce events: {}", reason),
                            }
                        },
                        None => {
                            results_closed = true;
                        }
                    }
                },
                res = status_rx.recv(), if !status_closed => {
                    match res {
                        Some(status) => {
                            let update = StatusUpdate{status: status.clone(), id: self.id};
                            self.monitor_chan.tell(Publish(update)).await.unwrap();
                            if matches!(status, Status::Finished) {
                                should_stop = true;
                            }
                        },
                        None => {
                            status_closed = true;
                        }
                    }
                },
            }

            if results_closed && status_closed {
                break;
            }
        }
        if should_stop {
            println!("Daemon is finished; exiting...");
            self.outgoing_connections.broadcast_shutdown().await;
            ctx.stop();
        }

        match fut.await {
            Ok(daemon) => {
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

impl Actor for DaemonActor {
    type Args = Self;
    type Error = Infallible;
    async fn on_start(args: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        // Send a trigger message to kick off the daemon.
        actor_ref.tell(()).await.unwrap();
        Ok(args)
    }
}
