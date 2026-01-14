use std::{collections::HashMap, sync::Arc};

use kameo::prelude::*;
use kameo::{actor::ActorRef, prelude::Message, Actor};
use kameo_actors::pubsub::{PubSub, Publish};
use tokio::{
    select,
    sync::mpsc::{self},
};
use tracing::Instrument;

use crate::{
    logging::global_registry,
    messages::{Status, StatusUpdate},
    runtime::connection::OutgoingConnections,
    system::util::new_id,
    task_defs::{source::Source, MuetlContext},
};

pub struct SourceActor {
    id: u64,
    trace_id: u64,
    task_name: String,
    source: Option<Box<dyn Source>>,
    monitor_chan: ActorRef<PubSub<StatusUpdate>>,
    current_context: MuetlContext,
    /// A mapping of output conn_names to internal sender IDs.
    outgoing_connections: OutgoingConnections,
}

impl SourceActor {
    pub fn new(
        trace_id: u64,
        task_name: String,
        source: Option<Box<dyn Source>>,
        monitor_chan: ActorRef<PubSub<StatusUpdate>>,
        outgoing_connections: OutgoingConnections,
    ) -> Self {
        Self::with_task_id(
            new_id(),
            trace_id,
            task_name,
            source,
            monitor_chan,
            outgoing_connections,
        )
    }

    pub fn with_task_id(
        task_id: u64,
        trace_id: u64,
        task_name: String,
        source: Option<Box<dyn Source>>,
        monitor_chan: ActorRef<PubSub<StatusUpdate>>,
        outgoing_connections: OutgoingConnections,
    ) -> Self {
        // Throwaway
        let (results_tx, _) = mpsc::channel(1);
        let (status_tx, _) = mpsc::channel(1);

        // Register this task with the log registry
        global_registry().register_task(task_id);

        SourceActor {
            id: task_id,
            trace_id,
            task_name,
            source,
            monitor_chan,
            current_context: MuetlContext {
                current_subscribers: HashMap::new(),
                results: results_tx,
                status: status_tx,
                event_name: None,
                event_headers: None,
            },
            outgoing_connections,
        }
    }
}

impl Message<()> for SourceActor {
    type Reply = ();
    async fn handle(
        &mut self,
        _: (),
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let (result_tx, mut result_rx) = mpsc::channel(100);
        let (status_tx, mut status_rx) = mpsc::channel(100);

        // Create a context for the source to own
        let source_context = MuetlContext {
            current_subscribers: self.current_context.current_subscribers.clone(),
            results: result_tx,
            status: status_tx,
            event_name: None,
            event_headers: None,
        };
        let mut source = self.source.take().unwrap();

        // Create span for task execution
        let span = tracing::info_span!(
            "task",
            trace_id = self.trace_id,
            task_id = self.id,
            task_name = %self.task_name,
        );

        let fut = tokio::spawn(
            async move {
                source.run(&source_context).await;
                source
            }
            .instrument(span),
        );

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
                                Err(reason) => tracing::error!("failed to produce events: {}", reason),
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
            tracing::info!(task_id = self.id, task_name = %self.task_name, "Source is finished; exiting...");

            // Allow the source to flush any buffered data before shutdown
            if let Some(mut source) = self.source.take() {
                let (result_tx, mut result_rx) = mpsc::channel(100);
                let (status_tx, _status_rx) = mpsc::channel(100);

                let shutdown_ctx = MuetlContext {
                    current_subscribers: HashMap::new(),
                    results: result_tx,
                    status: status_tx,
                    event_name: None,
                    event_headers: None,
                };

                let span = tracing::info_span!(
                    "task_shutdown",
                    trace_id = self.trace_id,
                    task_id = self.id,
                    task_name = %self.task_name,
                );

                async {
                    source.prepare_shutdown(&shutdown_ctx).await;

                    // Process any final results produced during shutdown
                    drop(shutdown_ctx);
                    while let Some(result) = result_rx.recv().await {
                        tracing::debug!(task_id = self.id, "Source produced final result during shutdown");
                        match self.outgoing_connections.publish_to(Arc::new(result)).await {
                            Ok(()) => {},
                            Err(reason) => tracing::error!(task_id = self.id, error = %reason, "Source failed to produce final events during shutdown"),
                        }
                    }
                }
                .instrument(span)
                .await;

                self.source = Some(source);
            }

            self.outgoing_connections.broadcast_shutdown().await;
            ctx.stop();
        }

        match fut.await {
            Ok(source) => {
                // Replace the source
                self.source = Some(source);
                // Enqueue another iteration
                // Explicitly catch a full mailbox error here
                match ctx.actor_ref().tell(()).try_send() {
                    Ok(_) => {}
                    Err(SendError::MailboxFull(())) => {
                        tracing::error!(taskid = self.id, task_name = %self.task_name, "Source mailbox is full");
                    }
                    Err(e) => {
                        tracing::error!(taskid =self.id, task_name = %self.task_name, "unknown error when enqueuing another iteration: {}", e);
                    }
                }
            }
            // Don't enqueue another iteration
            Err(e) => {
                tracing::error!(task_id = self.id, task_name = %self.task_name, error = %e, "Source task panicked");
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

impl Actor for SourceActor {
    type Args = Self;
    type Error = String;
    async fn on_start(args: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        // Send a trigger message to kick off the source.
        // Explicitly catch a full mailbox error here
        match actor_ref.tell(()).try_send() {
            Ok(_) => Ok(args),
            Err(SendError::MailboxFull(())) => {
                tracing::error!(taskid = args.id, task_name = %args.task_name, "mailbox is full on_start");
                Err(format!("failed to enqueue initial source iteration"))
            }
            Err(e) => {
                tracing::error!(taskid = args.id, task_name = %args.task_name, "unknown error on startup");
                Err(format!(
                    "unknown error occurred before initial source iteration: {}",
                    e
                ))
            }
        }
    }

    fn on_stop(
        &mut self,
        _actor_ref: kameo::actor::WeakActorRef<Self>,
        _reason: kameo::error::ActorStopReason,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        let id = self.id;
        async move {
            // Unregister from log registry
            global_registry().unregister_task(id);
            Ok(())
        }
    }
}
