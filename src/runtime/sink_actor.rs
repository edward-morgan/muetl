use std::collections::HashSet;
use std::sync::Arc;

use kameo::{actor::ActorRef, prelude::Message, Actor};
use kameo_actors::pubsub::{PubSub, Publish};
use tokio::sync::mpsc;
use tracing::Instrument;

use crate::logging::global_registry;
use crate::messages::SystemEvent;
use crate::runtime::connection::IncomingConnections;
use crate::{
    messages::{Status, StatusUpdate},
    runtime::event::InternalEvent,
    task_defs::{sink::Sink, MuetlSinkContext},
    util::new_id,
};

use super::event::Payload;

pub struct SinkActor {
    id: u64,
    trace_id: u64,
    task_name: String,
    sink: Option<Box<dyn Sink>>,
    monitor_chan: ActorRef<PubSub<StatusUpdate>>,
    /// The mapping set by the system at runtime to tell this actor which
    /// input conn_name events with a given sender_id should go to.
    subscriptions: IncomingConnections,
    active_subscriptions: HashSet<u64>,
}

impl SinkActor {
    pub fn new(
        trace_id: u64,
        task_name: String,
        sink: Option<Box<dyn Sink>>,
        monitor_chan: ActorRef<PubSub<StatusUpdate>>,
        subscriptions: IncomingConnections,
    ) -> Self {
        Self::with_task_id(
            new_id(),
            trace_id,
            task_name,
            sink,
            monitor_chan,
            subscriptions,
        )
    }

    pub fn with_task_id(
        task_id: u64,
        trace_id: u64,
        task_name: String,
        sink: Option<Box<dyn Sink>>,
        monitor_chan: ActorRef<PubSub<StatusUpdate>>,
        subscriptions: IncomingConnections,
    ) -> Self {
        let incoming_sender_ids = subscriptions.incoming_sender_ids();

        // Register this task with the log registry
        global_registry().register_task(task_id);

        SinkActor {
            id: task_id,
            trace_id,
            task_name,
            sink,
            monitor_chan,
            subscriptions,
            active_subscriptions: incoming_sender_ids,
        }
    }

    /// Determines whether or not this Sink should shut down, which relies on looking at each incoming connection
    /// and determining if any of them can potentially receive data.
    pub fn should_shut_down(&self) -> bool {
        self.active_subscriptions.is_empty()
    }
}

impl Message<Arc<SystemEvent>> for SinkActor {
    type Reply = ();
    async fn handle(
        &mut self,
        _msg: Arc<SystemEvent>,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        tracing::info!(task_id = self.id, task_name = %self.task_name, "Sink received shutdown signal");
        ctx.stop();
    }
}

impl Message<Arc<InternalEvent>> for SinkActor {
    type Reply = ();
    async fn handle(
        &mut self,
        msg: Arc<InternalEvent>,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match &msg.event {
            Payload::Stopped => {
                // Mark the current IncomingConnection as stopped
                self.active_subscriptions.remove(&msg.sender_id);
                // If no incoming connections are active, then stop the Actor.
                if self.should_shut_down() {
                    tracing::info!(task_id = self.id, task_name = %self.task_name, "No incoming connections are still active; Sink will shut down.");

                    // Allow the sink to flush any buffered data before shutdown
                    if let Some(mut sink) = self.sink.take() {
                        let (status_tx, _status_rx) = mpsc::channel(100);

                        let shutdown_ctx = MuetlSinkContext {
                            status: status_tx,
                            event_name: "shutdown".to_string(),
                            event_headers: Default::default(),
                        };

                        let span = tracing::info_span!(
                            "task_shutdown",
                            trace_id = self.trace_id,
                            task_id = self.id,
                            task_name = %self.task_name,
                        );

                        sink.prepare_shutdown(&shutdown_ctx)
                            .instrument(span)
                            .await;

                        self.sink = Some(sink);
                    }

                    ctx.stop();
                }
            }
            Payload::Data(ev) => {
                // Map the incoming event to the right input conn_name
                match self.subscriptions.conn_name_for(msg.clone()) {
                    Ok(input_conn_name) => {
                        let (status_tx, mut status_rx) = mpsc::channel(100);
                        let ctx = MuetlSinkContext {
                            status: status_tx,
                            event_name: ev.name.clone(),
                            event_headers: ev.headers.clone(),
                        };

                        let mut sink = self.sink.take().unwrap();
                        let m = ev.clone();
                        let conn_name = input_conn_name.clone();

                        // Create span for task execution
                        let span = tracing::info_span!(
                            "task",
                            trace_id = self.trace_id,
                            task_id = self.id,
                            task_name = %self.task_name,
                        );

                        let fut = tokio::spawn(
                            async move {
                                sink.handle_event_for_conn(&ctx, &conn_name, m).await;
                                sink
                            }
                            .instrument(span),
                        );

                        loop {
                            tokio::select! {
                                res = status_rx.recv() => {
                                    if let Some(status) = res {
                                        tracing::debug!(task_id = self.id, status = ?status, "Sink received status");
                                        let update = StatusUpdate{status: status, id: self.id};
                                        self.monitor_chan.tell(Publish(update)).await.unwrap();
                                    } else {
                                        break;
                                    }
                                },
                            }
                        }

                        match fut.await {
                            Ok(sink) => {
                                self.sink = Some(sink);
                            }
                            Err(e) => {
                                tracing::error!(task_id = self.id, task_name = %self.task_name, error = %e, "Sink task panicked");
                                // Send a failure message to the monitor
                                self.monitor_chan
                                    .tell(Publish(StatusUpdate {
                                        id: self.id,
                                        status: Status::Failed(e.to_string()),
                                    }))
                                    .await
                                    .unwrap()
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(task_id = self.id, error = %e, "Runtime error")
                    }
                }
            }
        }
    }
}
impl Actor for SinkActor {
    type Args = Self;
    type Error = String;
    async fn on_start(
        args: Self::Args,
        actor_ref: kameo::prelude::ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        // Subscribe to each of the subscriptions we've been initialized with
        match args.subscriptions.subscribe_to_all(actor_ref).await {
            Ok(()) => Ok(args),
            Err(e) => Err(e),
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
