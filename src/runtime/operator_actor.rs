use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use kameo::{actor::ActorRef, prelude::Message, Actor};
use kameo_actors::pubsub::{PubSub, Publish};
use tokio::sync::mpsc;
use tracing::Instrument;

use crate::logging::global_registry;
use crate::messages::SystemEvent;
use crate::runtime::connection::{IncomingConnections, OutgoingConnections};
use crate::{
    messages::{Status, StatusUpdate},
    runtime::event::InternalEvent,
    task_defs::{operator::Operator, MuetlContext},
    util::new_id,
};

use super::event::Payload;

pub struct OperatorActor {
    id: u64,
    trace_id: u64,
    task_name: String,
    operator: Option<Box<dyn Operator>>,
    monitor_chan: ActorRef<PubSub<StatusUpdate>>,
    /// The mapping set by the system at runtime to tell this actor which
    /// input conn_name events with a given sender_id should go to.
    subscriptions: IncomingConnections,
    active_subscriptions: HashSet<u64>,
    /// A mapping of output conn_names to internal sender IDs.
    outgoing_connections: OutgoingConnections,
}

impl OperatorActor {
    pub fn new(
        trace_id: u64,
        task_name: String,
        operator: Option<Box<dyn Operator>>,
        monitor_chan: ActorRef<PubSub<StatusUpdate>>,
        subscriptions: IncomingConnections,
        outgoing_connections: OutgoingConnections,
    ) -> Self {
        Self::with_task_id(
            new_id(),
            trace_id,
            task_name,
            operator,
            monitor_chan,
            subscriptions,
            outgoing_connections,
        )
    }

    pub fn with_task_id(
        task_id: u64,
        trace_id: u64,
        task_name: String,
        operator: Option<Box<dyn Operator>>,
        monitor_chan: ActorRef<PubSub<StatusUpdate>>,
        subscriptions: IncomingConnections,
        outgoing_connections: OutgoingConnections,
    ) -> Self {
        let incoming_sender_ids = subscriptions.incoming_sender_ids();

        // Register this task with the log registry
        global_registry().register_task(task_id);

        OperatorActor {
            id: task_id,
            trace_id,
            task_name,
            operator,
            monitor_chan,
            subscriptions,
            active_subscriptions: incoming_sender_ids,
            outgoing_connections,
        }
    }

    /// Determines whether or not this Operator should shut down, which relies on looking at each incoming connection
    /// and determining if any of them can potentially receive data.
    pub fn should_shut_down(&self) -> bool {
        self.active_subscriptions.is_empty()
    }
}

impl Message<Arc<SystemEvent>> for OperatorActor {
    type Reply = ();
    async fn handle(
        &mut self,
        _msg: Arc<SystemEvent>,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        tracing::info!(task_id = self.id, task_name = %self.task_name, "Operator received shutdown signal");
        ctx.stop();
    }
}

impl Message<Arc<InternalEvent>> for OperatorActor {
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
                    tracing::info!(task_id = self.id, task_name = %self.task_name, "No incoming connections are still active; Operator will shut down.");
                    self.outgoing_connections.broadcast_shutdown().await;
                    ctx.stop();
                }
            }
            Payload::Data(ev) => {
                // Map the incoming event to the right input conn_name
                match self.subscriptions.conn_name_for(msg.clone()) {
                    Ok(input_conn_name) => {
                        let (result_tx, mut result_rx) = mpsc::channel(100);
                        let (status_tx, mut status_rx) = mpsc::channel(100);

                        let operator_context = MuetlContext {
                            current_subscribers: HashMap::new(),
                            results: result_tx,
                            status: status_tx,
                            event_name: Some(ev.name.clone()),
                            event_headers: Some(ev.headers.clone()),
                        };

                        let mut operator = self.operator.take().unwrap();
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
                                operator
                                    .handle_event_for_conn(&operator_context, &conn_name, m)
                                    .await;
                                operator
                            }
                            .instrument(span),
                        );

                        let mut results_closed = false;
                        let mut status_closed = false;

                        loop {
                            tokio::select! {
                                res = result_rx.recv(), if !results_closed => {
                                    match res {
                                        Some(result) => {
                                            tracing::debug!(task_id = self.id, result = ?result, "Operator received result");
                                            match self.outgoing_connections.publish_to(Arc::new(result)).await {
                                                Ok(()) => {},
                                                Err(reason) => tracing::error!(task_id = self.id, error = %reason, "Operator failed to produce events"),
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
                                            tracing::debug!(task_id = self.id, status = ?status, "Operator received status");
                                            let update = StatusUpdate { status: status.clone(), id: self.id };
                                            self.monitor_chan.tell(Publish(update)).await.unwrap();
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

                        match fut.await {
                            Ok(operator) => {
                                self.operator = Some(operator);
                            }
                            Err(e) => {
                                tracing::error!(task_id = self.id, task_name = %self.task_name, error = %e, "Operator task panicked");
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

impl Actor for OperatorActor {
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
