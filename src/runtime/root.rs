use std::sync::Arc;

use kameo::prelude::*;

use crate::{
    flow::Flow,
    registry::{DaemonDef, Registry},
};

/// The Root is the core runtime that controls everything else inside muetl. It's responsible for:
/// 1. Parsing a Muetl flow definition and returning an error if it is invalid.
/// 2. Negotiating types for each edge in the flow and creating Connections.
/// 3. Spawning Actors in the order they need to be.
/// 4. Monitoring the status registry to determine how execution should proceed.
/// 5. Running the flow to completion and optionally exiting.
///
/// Note that in the event that more than one flow is running in a single muetl runtime, multiple Root actors may be present.
pub struct Root {
    id: u64,
    /// A fully validated Flow that will be managed by this Root.
    flow: Flow,
    /// A link to the current Registry that stores TaskDefs
    registry: Arc<Registry>,
}

// Where should validation of a Flow take place? Should Root actors do that on startup, or should that be pulled out to the runtime?
// Doing it in the root would be the most decentralized

type ValidationResult = Result<(), Vec<String>>;
impl Root {
    fn validate_flow(&mut self) -> ValidationResult {
        let mut errors = vec![];

        for (node_id, node) in &mut self.flow.nodes {
            match self.registry.daemon_for(&node.task_id, &node.configuration) {
                Ok(Some(d)) => {}
                Ok(None) => {}
                Err(e) => return Err(vec![e]),
            }
        }

        if errors.len() > 0 {
            Err(errors)
        } else {
            Ok(())
        }
    }

    fn validate_daemon(&mut self, node_id: &String, daemon: &DaemonDef) -> ValidationResult {
        let mut errors = vec![];

        if errors.len() > 0 {
            Err(errors)
        } else {
            Ok(())
        }
    }
}

impl Actor for Root {
    type Args = Self;
    type Error = ();
    async fn on_start(args: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(args)
    }
    async fn on_stop(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        reason: ActorStopReason,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}
