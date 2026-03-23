use std::any::TypeId;

use kameo::{actor::ActorId, error::SendError};
use thiserror::Error;

use crate::messages::RegisterRuntimeInfo;

#[derive(Error, Debug)]
pub enum RuntimeError {
    #[error("cannot find incoming connection for sender with id {id}")]
    UnknownIncomingConnection { id: u64 },
    #[error("cannot find connection named '{conn_name}'")]
    UnknownOutgoingConnection { conn_name: String },
    #[error("failed to publish event: {0}")]
    PublishError(String),
    #[error("type validation failed: duplicate event for type {:?}; must have a single event for each of {:?}", .event_type_id, .possible_type_ids)]
    DuplicateTypeError {
        event_type_id: TypeId,
        possible_type_ids: Vec<TypeId>,
    },
    #[error("type validation failed: number of events ({}) does not match the list of required types ({})", .num_events, .num_types)]
    AllOfTypeMismatch { num_events: usize, num_types: usize },
    #[error("type validation failed: all events must match single type {:?}, found illegal events {:?}", .event_type, .illegal_events)]
    TypeMismatch {
        event_type: TypeId,
        illegal_events: Vec<String>,
    },
    #[error("failed to resolve configuration: {0:?}")]
    ConfigResolutionError(Vec<String>),
    #[error("failed to register {0:?} with monitor: {1}")]
    MonitorRegistrationError(RegisterRuntimeInfo, SendError<RegisterRuntimeInfo>),
    #[error("supervised actor with id {0} has stopped but is not in root node mapping")]
    UnknownSupervisedActorStoppedError(ActorId),
    #[error("failed to start task '{}' in flow '{}': {}", .node_id, .flow_id, .msg)]
    FailedToBuildTaskError {
        node_id: String,
        flow_id: String,
        msg: String,
    },
}
