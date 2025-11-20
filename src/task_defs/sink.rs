use std::{future::Future, sync::Arc};


use crate::messages::event::Event;

use super::{HasInputs, MuetlSinkContext};

pub trait Sink: HasInputs + Send + Sync {
    /// Handle an Event sent to conn_name. The handler should disregard the conn_name
    /// inside the Event; that will be the name of the output conn from the source.
    fn handle_event_for_conn(
        &mut self,
        ctx: &MuetlSinkContext,
        conn_name: &String,
        ev: Arc<Event>,
    ) -> impl Future<Output = ()> + Send;

    /// Wrapper around `handle_event_for_conn()` that performs runtime type checking against the types
    /// specified by `get_outputs()`.
    fn handle_event(
        &mut self,
        ctx: &MuetlSinkContext,
        conn_name: &String,
        ev: Arc<Event>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async move { self.handle_event_for_conn(ctx, conn_name, ev.clone()).await }
    }
}
