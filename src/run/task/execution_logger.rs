use crate::protocol::Protocol;
use crate::run::prelude::*;

pub async fn execution_logger_task<P>(
    execution_log: String,
    mut from_workers: ExecutionInfoReceiver<P>,
) where
    P: Protocol,
{
}
