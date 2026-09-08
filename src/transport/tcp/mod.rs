use bytes::Bytes;

use crate::brokers::{BrokerError, BrokerErrorKind};
use crate::protocol::{ErrorCode, Response};

pub mod connection;
pub mod dispatcher;

pub(crate) fn error_response(error: BrokerError) -> Response {
    let code = match error.kind {
        BrokerErrorKind::Internal => ErrorCode::Internal,
        BrokerErrorKind::InvalidArgument => ErrorCode::InvalidArgument,
        BrokerErrorKind::ResourceNotFound => ErrorCode::ResourceNotFound,
        BrokerErrorKind::ResourceConfigConflict => ErrorCode::ResourceConfigConflict,
        BrokerErrorKind::NotAuthorized => ErrorCode::NotAuthorized,
        BrokerErrorKind::Fenced => ErrorCode::Fenced,
        BrokerErrorKind::NotMember => ErrorCode::NotMember,
        BrokerErrorKind::SlowConsumer => ErrorCode::SlowConsumer,
        BrokerErrorKind::Storage => ErrorCode::StorageError,
    };
    match error
        .details
        .and_then(|details| serde_json::to_vec(&details).ok())
    {
        Some(details) => Response::error_with_details(code, error.message, Bytes::from(details)),
        None => Response::error(code, error.message),
    }
}
