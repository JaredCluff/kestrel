pub mod auth;
pub mod message;

pub use message::{DisplayInfo, KestrelMessage, MsgKind, OsInfo, Payload};
// Note: auth re-exports (hmac_response, verify_response) will be added in Task 3
