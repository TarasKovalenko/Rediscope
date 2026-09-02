//! rediscope internals, exposed as a library so the integration tests can
//! drive the Redis layer directly.

pub mod app;
pub mod config;
pub mod input;
pub mod osc52;
pub mod redis_client;
pub mod tree;
pub mod ui;
