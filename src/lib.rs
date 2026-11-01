#![doc = include_str!("../README.md")]

mod async_actor;
mod core;
pub mod manager;
pub use async_actor::AsyncActor;
pub use core::{ActorContext, ActorCore, ActorHandle, CancelToken};
