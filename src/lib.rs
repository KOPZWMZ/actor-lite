#![doc = include_str!("../README.md")]

#[cfg(feature = "async")]
mod async_actor;
mod core;
#[cfg(feature = "manager")]
pub mod manager;
#[cfg(feature = "async")]
pub use async_actor::AsyncActor;

pub use core::{ActorContext, ActorCore, ActorHandle, CancelToken, SendCmdError};
