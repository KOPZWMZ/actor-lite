#![doc = include_str!("../README.md")]

#[cfg(feature = "async")]
mod async_actor;

mod core;

#[cfg(feature = "manager")]
pub mod manager;

mod thread_actor;
mod wait;

#[cfg(feature = "async")]
pub use async_actor::AsyncActor;

pub use core::{ActorContext, ActorCore, ActorHandle, CancelToken, SendCmdError};
pub use thread_actor::{
    CmdWait, STARTUP_TIMEOUT, SpawnError, ThreadActor, spawn_thread, try_spawn_thread,
};
pub use tokio::sync::{broadcast, mpsc, watch};
