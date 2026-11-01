//! Actor 管理器模块。
//!
//! [`ActorManager`] 是一个特殊的 [`AsyncActor`]，它管理一组同类型的子 actor，
//! 并聚合它们的状态、进度和事件，对外暴露为单一 actor 接口。
//!
//! # 架构
//!
//! ```text
//! ActorManager (本身也是 AsyncActor)
//! ├── Child Actor 0  (id=1)
//! ├── Child Actor 1  (id=2)
//! └── Child Actor 2  (id=3)
//! ```
//!
//! - 每个子 actor 由 [`ActorManager`] 启动并分配一个唯一 `id`。
//! - 管理器自动监听子 actor 的状态、进度和事件，并聚合到自身的 [`ManagerState`] 和 [`ManagerProgress`] 中。
//! - 外部通过 [`ManagerCmd`] 向管理器发送命令：启动/停止子 actor，或转发命令给某个子 actor。
//!
//! # 限制
//!
//! manager 及其子 actor 目前都要求异步壳（[`AsyncActor`]）：子 actor 走 `tokio::spawn`，
//! 聚合用的 `tokio::time::interval` 也依赖 runtime。因此整个 manager 必须跑在 tokio runtime 里。
//!
//! # 示例
//!
//! ```no_run
//! # use actor_lite::{ActorCore, AsyncActor, ActorContext, ActorHandle};
//! # use actor_lite::manager::{ActorManager, ManagerCmd, ManagerState, ManagerProgress, ManagerEvent};
//! # struct MyActor;
//! # impl ActorCore for MyActor {
//! #     type Cmd = ();
//! #     type Event = ();
//! #     type State = ();
//! #     type Progress = ();
//! #     fn init_state_and_progress(&self) -> (Self::State, Self::Progress) { ((), ()) }
//! # }
//! # impl AsyncActor for MyActor {
//! #     fn run(self, context: ActorContext<Self>) -> impl std::future::Future<Output = ()> + Send + 'static {
//! #         async {}
//! #     }
//! # }
//! # async fn example() {
//! // 创建并启动 manager
//! let manager = ActorManager::<MyActor>::new();
//! let handle = manager.spawn();
//!
//! let (id_tx,id_rx) = tokio::sync::oneshot::channel();
//! handle.send_cmd(ManagerCmd::SpawnActor{actor:MyActor,id_tx:Some(id_tx)}).unwrap();
//! let id = id_rx.await.unwrap();
//! // 读取所有子 actor 的状态
//! let state: ManagerState<()> = handle.get_state();
//! assert_eq!(state.len(), 1);
//!
//! // 向指定子 actor 转发命令
//! handle.send_cmd(ManagerCmd::Forward { id: 1, cmd: () }).unwrap();
//!
//! // 停止指定子 actor
//! handle.send_cmd(ManagerCmd::StopActor(1)).unwrap();
//! # }
//! ```
use crate::async_actor::AsyncActor;
use crate::core::{ActorContext, ActorCore, ActorHandle, CancelToken};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::Duration;
use tracing::{debug, warn};

/// 子 actor 管理器。
///
/// `ActorManager` 本身实现了 [`AsyncActor`]，因此可以像普通 actor 一样被 [`AsyncActor::spawn`]
/// 启动和持有。
///
/// 它管理一组同类型的子 actor，对外提供：
/// - 启动/停止子 actor
/// - 向指定子 actor 转发命令
/// - 聚合所有子 actor 的状态、进度和事件
///
/// # 类型参数
///
/// - `A`: 子 actor 的类型，必须实现 [`AsyncActor`]。
/// - `E`: 管理器事件通道（`broadcast`）的容量。
///
/// # 示例
///
/// ```no_run
/// # use actor_lite::{ActorCore, AsyncActor, ActorContext};
/// # use actor_lite::manager::{ActorManager, ManagerCmd};
/// # struct MyActor;
/// # impl ActorCore for MyActor {
/// #     type Cmd = ();
/// #     type Event = ();
/// #     type State = ();
/// #     type Progress = ();
/// #     fn init_state_and_progress(&self) -> (Self::State, Self::Progress) { ((), ()) }
/// # }
/// # impl AsyncActor for MyActor {
/// #     fn run(self, context: ActorContext<Self>) -> impl std::future::Future<Output = ()> + Send + 'static {
/// #         async {}
/// #     }
/// # }
/// # async fn example() {
/// let manager = ActorManager::<MyActor>::new();
/// let handle = manager.spawn();
///
/// // 启动子 actor，返回分配的 id
/// let (id_tx,id_rx) = tokio::sync::oneshot::channel();
/// handle.send_cmd(ManagerCmd::SpawnActor{actor:MyActor,id_tx:Some(id_tx)}).unwrap();
/// let id = id_rx.await.unwrap();

/// # }
/// ```
pub struct ActorManager<A: AsyncActor, const E: usize = 256> {
    actors: HashMap<usize, ActorHandle<A>>,
    next_id: AtomicUsize,
    state: ManagerState<A::State>,
    progress: ManagerProgress<A::Progress>,
    listeners: HashMap<usize, CancelListeners>,
    update_interval: Duration,
}

impl<A: AsyncActor, const E: usize> ActorManager<A, E> {
    /// 创建一个空的 `ActorManager`。
    ///
    /// 初始时不管理任何子 actor，需要启动后通过 [`ManagerCmd::SpawnActor`] 添加。
    pub fn new() -> Self {
        Self {
            actors: HashMap::new(),
            next_id: AtomicUsize::new(1),
            state: ManagerState {
                actors: HashMap::new(),
            },
            progress: ManagerProgress {
                actors: HashMap::new(),
            },
            listeners: HashMap::new(),
            update_interval: Duration::from_millis(500),
        }
    }
    pub fn set_update_interval(&mut self, interval: Duration) {
        self.update_interval = interval;
    }
    fn get_id(&self) -> usize {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }
    fn spawn_actor(
        &mut self,
        actor: A,
        update_tx: mpsc::Sender<Update>,
        event_tx: broadcast::Sender<ManagerEvent<A::Event>>,
    ) -> usize {
        let id = self.get_id();
        let handle = actor.spawn();
        let progress = Self::listen_progress(&handle, id, update_tx.clone());
        let state = Self::listen_state(&handle, id, update_tx);
        let events = Self::listen_events(&handle, id, event_tx);
        self.actors.insert(id, handle);
        self.listeners.insert(
            id,
            CancelListeners {
                state,
                progress,
                events,
            },
        );
        id
    }

    fn stop_actor(&mut self, id: usize) {
        if let Some(handle) = self.actors.remove(&id) {
            handle.stop();
        };
        self.state.actors.remove(&id);
        self.progress.actors.remove(&id);
        if let Some(listeners) = self.listeners.remove(&id) {
            listeners.stop_all();
        }
    }

    fn listen_state(
        handle: &ActorHandle<A>,
        id: usize,
        update_tx: mpsc::Sender<Update>,
    ) -> CancelToken {
        handle.state_listen_async(move |_| {
            let state_tx = update_tx.clone();
            async move {
                let _ = state_tx.send(Update::State(id)).await;
            }
        })
    }

    fn listen_progress(
        handle: &ActorHandle<A>,
        id: usize,
        update_tx: mpsc::Sender<Update>,
    ) -> CancelToken {
        handle.progress_listen_async(move |_| {
            let progress_tx = update_tx.clone();
            async move {
                let _ = progress_tx.send(Update::Progress(id)).await;
            }
        })
    }
    fn listen_events(
        handle: &ActorHandle<A>,
        id: usize,
        event_tx: broadcast::Sender<ManagerEvent<A::Event>>,
    ) -> CancelToken {
        handle.events_listen_async(move |event| {
            let events_tx = event_tx.clone();
            async move {
                let _ = events_tx.send(ManagerEvent::ChildEvent { id, event });
            }
        })
    }
}

impl<A: AsyncActor, const E: usize> ActorCore for ActorManager<A, E> {
    type Cmd = ManagerCmd<A>;
    type State = ManagerState<A::State>;
    type Progress = ManagerProgress<A::Progress>;
    type Event = ManagerEvent<A::Event>;
    const EVENT_CAPACITY: usize = E;

    fn init_state_and_progress(&self) -> (Self::State, Self::Progress) {
        (self.state.clone(), self.progress.clone())
    }
}
impl<A: AsyncActor, const E: usize> AsyncActor for ActorManager<A, E> {
    fn run(self, context: ActorContext<Self>) -> impl Future<Output = ()> + Send + 'static {
        let mut context = context;
        let mut manager = self;
        let cancel = context.cancel_clone();

        async move {
            let (update_tx, mut update_rx) = mpsc::channel::<Update>(256);
            let mut push_interval = tokio::time::interval(manager.update_interval);
            push_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => { debug!("Manager 被 cancel 了！");break},
                    Some(cmd) = context.wait_for_cmd() => {
                        match cmd {
                            ManagerCmd::SpawnActor{actor, id_tx} => {
                                let id = manager.spawn_actor(actor, update_tx.clone(), context.get_events_clone());
                                if let Some(tx) = id_tx{
                                    let _ = tx.send(id);
                                }
                                let _ = context.send_event(ManagerEvent::Spawned { id });
                            }
                            ManagerCmd::StopActor(id) => {
                                manager.stop_actor(id);
                                context.update_state(manager.state.clone());
                                context.update_progress(manager.progress.clone());
                                let _ = context.send_event(ManagerEvent::Stopped { id });
                            }
                            ManagerCmd::Forward { id, cmd } => {
                                match manager.actors.get(&id) {
                                    Some(handle) => {
                                        if handle.send_cmd(cmd).is_err() {
                                            warn!("子Actor已经停止了: id={id}");
                                        }
                                    }
                                    None => warn!("没有找到id对应的子Actor: id={id}"),
                                }
                            }
                        }
                    }
                    Some(update) = update_rx.recv() => {
                        match update {
                            Update::State(id) => {
                                if let Some(handle) = manager.actors.get(&id) {
                                    let state = handle.get_state();
                                    manager.state.actors.insert(id, state);
                                    context.update_state(manager.state.clone());
                                }
                            }
                            Update::Progress(id) => {
                                if let Some(handle) = manager.actors.get(&id) {
                                    let progress = handle.get_progress();
                                    manager.progress.actors.insert(id, progress);
                                }
                            }
                        }
                        }
                    _ = push_interval.tick() => {
                        context.update_progress(manager.progress.clone());
                    }
                }
            }
        }
    }
}

enum Update {
    State(usize),
    Progress(usize),
}

struct CancelListeners {
    state: CancelToken,
    progress: CancelToken,
    events: CancelToken,
}

impl CancelListeners {
    fn stop_all(self) {
        self.state.cancel();
        self.progress.cancel();
        self.events.cancel();
    }
}

/// 管理器的命令枚举。
///
/// 通过 [`ActorHandle::send_cmd`] 发送给 [`ActorManager`]，控制子 actor 的生命周期和通信。
///
/// # 变体
///
/// | 变体 | 说明 |
/// |------|------|
/// | [`ManagerCmd::SpawnActor`] | 启动一个新的子 actor，可以选择返回分配的 `id`（通过oneshot通道） |
/// | [`ManagerCmd::StopActor`] | 停止指定 `id` 的子 actor |
/// | [`ManagerCmd::Forward`] | 向指定 `id` 的子 actor 转发一条命令 |
///
/// # 示例
///
/// ```no_run
/// # use actor_lite::{ActorCore, AsyncActor, ActorContext};
/// # use actor_lite::manager::{ActorManager, ManagerCmd};
/// # struct MyActor;
/// # impl ActorCore for MyActor {
/// #     type Cmd = String;
/// #     type Event = ();
/// #     type State = ();
/// #     type Progress = ();
/// #     fn init_state_and_progress(&self) -> (Self::State, Self::Progress) { ((), ()) }
/// # }
/// # impl AsyncActor for MyActor {
/// #     fn run(self, context: ActorContext<Self>) -> impl std::future::Future<Output = ()> + Send + 'static {
/// #         async {}
/// #     }
/// # }
/// # async fn example() {
/// let handle = ActorManager::<MyActor>::new().spawn();
///
/// // 启动子 actor
/// let (id_tx,id_rx) = tokio::sync::oneshot::channel();
/// handle.send_cmd(ManagerCmd::SpawnActor{actor:MyActor,id_tx:Some(id_tx)}).unwrap();
/// let id = id_rx.await.unwrap();
/// // 转发命令给 id=1 的子 actor
/// handle.send_cmd(ManagerCmd::Forward { id: 1, cmd: "hello".into() }).unwrap();
///
/// // 停止 id=1 的子 actor
/// handle.send_cmd(ManagerCmd::StopActor(1)).unwrap();
/// # }
/// ```

pub enum ManagerCmd<A: AsyncActor> {
    /// 启动一个新的子 actor。
    ///
    /// 管理器会为其分配唯一 `id`，并通过 [`ManagerEvent::Spawned`] 事件通知。
    SpawnActor {
        actor: A,
        id_tx: Option<oneshot::Sender<usize>>,
    },
    /// 停止指定 `id` 的子 actor。
    ///
    /// 停止后通过 [`ManagerEvent::Stopped`] 事件通知。
    StopActor(usize),
    /// 向指定 `id` 的子 actor 转发一条命令。
    ///
    /// 若 `id` 不存在，命令会被静默丢弃。
    Forward { id: usize, cmd: A::Cmd },
}

/// 管理器聚合的所有子 actor 状态。
///
/// 以 `id` 为键，存储每个子 actor 的最新状态。
/// 通过 [`ActorHandle::get_state`] 获取。
///
/// # 示例
///
/// ```no_run
/// # use actor_lite::{ActorCore, AsyncActor, ActorContext};
/// # use actor_lite::manager::{ActorManager, ManagerState};
/// # struct MyActor;
/// # impl ActorCore for MyActor {
/// #     type Cmd = ();
/// #     type Event = ();
/// #     type State = u32;
/// #     type Progress = ();
/// #     fn init_state_and_progress(&self) -> (Self::State, Self::Progress) { (0, ()) }
/// # }
/// # impl AsyncActor for MyActor {
/// #     fn run(self, context: ActorContext<Self>) -> impl std::future::Future<Output = ()> + Send + 'static {
/// #         async {}
/// #     }
/// # }
/// # async fn example() {
/// # let handle = ActorManager::<MyActor>::new().spawn();
/// let state: ManagerState<u32> = handle.get_state();
///
/// // 按 id 读取
/// if let Some(&42) = state.get(1) {
///     println!("actor 1 的状态是 42");
/// }
///
/// // 遍历所有子 actor 状态
/// for (id, s) in state.iter() {
///     println!("actor {id}: {s}");
/// }
///
/// println!("共 {} 个子 actor", state.len());
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ManagerState<State> {
    actors: HashMap<usize, State>,
}

impl<State> ManagerState<State> {
    /// 按 `id` 读取某个子 actor 的状态。
    pub fn get(&self, id: usize) -> Option<&State> {
        self.actors.get(&id)
    }

    /// 遍历所有子 actor 的状态，返回 `(id, &state)` 迭代器。
    pub fn iter(&self) -> impl Iterator<Item = (usize, &State)> {
        self.actors.iter().map(|(&id, s)| (id, s))
    }

    /// 当前管理的子 actor 数量。
    pub fn len(&self) -> usize {
        self.actors.len()
    }

    /// 是否没有子 actor。
    pub fn is_empty(&self) -> bool {
        self.actors.is_empty()
    }
}

/// 管理器聚合的所有子 actor 进度。
///
/// 以 `id` 为键，存储每个子 actor 的最新进度。
/// 进度会定时（每秒）推送到管理器的 `progress` 通道。
///
/// # 示例
///
/// ```no_run
/// # use actor_lite::{ActorCore, AsyncActor, ActorContext};
/// # use actor_lite::manager::{ActorManager, ManagerProgress};
/// # struct MyActor;
/// # impl ActorCore for MyActor {
/// #     type Cmd = ();
/// #     type Event = ();
/// #     type State = ();
/// #     type Progress = f32;
/// #     fn init_state_and_progress(&self) -> (Self::State, Self::Progress) { ((), 0.0) }
/// # }
/// # impl AsyncActor for MyActor {
/// #     fn run(self, context: ActorContext<Self>) -> impl std::future::Future<Output = ()> + Send + 'static {
/// #         async {}
/// #     }
/// # }
/// # async fn example() {
/// # let handle = ActorManager::<MyActor>::new().spawn();
/// let progress: ManagerProgress<f32> = handle.get_progress();
///
/// for (id, p) in progress.iter() {
///     println!("actor {id} 进度: {:.1}%", p * 100.0);
/// }
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ManagerProgress<Progress> {
    actors: HashMap<usize, Progress>,
}

impl<Progress> ManagerProgress<Progress> {
    /// 按 `id` 读取某个子 actor 的进度。
    pub fn get(&self, id: usize) -> Option<&Progress> {
        self.actors.get(&id)
    }

    /// 遍历所有子 actor 的进度，返回 `(id, &progress)` 迭代器。
    pub fn iter(&self) -> impl Iterator<Item = (usize, &Progress)> {
        self.actors.iter().map(|(&id, s)| (id, s))
    }

    /// 当前管理的子 actor 数量。
    pub fn len(&self) -> usize {
        self.actors.len()
    }

    /// 是否没有子 actor。
    pub fn is_empty(&self) -> bool {
        self.actors.is_empty()
    }
}

/// 管理器发出的事件枚举。
///
/// 聚合了所有子 actor 的事件以及管理器自身的生命周期事件。
/// 通过 [`ActorHandle::get_events_rx`] 或 `events_listen_async` 订阅。
///
/// # 变体
///
/// | 变体 | 说明 |
/// |------|------|
/// | [`ManagerEvent::ChildEvent`] | 某个子 actor 发出的事件，附带其 `id` |
/// | [`ManagerEvent::Spawned`] | 新子 actor 已启动，附带分配的 `id` |
/// | [`ManagerEvent::Stopped`] | 子 actor 已停止，附带其 `id` |
///
/// # 示例
///
/// ```no_run
/// # use actor_lite::{ActorCore, AsyncActor, ActorContext};
/// # use actor_lite::manager::{ActorManager, ManagerEvent};
/// # struct MyActor;
/// # impl ActorCore for MyActor {
/// #     type Cmd = ();
/// #     type Event = String;
/// #     type State = ();
/// #     type Progress = ();
/// #     fn init_state_and_progress(&self) -> (Self::State, Self::Progress) { ((), ()) }
/// # }
/// # impl AsyncActor for MyActor {
/// #     fn run(self, context: ActorContext<Self>) -> impl std::future::Future<Output = ()> + Send + 'static {
/// #         async {}
/// #     }
/// # }
/// # async fn example() {
/// let handle = ActorManager::<MyActor>::new().spawn();
/// let mut rx = handle.get_events_rx();
///
/// while let Ok(event) = rx.recv().await {
///     match event {
///         ManagerEvent::ChildEvent { id, event } => {
///             println!("actor {id} 发出事件: {event}");
///         }
///         ManagerEvent::Spawned { id } => {
///             println!("新 actor 已启动，id={id}");
///         }
///         ManagerEvent::Stopped { id } => {
///             println!("actor {id} 已停止");
///         }
///     }
/// }
/// # }
/// ```
#[derive(Debug, Clone)]
pub enum ManagerEvent<Event> {
    /// 某个子 actor 发出的事件。
    ChildEvent { id: usize, event: Event },
    /// 新子 actor 已启动，`id` 为其分配的标识。
    Spawned { id: usize },
    /// 子 actor 已停止，`id` 为其标识。
    Stopped { id: usize },
}
