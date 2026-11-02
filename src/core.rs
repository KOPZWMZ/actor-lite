use core::fmt;
use tokio::sync::{broadcast, mpsc, watch};
use tracing::warn;

use crate::wait::{self, SharedWaker};
/// Actor 的**数据面** trait。
///
/// `ActorCore` 只定义 actor「有什么」，不定义「怎么跑」。每个 actor 有四个关联类型：
///
/// | 类型 | 说明 |
/// |------|------|
/// | [`Cmd`](ActorCore::Cmd) | 命令枚举，外部通过它向 actor 发送指令 |
/// | [`Event`](ActorCore::Event) | 事件类型，actor 对外广播的异步消息 |
/// | [`State`](ActorCore::State) | 低频状态，变化不频繁，可随时读取最新值 |
/// | [`Progress`](ActorCore::Progress) | 高频进度，适合进度条、实时指标等场景 |
///
/// 实现之后，[`ActorCore::channel`] 会给你两端：[`ActorContext`]（后台端，收命令、更新状态、
/// 广播事件）和 [`ActorHandle`]（控制端，发命令、读状态、订阅事件）。
///
/// # 怎么跑
///
/// 「怎么跑」由壳 trait 决定，`ActorCore` 自己不关心：
///
/// - [`AsyncActor`](crate::AsyncActor)：`run` 返回一个 future，[`AsyncActor::spawn`](crate::AsyncActor::spawn) 用 `tokio::spawn` 起任务；
/// - 也可以完全不用壳：自己调用 [`ActorCore::channel`]，再把 [`ActorContext`] 交给任意
///   runtime 或线程驱动 —— 本 crate 不规定 actor 怎么运行。
///
/// # 生命周期
///
/// 1. 启动 actor（如 [`AsyncActor::spawn`](crate::AsyncActor::spawn)），得到 [`ActorHandle`]
/// 2. 通过 [`ActorHandle`] 发送命令、读取状态/进度、订阅事件
/// 3. 调用 [`ActorHandle::stop`] 显式停止；所有 handle 都被 drop 后命令通道关闭，
///    `run` 里的等待会返回 `None`，actor 自然退出
///
/// # 与其他 trait 的关系
///
/// 0.1.0 起原 `Actor` 拆成两层：关联类型与初始化留在 `ActorCore`，
/// `run`/`spawn` 移到壳 trait（[`AsyncActor`](crate::AsyncActor)）。因此定义一个 actor 需要两段 `impl`。
///
/// # 示例
///
/// ```no_run
/// # use actor_lite::{ActorCore, AsyncActor, ActorContext};
/// struct Counter {
///     target: u32,
/// }
///
/// enum CounterCmd {
///     Reset,
/// }
///
/// impl ActorCore for Counter {
///     type Cmd = CounterCmd;
///     type Event = u32;
///     type State = u32;
///     type Progress = f32;
///
///     fn init_state_and_progress(&self) -> (Self::State, Self::Progress) {
///         (0, 0.0)
///     }
///
/// }
/// impl AsyncActor for Counter {
///     fn run(self, context: ActorContext<Self>) -> impl std::future::Future<Output = ()> + Send + 'static {
///         let mut context = context;
///         async move {
///             let mut count = 0u32;
///             loop {
///                 if let Some(cmd) = context.check_cmd() {
///                     match cmd {
///                         CounterCmd::Reset => {
///                             count = 0;
///                             context.update_state(count);
///                             context.update_progress(0.0);
///                         }
///                     }
///                 }
///                 if count >= self.target {
///                     break;
///                 }
///                 count += 1;
///                 context.update_state(count);
///                 context.update_progress(count as f32 / self.target as f32);
///                 context.send_event(count);
///             }
///         }
///     }
/// }
/// ```
pub trait ActorCore: Sized {
    /// actor 命令枚举
    ///
    /// 例如：
    /// ```
    /// enum MyCmd {
    ///     Start,
    ///     Pause,
    ///     //...
    /// }
    /// ```
    type Cmd: Send + 'static;
    /// actor 里的事件结构体，需要实现`Clone`
    type Event: Clone + Send + 'static;
    /// actor 里的低频状态，如果不需要，用 `type State = ();`。
    type State: Clone + Send + Sync + 'static;
    /// actor 里的高频状态，如果没有高频进度时，用 `type Progress = ();`。
    type Progress: Clone + Send + Sync + 'static;

    /// actor 的事件通道容量
    const EVENT_CAPACITY: usize = 32;
    /// 创建 actor 的通道。
    /// ActorContext 是 actor 的后台端，ActorHandle 是 actor 的控制端。
    fn channel(
        init_state: Self::State,
        init_progress: Self::Progress,
    ) -> (ActorContext<Self>, ActorHandle<Self>) {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (state_tx, state_rx) = watch::channel(init_state);
        let (event_tx, _) = broadcast::channel(Self::EVENT_CAPACITY);
        let (progress_tx, progress_rx) = watch::channel(init_progress);

        let waker = SharedWaker::default();
        let cancel = CancelToken::with_waker(waker.clone());
        let context_cancel = cancel.clone();
        let (alive_tx, alive_rx) = watch::channel(true);
        let context = ActorContext {
            cmd_rx,
            event_tx: event_tx.clone(),
            state_tx,
            progress_tx,
            cancel: context_cancel,
            waker: waker.clone(),
            _alive: AliveGuard(alive_tx),
        };
        let handle = ActorHandle {
            cmd_tx,
            event_tx,
            state_rx,
            progress_rx,
            cancel,
            waker,
            alive_rx,
        };
        (context, handle)
    }

    fn init_state_and_progress(&self) -> (Self::State, Self::Progress);
}

/// actor 的控制端。
///
/// 由 [`ActorCore::channel`] 创建，通常由外部控制方持有。用于发送命令、读取状态/进度、
/// 订阅事件，以及注册异步监听。
///
/// # 线程安全
///
/// `ActorHandle<A>` 的 `Send`/`Sync` 只取决于四个关联类型，**与 actor 本身是否 `Send` 无关**：
/// 即使 `A` 是 `!Send`（比如握着 `Rc<...>` 资源的线程 actor），它照样可以放进 GUI props、
/// 跨线程传递、塞进 `Arc`。
///
/// # 发送命令
///
/// [`send_cmd`](ActorHandle::send_cmd) 是**同步**的：不阻塞、不返回 future，唯一的失败原因是
/// actor 已停止（或所有后台端已退出）。因此可以在 GUI 回调、`Drop`、同步宿主里直接调用。
///
/// # 示例
///
/// ```no_run
/// # use actor_lite::{ActorCore, AsyncActor, ActorHandle, ActorContext};
/// # struct MyActor;
/// # impl ActorCore for MyActor {
/// #    type Cmd = ();
/// #    type Event = ();
/// #    type State = ();
/// #    type Progress = ();
/// #    fn init_state_and_progress(&self) -> (Self::State, Self::Progress) {
/// #        ((), ())
/// #    }
/// # }
/// # impl AsyncActor for MyActor {
/// #    fn run(self, context: ActorContext<Self>) -> impl Future<Output = ()> + Send + 'static {
/// #         async {}
/// #     }
/// # }
/// # async fn example() {
/// let (context, handle) = MyActor::channel( (), ());
///
/// handle.send_cmd(()).unwrap();
/// let state = handle.get_state();
///
/// let listener = handle.state_listen_async(|s| async move { });
/// listener.cancel();
/// # }
/// ```
pub struct ActorHandle<A: ActorCore> {
    cmd_tx: mpsc::UnboundedSender<A::Cmd>,
    pub(crate) state_rx: watch::Receiver<A::State>,
    pub(crate) event_tx: broadcast::Sender<A::Event>,
    pub(crate) progress_rx: watch::Receiver<A::Progress>,
    cancel: CancelToken,
    pub(crate) waker: SharedWaker,
    pub(crate) alive_rx: watch::Receiver<bool>,
}

impl<A: ActorCore> ActorHandle<A> {
    /// 向 actor 发送一条命令。
    ///
    /// 同步、不阻塞（命令通道是无界的），不会阻塞调用线程。失败只会在 actor 已停止
    /// （[`ActorHandle::stop`] 之后，或后台端已退出）时发生。
    pub fn send_cmd(&self, cmd: A::Cmd) -> Result<(), SendCmdError> {
        if self.cancel.is_cancelled() {
            warn!("send cmd failed: actor stopped");
            return Err(SendCmdError::Closed);
        }
        self.cmd_tx.send(cmd).map_err(|_| {
            warn!("send cmd failed: channel closed");
            SendCmdError::Stopped
        })?;
        self.waker.wake();
        Ok(())
    }
    /// 获取 actor 的最新状态。
    pub fn get_state(&self) -> A::State {
        self.state_rx.borrow().to_owned()
    }

    /// 获取 actor 的最新进度。
    ///
    /// 进度通道是 `watch`：读者只会看到**最新值**，中间值可能被跳过。
    /// 适合进度条、实时指标，不要拿它当消息队列用。
    pub fn get_progress(&self) -> A::Progress {
        self.progress_rx.borrow().to_owned()
    }

    /// 订阅事件流，返回一个 `broadcast` 接收端。
    ///
    /// 这是**同步**用法：在 GUI 帧循环里 `try_recv()` 即可，不需要 async runtime。
    /// 落后太多时 `recv()` 会返回 `Lagged`，需要自行决定是跳过还是重试。
    pub fn get_events_rx(&self) -> broadcast::Receiver<A::Event> {
        self.event_tx.subscribe()
    }

    /// 请求 actor 停止。
    ///
    /// 只是发出取消信号（[`CancelToken::cancel`]），**不会**强行中断 `run`：
    /// 实现者需要在自己的循环里检查取消信号（如 `select!` 到 `cancel.cancelled()`，
    /// 或每轮 `is_cancelled()`），否则 actor 会继续跑到 `run` 返回为止。
    pub fn stop(&self) {
        self.cancel.cancel();
    }
    pub fn is_alive(&self) -> bool {
        *self.alive_rx.borrow()
    }

    /// 订阅"存活状态"，actor 结束时变成 `false`。
    ///
    /// 用来替代轮询 [`is_alive`](ActorHandle::is_alive)：GUI 每帧
    /// `has_changed()` 一次即可（`watch` 语义，中间变化可丢，只关心最终值）。
    ///
    /// 后台端 drop 后 sender 也随之消失，因此 `changed()` 可能返回 `Err`
    /// —— 那同样意味着 actor 已经结束，按 `false` 处理即可。
    pub fn alive_rx(&self) -> watch::Receiver<bool> {
        self.alive_rx.clone()
    }
}
impl<A: ActorCore> Clone for ActorHandle<A> {
    fn clone(&self) -> Self {
        Self {
            cmd_tx: self.cmd_tx.clone(),
            state_rx: self.state_rx.clone(),
            event_tx: self.event_tx.clone(),
            progress_rx: self.progress_rx.clone(),
            cancel: self.cancel.clone(),
            waker: self.waker.clone(),
            alive_rx: self.alive_rx.clone(),
        }
    }
}
/// actor 的后台端。
///
/// 由 [`ActorCore::channel`] 创建，交给 `run` 持有。用于接收命令、更新状态/进度、广播事件。
///
/// # 同步 API
///
/// 这里的写入方法（[`update_state`](ActorContext::update_state)、
/// [`update_progress`](ActorContext::update_progress)、
/// [`send_event`](ActorContext::send_event)）和 [`check_cmd`](ActorContext::check_cmd)
/// 都是同步的，不需要 runtime；只有 [`wait_for_cmd`](ActorContext::wait_for_cmd) 返回 future。
///
/// # 收命令
///
/// - [`check_cmd`](ActorContext::check_cmd)：非阻塞 `try_recv`，适合"每轮循环顺带看一眼"；
/// - [`wait_for_cmd`](ActorContext::wait_for_cmd)：`recv().await`，适合异步壳；
/// - `recv_cmd_timeout` / `recv_cmd`：**阻塞**版本，只在 [`ThreadActor`](crate::ThreadActor)
///   的上下文里才有（见 `thread_actor` 模块）。
///
/// 命令通道关闭（所有 [`ActorHandle`] 都 drop 了）后两者分别返回 `None`，
/// 实现者据此退出循环即可。
pub struct ActorContext<A: ActorCore> {
    pub(crate) cmd_rx: mpsc::UnboundedReceiver<A::Cmd>,
    state_tx: watch::Sender<A::State>,
    event_tx: broadcast::Sender<A::Event>,
    progress_tx: watch::Sender<A::Progress>,
    pub(crate) cancel: CancelToken,
    pub(crate) waker: SharedWaker,
    _alive: AliveGuard,
}
impl<A: ActorCore> ActorContext<A> {
    /// 获取额外的事件发送端。
    ///
    /// 用于把事件发送端交给别的线程/任务（比如 IO 回调里直接发事件出队列）。
    pub fn get_events_clone(&self) -> broadcast::Sender<A::Event> {
        self.event_tx.clone()
    }
    /// 发送事件（同步，永不失败；没有订阅者时事件被丢弃）。
    pub fn send_event(&self, event: A::Event) {
        let _ = self.event_tx.send(event);
    }
    /// 更新状态。
    ///
    /// `watch` 语义：读者只会看到最新值，中间值可能被跳过。
    /// 状态应是低频、可幂等覆盖的数据；高频数据用 [`update_progress`](ActorContext::update_progress)。
    pub fn update_state(&self, state: A::State) {
        self.state_tx.send_replace(state);
    }
    /// 更新进度。
    ///
    /// `watch` 语义：读者只会看到最新值，中间值可能被跳过 —— 对进度条是优点，
    /// 但**不要**拿它当消息队列（每一帧都要的话走事件）。
    pub fn update_progress(&self, progress: A::Progress) {
        self.progress_tx.send_replace(progress);
    }
    /// 等待下一条命令（async）。
    ///
    /// 命令通道关闭时返回 `None`，可据此结束 `run`。
    pub fn wait_for_cmd(&mut self) -> impl Future<Output = Option<A::Cmd>> {
        self.cmd_rx.recv()
    }
    /// 非阻塞检查命令。
    ///
    /// 立即返回：有命令就 `Some`，没有或通道已关闭都是 `None`。
    pub fn check_cmd(&mut self) -> Option<A::Cmd> {
        self.cmd_rx.try_recv().ok()
    }

    /// 获取取消信号，用于在自己循环里检查/等待停止。
    ///
    /// 异步壳一般写 `select! { _ = cancel.cancelled() => break, ... }`。
    pub fn cancel_clone(&self) -> CancelToken {
        self.cancel.clone()
    }
    /// actor 是否已被要求停止。
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }
}
/// 取消信号。
///
/// 可在多个任务/线程间 clone 共享，clone 出来的所有副本共享同一个取消状态：
///
/// - [`cancel`](CancelToken::cancel)：同步、幂等、永不失败。即使当下没有等待者，
///   后来的 [`is_cancelled`](CancelToken::is_cancelled) 也会是 `true`；
/// - [`is_cancelled`](CancelToken::is_cancelled)：同步查询，适合轮询/每轮检查；
/// - [`cancelled`](CancelToken::cancelled)：async 等待，已经取消时立即返回。
///
/// 典型来源是 [`ActorHandle::stop`]；[`ActorHandle`] 上的 `*_listen_async` 返回的也是它。
#[derive(Clone, Debug)]
pub struct CancelToken {
    tx: watch::Sender<bool>,
    waker: SharedWaker,
}
impl Default for CancelToken {
    fn default() -> Self {
        Self::new()
    }
}
impl CancelToken {
    /// 创建一个未取消的信号。
    pub fn new() -> Self {
        let tx = watch::channel(false).0;
        let waker = SharedWaker::default();
        Self { tx, waker }
    }
    pub(crate) fn with_waker(waker: SharedWaker) -> Self {
        Self {
            tx: watch::channel(false).0,
            waker,
        }
    }
    /// 检查取消信号。
    pub fn is_cancelled(&self) -> bool {
        *self.tx.borrow()
    }
    /// 等待取消信号；已经取消时立即返回。
    pub async fn cancelled(&self) {
        let mut rx = self.tx.subscribe();
        // wait_for 会先拿当前值比一次，再等变化 → 不存在"通知丢失"的窗口
        let _ = rx.wait_for(|cancelled| *cancelled).await;
    }
    /// 发送取消信号（幂等）。
    ///
    /// 唤醒所有正在 [`cancelled`](CancelToken::cancelled) 上等待的任务；
    /// 信号不会"过期"，之后再调用 `cancelled()` 也会立即返回。
    pub fn cancel(&self) {
        self.tx.send_replace(true);
        self.waker.wake();
    }
    /// 阻塞等待取消（线程 actor 用；异步壳请用 `cancelled()`）。
    pub fn cancelled_blocking(&self) {
        if self.is_cancelled() {
            return;
        }
        let waker = self.waker.waker();
        let _ = wait::drive_until(self.cancelled(), None, &waker);
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SendCmdError {
    /// 已经调用过 [`ActorHandle::stop`]，actor 不再接受新命令。
    Stopped,
    /// 后台端已结束：`run` 返回、panic，或所有 [`ActorContext`] 都已 drop。
    Closed,
}

impl fmt::Display for SendCmdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Stopped => "actor has been stopped",
            Self::Closed => "actor is closed",
        })
    }
}

impl std::error::Error for SendCmdError {}
/// ctx 被 drop（正常返回或 unwind）时把存活状态置 false。
struct AliveGuard(watch::Sender<bool>);
impl Drop for AliveGuard {
    fn drop(&mut self) {
        self.0.send_replace(false);
    }
}
