//! 线程壳：把 actor 跑在一条独占的 `std::thread` 上。
//!
//! 和异步壳的区别只有两点：`run` 是阻塞签名、启动方式是 `std::thread`。
//! 通道、状态、事件、取消的语义完全一致（共用 `core`）。

use std::time::Duration;
use std::{panic::AssertUnwindSafe, time::Instant};

use tracing::{error, trace};

use crate::core::{ActorContext, ActorCore, ActorHandle};
use crate::wait;
/// actor 的**线程壳**。允许 `!Send` 的资源（GPU 句柄、`Rc<RefCell<_>>`、阻塞式第三方库）。
///
/// 不用 `Send`：整个 actor 只会被搬到一条新线程的栈上，从不跨线程移动。
pub trait ThreadActor: ActorCore {
    /// actor 的主循环，**阻塞**签名。
    ///
    /// 典型写法：
    ///
    /// ```no_run
    /// # use std::time::Duration;
    /// # use actor_lite::{ActorContext, ActorCore, CmdWait, ThreadActor};
    /// # struct Player;
    /// # impl ActorCore for Player {
    /// #     type Cmd = (); type Event = (); type State = (); type Progress = ();
    /// #     fn init_state_and_progress(&self) -> ((), ()) { ((), ()) }
    /// # }
    /// impl ThreadActor for Player {
    ///     fn run(self, mut ctx: ActorContext<Self>) {
    ///         loop {
    ///             match ctx.recv_cmd_timeout(Duration::from_millis(8)) {
    ///                 Ok(_cmd) => { /* 处理命令 */ }
    ///                 Err(CmdWait::Timeout) => { /* 周期性工作：出帧 / 上报进度 */ }
    ///                 Err(CmdWait::Closed) => break,   // stop() 或所有 handle 都没了
    ///                 _ => break,                      // CmdWait 是 non_exhaustive，留个兜底
    ///             }
    ///         }
    ///     }
    /// }
    /// ```
    fn run(self, ctx: ActorContext<Self>);

    /// 在一条新线程上启动 actor，返回控制端。
    ///
    /// **只对 `Send` 的 actor 可用。** `!Send` 的 actor（`Rc<...>`、GPU 句柄、
    /// 阻塞式第三方运行时）不能这样起 —— 实例没法搬进 `std::thread` 的闭包
    /// （`error[E0277]: cannot be sent between threads safely`），
    /// 请改用 [`spawn_thread`] 传**工厂**，让 actor 在新线程内部构造。
    ///
    /// ```no_run
    /// # use actor_lite::{ActorContext, ActorCore, ThreadActor};
    /// # struct Worker;
    /// # impl ActorCore for Worker {
    /// #     type Cmd = (); type Event = (); type State = (); type Progress = ();
    /// #     fn init_state_and_progress(&self) -> ((), ()) { ((), ()) }
    /// # }
    /// # impl ThreadActor for Worker {
    /// #     fn run(self, _ctx: ActorContext<Self>) {}
    /// # }
    /// // Worker 是 Send（没有 Rc 之类）→ 可以直接用方法形式
    /// let handle = Worker.spawn_thread("worker");
    /// handle.stop();
    /// ```
    fn spawn_thread(self, name: &str) -> ActorHandle<Self>
    where
        Self: Send + 'static,
    {
        spawn_thread(name, move || self)
    }
}
/// 在一条新线程上启动 actor。
///
/// # 为什么是工厂而不是实例
///
/// `!Send` 的 actor 无法被搬进 `std::thread` 的闭包（`E0277`），所以 actor 必须在
/// 新线程内部**构造**。传给闭包的只有构造它所需的数据，那些必须是 `Send`。
///
/// ```text
/// 主线程                     新线程
/// spawn_thread(name, f)  →  let actor = f();     ← !Send 资源在这里出生
///                            ctx/handle 建好
/// recv(handle) ←──────────── handle 回传
/// ```
///
/// 返回前会阻塞等 handle 回传（μs 级），所以返回的 handle 一定带着初始化好的 state。
///
/// 如果你的 actor 是 `Send` 的，也可以用更顺手的方法形式：[`ThreadActor::spawn_thread`]。
///
/// # Panic
///
/// 启动失败会 panic（`expect`），需要优雅处理请改用 [`try_spawn_thread`]。
/// `run` 里的 panic **不会**传播：被 `catch_unwind` 兜住，只记一条 error 日志，
/// 之后 [`ActorHandle::is_alive`] 变 `false`、`send_cmd` 返回 `Err`。
pub fn spawn_thread<A, F>(name: &str, factory: F) -> ActorHandle<A>
where
    A: ThreadActor + 'static,
    F: FnOnce() -> A + Send + 'static,
{
    try_spawn_thread(name, factory).expect("failed to start thread actor")
}

/// 启动时最多等多久让 actor 交出 handle。超过就判定"构造阶段卡住了"。
pub const STARTUP_TIMEOUT: Duration = Duration::from_secs(5);

/// 线程 actor 启动失败的原因。
#[derive(Debug)]
#[non_exhaustive]
pub enum SpawnError {
    /// `std::thread` 建不出来（线程数耗尽、资源不足、名字不合法）。
    Thread(std::io::Error),
    /// 构造函数或 `init_state_and_progress` panic 了（含 panic 消息）。
    ConstructorPanicked(String),
    /// 构造阶段超过 [`STARTUP_TIMEOUT`] 还没交出 handle（初始化卡住了）。
    StartupTimeout(Duration),
}

impl std::fmt::Display for SpawnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Thread(e) => write!(f, "failed to spawn actor thread: {e}"),
            Self::ConstructorPanicked(msg) => write!(f, "actor failed to start: {msg}"),
            Self::StartupTimeout(d) => {
                write!(f, "actor did not hand over its handle within {d:?}")
            }
        }
    }
}

impl std::error::Error for SpawnError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Thread(e) => Some(e),
            _ => None,
        }
    }
}

/// 在一条新线程上启动 actor，失败时返回 [`SpawnError`] 而不是 panic。
///
/// GUI/服务器这类"不能让一个 actor 起不来就拖垮整个程序"的场景用这个，
/// 其余情况用 [`spawn_thread`] 更省事。
///
/// # 为什么是工厂而不是实例
///
/// `!Send` 的 actor 无法被搬进 `std::thread` 的闭包（`error[E0277]`），所以 actor 必须
/// 在新线程内部**构造**。传给闭包的只有构造它所需的数据，那些必须是 `Send`。
///
/// # 错误分类
///
/// - [`SpawnError::ConstructorPanicked`]：构造函数或初始化 panic（多为环境问题：
///   文件打不开、驱动不兼容、设备被占用）—— **值得展示给用户**；
/// - [`SpawnError::StartupTimeout`]：初始化卡住；
/// - [`SpawnError::Thread`]：系统级失败，通常只记日志。
pub fn try_spawn_thread<A, F>(name: &str, factory: F) -> Result<ActorHandle<A>, SpawnError>
where
    A: ThreadActor + 'static,
    F: FnOnce() -> A + Send + 'static,
{
    let thread_name = name.to_owned();
    let (handle_tx, handle_rx) = std::sync::mpsc::channel::<Result<ActorHandle<A>, SpawnError>>();

    std::thread::Builder::new()
        .name(thread_name.clone())
        .spawn(move || {
            // ① 构造：把 panic 变成可回传的错误，而不是让线程静默死掉
            let actor = match std::panic::catch_unwind(AssertUnwindSafe(factory)) {
                Ok(actor) => actor,
                Err(payload) => {
                    let _ = handle_tx.send(Err(SpawnError::ConstructorPanicked(panic_message(
                        &*payload,
                    ))));
                    return;
                }
            };

            let (init_state, init_progress) = match std::panic::catch_unwind(AssertUnwindSafe(
                || actor.init_state_and_progress(),
            )) {
                Ok(v) => v,
                Err(payload) => {
                    let _ = handle_tx.send(Err(SpawnError::ConstructorPanicked(panic_message(
                        &*payload,
                    ))));
                    return;
                }
            };

            let (ctx, handle) = A::channel(init_state, init_progress);

            // 点亮 waker：之后 send_cmd / stop 就能 unpark 这条线程
            handle.attach_current_thread();

            if handle_tx.send(Ok(handle)).is_err() {
                return; // 调用方已经放弃等待
            }

            trace!(
                actor = std::any::type_name::<A>(),
                thread = %thread_name,
                "thread actor started"
            );

            // ② 运行：panic 也不能拖垮进程
            if let Err(payload) = std::panic::catch_unwind(AssertUnwindSafe(|| actor.run(ctx))) {
                error!(
                    actor = std::any::type_name::<A>(),
                    panic = %panic_message(&*payload),
                    "thread actor panicked"
                );
            }
        })
        .map_err(SpawnError::Thread)?;

    match handle_rx.recv_timeout(STARTUP_TIMEOUT) {
        Ok(result) => result,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            Err(SpawnError::StartupTimeout(STARTUP_TIMEOUT))
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Err(
            // 线程在交出 handle 之前就死了（上面两条 catch_unwind 之外的路径）
            SpawnError::ConstructorPanicked("actor thread died during startup".to_owned()),
        ),
    }
}

/// 从 `catch_unwind` 的 payload 里取出可读信息。
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<non-string panic payload>".to_owned())
}

/// [`ActorContext::recv_cmd_timeout`](crate::thread_actor::ActorContext::recv_cmd_timeout) 的等待结果。
/// `Timeout` 不是错误，是正常的空闲 tick。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CmdWait {
    /// 这段时间内没有命令。
    Timeout,
    /// actor 已停止，或命令通道关闭 —— 该退出循环了。
    Closed,
}

impl<A: ThreadActor> ActorHandle<A> {
    /// 只在 `spawn_thread` 里调用：把共享 waker 绑定到调用线程。
    pub(crate) fn attach_current_thread(&self) {
        self.waker.attach_current();
    }
}

impl<A: ThreadActor> ActorContext<A> {
    /// 阻塞等待下一条命令，最多等 `dur`。会把当前线程 park 住。
    ///
    /// 有命令 → `Ok(cmd)`；`dur` 内无命令 → `Err(CmdWait::Timeout)`；
    /// 已停止/通道关闭 → `Err(CmdWait::Closed)`。会被 `send_cmd` / `stop` 提前唤醒。
    /// 传 `Duration::MAX` 等价于无限等待。
    pub fn recv_cmd_timeout(&mut self, dur: Duration) -> Result<A::Cmd, CmdWait> {
        if self.cancel.is_cancelled() {
            return Err(CmdWait::Closed);
        }
        let waker = self.waker.waker();
        let deadline = Instant::now().checked_add(dur);
        // 两个字段分别可变/不可变借用 → 合法
        let cmd_fut = self.cmd_rx.recv();
        let cancel_fut = self.cancel.cancelled();

        match wait::drive_either(cmd_fut, cancel_fut, deadline, &waker) {
            Some(wait::Either2::First(Some(cmd))) => Ok(cmd),
            Some(wait::Either2::First(None)) => Err(CmdWait::Closed), // 所有 handle 都没了
            Some(wait::Either2::Second(())) => Err(CmdWait::Closed),  // 被 stop()
            None => Err(CmdWait::Timeout),
        }
    }
    /// 一直阻塞到有命令、或 actor 被停止。
    pub fn recv_cmd(&mut self) -> Option<A::Cmd> {
        match self.recv_cmd_timeout(Duration::MAX) {
            Ok(cmd) => Some(cmd),
            Err(CmdWait::Closed) => None,
            Err(CmdWait::Timeout) => unreachable!("无限等待不会超时"),
        }
    }
}
