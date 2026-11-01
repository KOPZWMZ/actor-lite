use crate::{ActorContext, ActorHandle, core::ActorCore};

use tracing::trace;
/// actor 的**异步壳**。
///
/// 在 [`ActorCore`] 之上增加两件事：`run` 的签名（返回 future）和 [`spawn`](AsyncActor::spawn)。
/// 相比只实现 `ActorCore`，适合"跑在 tokio 任务里、用 `.await` 等网络/定时器"的 actor。
///
/// # 要求
///
/// - `Self: Send + 'static`：整个 actor 会被搬进 tokio 任务。握着 `Rc<...>`、
///   GPU 句柄这类 `!Send` 资源的 actor **不能**用它，请改用只要求 `ActorCore` 的方式，
///   或等线程壳；
/// - [`spawn`](AsyncActor::spawn) 内部调用 `tokio::spawn`，**调用线程必须有 tokio runtime**。
///
/// # 定义一个 actor
///
/// 需要两段 `impl`：关联类型与初始化给 [`ActorCore`]，`run` 给本 trait。
///
/// # 示例
///
/// ```no_run
/// # use actor_lite::{ActorCore, AsyncActor, ActorContext};
/// struct Counter {
///     target: u32,
/// }
///
/// impl ActorCore for Counter {
///     type Cmd = ();
///     type Event = u32;
///     type State = u32;
///     type Progress = f32;
///     fn init_state_and_progress(&self) -> (Self::State, Self::Progress) {
///         (0, 0.0)
///     }
/// }
///
/// impl AsyncActor for Counter {
///     fn run(self, mut context: ActorContext<Self>) -> impl Future<Output = ()> + Send + 'static {
///         async move {
///             let mut count = 0u32;
///             loop {
///                 if context.check_cmd().is_none() {
///                     // 没有命令时做自己的事
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
/// # let _ = Counter { target: 3 }.spawn();
/// ```
pub trait AsyncActor: ActorCore + Send + 'static {
    /// 启动 actor：把 `run` 返回的 future 交给 `tokio::spawn`。
    ///
    /// 返回 actor 的控制端 [`ActorHandle`]，外部通过它发送命令、读取状态/进度、订阅事件。
    ///
    /// # Panic
    ///
    /// 必须从 tokio runtime 上下文里调用，否则 panic（`there is no reactor running`）。
    /// 另外 `run` 自己 panic 只会让这个任务死掉，handle 不会收到通知。
    fn spawn(self) -> ActorHandle<Self> {
        let (init_state, init_progress) = self.init_state_and_progress();
        let (context, handle) = Self::channel(init_state, init_progress);
        tokio::spawn(self.run(context));
        trace!("spawn actor: {}", std::any::type_name::<Self>());
        handle
    }
    /// actor 的主循环。
    ///
    /// 拿到后端的 [`ActorContext`] 后自己决定循环结构：收命令、干活、更新状态/进度、发事件。
    /// 返回时 actor 结束，之后 handle 的 [`send_cmd`](ActorHandle::send_cmd) 会返回 `Err`。
    ///
    /// # 注意
    ///
    /// - 想让 [`ActorHandle::stop`] 生效，循环里必须监听取消信号
    ///   （`select!` 到 `ctx.cancel_clone().cancelled()`，或每轮 `is_cancelled()`）；
    /// - 所有 handle 都被 drop 后两件事同时发生：命令通道关闭、
    ///   [`wait_for_cmd`](ActorContext::wait_for_cmd) 返回 `None`。
    ///
    /// # 示例
    ///
    /// ```no_run
    /// # use actor_lite::{ActorCore, AsyncActor, ActorHandle, ActorContext};
    /// struct MyActor;
    /// impl ActorCore for MyActor {
    ///     type Cmd = ();
    ///     type Event = ();
    ///     type State = ();
    ///     type Progress = ();
    ///     fn init_state_and_progress(&self) -> (Self::State, Self::Progress) {
    ///         ((), ())
    ///     }
    /// }
    /// impl AsyncActor for MyActor {
    ///     fn run(self, mut context: ActorContext<Self>) -> impl Future<Output = ()> + Send + 'static {
    ///         async move {
    ///             let cancel = context.cancel_clone();
    ///             loop {
    ///                 tokio::select! {
    ///                     _ = cancel.cancelled() => break,
    ///                     cmd = context.wait_for_cmd() => {
    ///                         if cmd.is_none() { break; }   // 所有 handle 都没了
    ///                         // ...处理命令
    ///                     }
    ///                 }
    ///                 // 发送事件、更新进度和状态
    ///                 context.send_event(());
    ///                 context.update_progress(());
    ///                 context.update_state(());
    ///             }
    ///         }
    ///     }
    /// }
    /// ```
    fn run(self, context: ActorContext<Self>) -> impl Future<Output = ()> + Send + 'static;
}
