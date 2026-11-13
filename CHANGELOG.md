# Changelog

本项目的所有重要变更都会记录在此文件中。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [Semantic Versioning](https://semver.org/lang/zh-CN/)。

## [Unreleased]

## [0.1.1] - 2026-11-14

### Fixed

- **`is_alive()` / `alive_rx()` 的语义修正（线程壳）**：存活状态现在在 **actor 的资源完全析构之后**
  才变 `false`，而不是 `run()` 一返回就变。
  - **旧行为**：`AliveGuard` 挂在 `ActorContext` 上，`run(self, ctx)` 一返回 ctx 就被 drop →
    立刻发送 `false`；而 actor 自身的字段（`presenter`、Vulkan 设备、解码器、字体库……）
    要等闭包结束、`actor` 被 drop 时才释放。于是"看到 `is_alive() == false`"和
    "后台线程真的退干净"之间存在几十～几百毫秒的窗口。
  - **后果**：调用方据此认为"可以安全退出了"，进程 `exit()` 与后台线程的 `free()` 赛跑，
    偶发 glibc 堆损坏（`corrupted double-linked list` / `corrupted size vs. prev_size`）→ `SIGABRT`。
    典型触发场景：短时间内反复创建/销毁 actor 后立即退出（测试、命令行工具、播放器切换片源）。
  - **现在**：守卫由线程闭包持有（内部新增 `ActorContext::take_alive_guard()`），
    在 `actor.run(ctx)` 消费掉 actor、其字段析构之后**最后**才 drop。
    `is_alive() == false` 从此表示"线程已结束且资源已释放"，可用于判断能否退出进程。
- **新增回归测试** `tests/is_alive_semantics.rs`：在 actor 的 `Drop` 里故意停留 200ms，
  把"`is_alive()` 已变 `false` 但资源未释放"的窗口从几毫秒放大到 200ms，
  于是断言可以**确定性**地检查"观测到 `false` 的那一刻，析构必须已经做完"。
  - 覆盖两条真实退出路径：`stop()` + 轮询 `is_alive()`，以及 **drop 掉最后一个 handle** +
    通过 `alive_rx()` 观测（GUI / 播放器句柄的真实退出路径）。
  - 已验证：把守卫放回 `ActorContext`（模拟 0.1.0）时两条用例**必红**（0.00s，非偶发）；
    修复后全绿。这个 bug 从此不会再无声无息地回归。
- 异步壳（`AsyncActor`）不受影响：actor 与 `ctx` 都在 future 内部，完成时一并析构，语义本来就正确。

### Changed

- `is_alive()` / `alive_rx()` 的**文档**明确写出 `false` 的准确含义（资源已释放，不只是 `run` 返回），
  并说明反方向不成立：`true` 不代表 actor 干得动活（阻塞卡死时仍是 `true`）。

## [0.1.0] - 2026-11-02

### Breaking

- 原 `Actor` 拆成两层：`ActorCore`（关联类型 + `init_state_and_progress` + `channel()`，不要求 `Send`）
  与 `AsyncActor`（`run` + `spawn`，要求 `Send + 'static`）。定义一个 actor 现在需要两段 `impl`。
- `ActorHandle::send_cmd` 由 `async` 改为**同步**，错误类型改为 `SendCmdError`。
- 命令通道由有界改为无界，删除 `CMD_CAPACITY`；`ActorManager<A, C, E>` 变为 `ActorManager<A, E>`。
- `*_listen_async` 移到异步壳，只挂在 `A: AsyncActor` 的 handle 上；内部 `tokio::spawn`，
  调用线程必须有 tokio runtime。

### Added

- `ThreadActor`：阻塞签名的线程壳，允许 `!Send` 资源。启动方式两种：
  自由函数 `spawn_thread(名字, 工厂)`（**唯一**能给 `!Send` 用的形式，actor 在新线程内部构造），
  以及 `Send` 的 actor 可用的方法形式 `ThreadActor::spawn_thread(self, 名字)`。
  两种都不需要 tokio runtime。
- `try_spawn_thread()` + `SpawnError`（`Thread` / `ConstructorPanicked(msg)` / `StartupTimeout`）
  与 `STARTUP_TIMEOUT`：启动失败不再只有 panic 一条路，GUI 可以优雅降级。
- `ActorHandle::alive_rx()`：`watch::Receiver<bool>` 形式的存活订阅，替代轮询 `is_alive()`；
  `is_alive()` 内部同步改为 `watch` 通道实现。
- `run` panic 时把 panic 消息记进日志（`catch_unwind` 的 payload 会被取出解析）。
- `ActorContext::recv_cmd_timeout()` / `recv_cmd()`：同步阻塞等待命令，带超时与取消；
  `CmdWait`（`Timeout` / `Closed`）表示三种出口。
- `ActorHandle::is_alive()`：`run` 返回或 panic 之后变 `false`。
- `CancelToken::cancelled_blocking()`：阻塞等待取消（线程壳用）。
- feature：`async`（默认）、`manager`（默认，隐含 `async`）；
  `default-features = false` 只剩 `tokio/sync`，编出零 runtime 的核 + 线程壳。

### Fixed

- `CancelToken` 的丢唤醒竞态（原实现 `notified()` 与 `notify_waiters()` 之间存在窗口，
  监听任务可能永久挂住）。改为 `watch::Sender<bool>` 的状态语义，实现上不可能再丢。
- `update_state` / `update_progress` 在没有任何订阅者时不写入新值（`watch::Sender::send`
  在 receiver 数为 0 时直接返回 `Err`），改用 `send_replace`。
- `stop()` 唤不醒阻塞等待中的线程 actor：等待时改为同时监听"命令"与"取消"
  （`wait::drive_either`），`Duration::MAX` 的纯阻塞也能被立刻打断。
- `run` panic 时静默死线程：现在 `catch_unwind` 兜住并记 `error!` 日志，`is_alive()` 可查。


## [0.0.2] - 2026-09-27

### Fixed

- 修复 `ActorHandle` 被 clone 后，任意一个副本 drop 会意外取消整个 actor 的 bug。
  现在 `ActorHandle` drop 不再触发取消，只有显式调用 `.stop()` 才停止 actor。
  所有 handle 都被 drop 后，actor 通过 `cmd_rx` 关闭自然退出。

### Changed

- `ActorHandle` 不再实现 `Drop`。停止 actor 请显式调用 `.stop()`。
- `ManagerCmd::SpawnActor` 添加了 `Option<oneshot::Sender<usize>>` 参数，用于立刻接收返回的 id。
- 所有的`listen` 要求的闭包取消了bool 返回值，监听取消必须依靠返回的 CancelToken

## [0.0.1] - 2026-09-26

### Added

- 首个版本。
- `Actor` trait、`ActorHandle`、`ActorContext`。
- `ActorManager` 管理器。
- `CancelToken` / `CancelHandle`。
