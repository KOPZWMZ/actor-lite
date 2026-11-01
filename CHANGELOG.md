# Changelog

本项目的所有重要变更都会记录在此文件中。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [Semantic Versioning](https://semver.org/lang/zh-CN/)。

## [Unreleased]

## [0.1.0] - 2026-11-01

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
