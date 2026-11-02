# actor-lite

基于 tokio 的轻量 actor 抽象，分成两层：**核**只描述 actor「有什么」，**壳**决定「怎么跑」。

实现 [`ActorCore`] 就能拿到一对通道端点：

- [`ActorHandle`]：控制端，发命令、读状态/进度、订阅事件、注册监听
- [`ActorContext`]：后台端，收命令、更新状态/进度、广播事件

至于后台端交给谁驱动 —— tokio 任务、独立线程，还是自己手写的循环 ——
由壳 trait 决定，或者完全由你自己决定。这个 crate 只负责"控制"和"观察"这一层。

## 安装

```toml
[dependencies]
actor-lite = "0.1"
```

## 组成

| 项 | 说明 |
|------|------|
| [`ActorCore`] | 数据面：4 个关联类型 + `init_state_and_progress` + `channel()`，**不要求 `Send`** |
| [`AsyncActor`] | 异步壳：`run(self, ctx) -> impl Future + Send`，`spawn()` 即 `tokio::spawn` |
| [`ThreadActor`] | 线程壳：`run(self, ctx)`（**阻塞**），`spawn_thread(名字, 工厂)` 起一条独立线程；允许 `!Send` |
| [`ActorHandle`] | 控制端：同步发命令、读状态/进度、订阅事件、注册异步监听、`is_alive()` |
| [`ActorContext`] | 后台端：写入接口全是同步的；`wait_for_cmd()`（async）与 `recv_cmd_timeout()`（阻塞，仅线程壳） |
| [`CancelToken`] | 取消信号，幂等、可 clone 共享 |
| [`CmdWait`] / [`SendCmdError`] | 等待结果（`Timeout`/`Closed`）与发送失败原因（`Stopped`/`Closed`） |
| [`manager`] | 管理一组同类型子 actor，聚合它们的状态/进度/事件（仅异步壳） |

两种壳只差两点 —— **`run` 的签名**和**怎么起**，其余（通道、状态、事件、取消、监听语义）完全共用：

| | [`AsyncActor`] | [`ThreadActor`] |
|---|---|---|
| `run` 签名 | `-> impl Future + Send` | `fn run(self, ctx)`（阻塞） |
| 起法 | `spawn()` = `tokio::spawn` | `spawn_thread(名字, 工厂)` = `std::thread` |
| 需要 runtime | 是 | **否**（只用 `tokio::sync` 的纯数据结构） |
| 能用 `!Send` 资源 | 否 | **是** |
| 典型场景 | 网络 / 定时 / IO | GPU 句柄、`Rc<RefCell<_>>`、阻塞式第三方库 |

## 为什么是两段 impl

0.1.0 起原来的 `Actor` 拆成两层：

- [`ActorCore`]：四个关联类型 + `init_state_and_progress` + `channel()`，不带 `Send`；
- [`AsyncActor`]：`run` 的签名与 `spawn`，要求 `Send + 'static`（整个 actor 会被搬进 tokio 任务）。

这样通道、取消、监听这些语义只有一份实现，将来换壳（比如把 `!Send` 的资源丢到独立线程里）
不用重写一遍。代价就是定义一个 actor 要写两段 `impl`：关联类型给 `ActorCore`，`run` 给壳。

## 快速开始

### 1. 定义 actor

```rust
use std::future::Future;

use actor_lite::{ActorContext, ActorCore, AsyncActor};

struct Counter {
    target: u32,
}

enum CounterCmd {
    Reset,
}

impl ActorCore for Counter {
    type Cmd = CounterCmd;
    type Event = u32;
    type State = u32;
    type Progress = f32;

    fn init_state_and_progress(&self) -> (Self::State, Self::Progress) {
        (0, 0.0)
    }
}

impl AsyncActor for Counter {
    fn run(self, mut ctx: ActorContext<Self>) -> impl Future<Output = ()> + Send + 'static {
        async move {
            let cancel = ctx.cancel_clone();
            let mut count = 0u32;

            loop {
                tokio::select! {
                    // stop() 之后从这里退出
                    _ = cancel.cancelled() => break,
                    cmd = ctx.wait_for_cmd() => {
                        // None 表示所有 handle 都不在了
                        let Some(cmd) = cmd else { break };
                        match cmd {
                            CounterCmd::Reset => {
                                count = 0;
                                ctx.update_state(count);
                                ctx.update_progress(0.0);
                            }
                        }
                    }
                }

                if count >= self.target {
                    break;
                }
                count += 1;
                ctx.update_state(count);
                ctx.update_progress(count as f32 / self.target as f32);
                ctx.send_event(count);
            }
        }
    }
}
```

### 2. 启动并使用

```no_run
# use std::future::Future;
# use actor_lite::{ActorContext, ActorCore, AsyncActor};
# struct Counter { target: u32 }
# enum CounterCmd { Reset }
# impl ActorCore for Counter {
#     type Cmd = CounterCmd;
#     type Event = u32;
#     type State = u32;
#     type Progress = f32;
#     fn init_state_and_progress(&self) -> (Self::State, Self::Progress) { (0, 0.0) }
# }
# impl AsyncActor for Counter {
#     fn run(self, mut ctx: ActorContext<Self>) -> impl Future<Output = ()> + Send + 'static { async {} }
# }
# async fn example() {
// 启动：内部是 tokio::spawn，必须有 runtime
let handle = Counter { target: 10 }.spawn();

// 发命令：同步、不阻塞、不 panic，只在 actor 已停止时返回 Err
handle.send_cmd(CounterCmd::Reset).ok();

// 读状态/进度：同步取最新快照
let state: u32 = handle.get_state();
# let _ = state;

// 订阅事件流：拿到的是 broadcast::Receiver，可以自己 recv / try_recv
let mut events = handle.get_events_rx();
# let _ = &mut events;

handle.stop();
# }
```

## 命令 / 状态 / 进度 / 事件 的语义

| 用途 | 通道 | 语义 |
|------|------|------|
| 命令 | `mpsc`（无界） | 不丢、发送不阻塞、无背压；`send_cmd` 是同步的，唯一失败原因是 actor 已停止 |
| 状态 | `watch` | 只保留最新值，随时可读；中间值可能被跳过 |
| 进度 | `watch` | 同上，适合进度条/实时指标；**不要**当消息队列用 |
| 事件 | `broadcast` | 多订阅者、按容量有界；落后太多会收到 `Lagged`（丢事件，不重放） |

## 监听

[`ActorHandle`] 上的 `state_listen_async` / `progress_listen_async` / `events_listen_async`
返回 [`CancelToken`]：调用 `cancel()` 停止对应的监听任务。

> ⚠️ 这三个方法内部用 `tokio::spawn`，**调用线程必须有 tokio runtime**，否则会 panic
> （`there is no reactor running`）。同步宿主（GUI 回调、GTK/iOS 主线程等）请改用"拉"的方式：
> `get_state()` 轮询，或用 `get_events_rx()` 拿 `broadcast::Receiver` 后每帧 `try_recv()`。

## 线程 actor（同步壳）

`!Send` 的资源（GPU 句柄、`Rc<RefCell<_>>` 缓存树、阻塞式播放器/编辑器）装不进异步壳，
因为它们必须独占一条线程、又不需要 `.await`。这时用 [`ThreadActor`]。

### 1. 实现

```rust
# use std::rc::Rc;
# use std::cell::Cell;
# use std::time::Duration;
# use actor_lite::{ActorContext, ActorCore, CmdWait, ThreadActor};
# enum Cmd { Play, Pause }
# #[derive(Clone)] enum Ev { Started }
struct Player {
    // 假设这是某个 !Send 的第三方运行时句柄
    runtime: Rc<Cell<u32>>,
}

impl ActorCore for Player {
    type Cmd = Cmd;
    type Event = Ev;
    type State = u32;
    type Progress = ();
    fn init_state_and_progress(&self) -> (u32, ()) { (0, ()) }
}

impl ThreadActor for Player {
    fn run(mut self, mut ctx: ActorContext<Self>) {
        loop {
            match ctx.recv_cmd_timeout(Duration::from_millis(8)) {
                Ok(Cmd::Play) => { /* 这里可以随便阻塞：open/seek/decode/render */ }
                Ok(Cmd::Pause) => {}
                Err(CmdWait::Timeout) => { /* 周期性工作：出帧、上报进度 */ }
                Err(CmdWait::Closed) => break,   // stop() 或所有 handle 都没了
                _ => break,                      // CmdWait 是 non_exhaustive
            }
            let _ = &mut self.runtime;
            let _ = &mut ctx;
        }
    }
}
```

### 2. 启动：传工厂，不是实例

```rust
# use std::rc::Rc;
# use std::cell::Cell;
# use std::time::Duration;
# use actor_lite::{ActorContext, ActorCore, CmdWait, ThreadActor, spawn_thread};
# enum Cmd { Play }
# #[derive(Clone)] enum Ev {}
# struct Player { runtime: Rc<Cell<u32>> }
# impl ActorCore for Player {
#     type Cmd = Cmd; type Event = Ev; type State = u32; type Progress = ();
#     fn init_state_and_progress(&self) -> (u32, ()) { (0, ()) }
# }
# impl ThreadActor for Player {
#     fn run(self, mut ctx: ActorContext<Self>) {
#         loop {
#             match ctx.recv_cmd_timeout(Duration::from_millis(8)) {
#                 Ok(_) => {}
#                 Err(CmdWait::Timeout) => {}
#                 Err(CmdWait::Closed) => break,
#                 _ => break,
#             }
#         }
#     }
# }
// actor 在**新线程内部**构造，所以 !Send 也没关系
let handle = spawn_thread("player", || Player { runtime: Rc::new(Cell::new(0)) });

handle.send_cmd(Cmd::Play).unwrap();   // 同步、不阻塞、不 panic
let state = handle.get_state();
handle.stop();
```

> 为什么必须传工厂：`std::thread` 的闭包要求 `Send + 'static`，直接搬一个 `!Send` 的 actor 进去会
> `error[E0277]`。工厂闭包只捕获构造所需的数据（那些必须是 `Send`），actor 本身在线程里出生。
> 如果你的 actor 本来就是 `Send`（没有 `Rc` 之类），可以直接用方法形式：
> `actor.spawn_thread("名字")` —— 和异步壳的 `spawn()` 对称。

### 3. 启动失败怎么处理（GUI 必读）

`spawn_thread` 在启动失败时 panic（`expect`）。GUI / 服务器这类"不能让一个 actor 起不来就
拖垮整个程序"的场景用 `try_spawn_thread`：

```rust
# use std::time::Duration;
# use actor_lite::{ActorContext, ActorCore, CmdWait, SpawnError, ThreadActor, try_spawn_thread};
# enum Cmd {}
# #[derive(Clone)] enum Ev {}
# struct Player;
# impl ActorCore for Player {
#     type Cmd = Cmd; type Event = Ev; type State = u32; type Progress = ();
#     fn init_state_and_progress(&self) -> (u32, ()) { (0, ()) }
# }
# impl ThreadActor for Player {
#     fn run(self, mut ctx: ActorContext<Self>) {
#         loop {
#             match ctx.recv_cmd_timeout(Duration::from_millis(8)) {
#                 Ok(_) => {}
#                 Err(CmdWait::Timeout) => {}
#                 Err(CmdWait::Closed) => break,
#                 _ => break,
#             }
#         }
#     }
# }
match try_spawn_thread("player", || Player) {
    Ok(handle) => { /* 正常：把 handle 交给 UI */ }
    Err(e @ SpawnError::ConstructorPanicked(_)) | Err(e @ SpawnError::StartupTimeout(_)) => {
        // 多半是环境问题（文件打不开 / 驱动不兼容 / 设备被占用）→ 值得展示给用户
        eprintln!("启动失败，请在界面上提示：{e}");
    }
    Err(SpawnError::Thread(io)) => {
        // 系统级失败（线程数耗尽等）→ 记日志、降级即可
        eprintln!("无法创建线程：{io}");
    }
    Err(_) => {} // SpawnError 是 non_exhaustive
}
```

失败分类与处置：

| 变体 | 常见原因 | 给用户看？ |
|---|---|---|
| `SpawnError::ConstructorPanicked(msg)` | 构造函数 / `init_state_and_progress` panic：文件打不开、解码器缺失、驱动不兼容、设备被占用 | ✅ 展示 + 重试按钮 |
| `SpawnError::StartupTimeout(d)` | 初始化卡住（超过 `STARTUP_TIMEOUT`，默认 5s） | ✅ 提示"启动超时" |
| `SpawnError::Thread(io)` | 线程建不出来（资源耗尽） | ❌ 记日志 + 降级 |

**运行中**崩溃（`run` panic）不会被传播：只记 `error!` 日志，`is_alive()` 变 `false`、
`send_cmd` 返回 `Err`。UI 想感知它就用订阅，而不是轮询：

```rust
# use actor_lite::{ActorContext, ActorCore, CmdWait, ThreadActor, spawn_thread};
# struct W;
# impl ActorCore for W { type Cmd = (); type Event = (); type State = (); type Progress = ();
#     fn init_state_and_progress(&self) -> ((), ()) { ((), ()) } }
# impl ThreadActor for W { fn run(self, _ctx: ActorContext<Self>) {} }
let handle = spawn_thread("w", || W);
let mut alive = handle.alive_rx();          // watch::Receiver<bool>
// GUI 每帧：if *alive.borrow_and_update() == false { 切到"已停止" }
# let _ = &mut alive;
```

> 注意线程 actor 最怕的不是 panic（可观测、已隔离），而是**卡死**：阻塞调用永不返回时
> `is_alive()` 仍是 `true`。这类问题只能靠上层心跳（actor 每次 tick 更新 progress，
> UI 检测"多久没动"）和命令超时（发 `Ping`、等不到 `Pong` 就重建）来兜。

### 3. 主循环的三种等待形态（按需选）

| 你想怎样 | 写法 | 说明 |
|---|---|---|
| 不等待（每轮都有活干） | `ctx.check_cmd()` + 自己的 `sleep` | 忙轮询，空闲时 100% CPU |
| 等待 + 周期唤醒 | `ctx.recv_cmd_timeout(IDLE)` | 60fps → `IDLE = 8ms`；暂停 → 换成长超时 |
| 纯阻塞（只等命令/停止） | `ctx.recv_cmd()` | 0 唤醒、~0 CPU，`stop()` 仍能立刻叫醒它 |

`recv_cmd_timeout` 的三条出口正好对应循环里的三件事：`Ok(cmd)` 处理命令、`Err(CmdWait::Timeout)`
做周期性工作、`Err(CmdWait::Closed)` 退出。注意它是**真睡**（`park`），不是空转；
有命令时会立刻被唤醒，不会等满 `IDLE`。

### 4. 状态别放进阻塞等待里

「要一帧」这类高频信号**不要**用命令队列排队（延迟会累积），建议用共享标记 + 命令唤醒的组合，
或者把请求合并：一次 `check_cmd()` 把积压的 `Cmd::RequestFrame` 全部取走，只出一帧。

## 不用壳，自己驱动

`ActorCore` 不规定 actor 怎么跑。自己调 `channel` 拿两端，把 `ActorContext` 交给任意
runtime / 线程 / 自己写的循环即可：

```no_run
# use actor_lite::{ActorContext, ActorCore};
# struct MyActor;
# impl ActorCore for MyActor {
#     type Cmd = ();
#     type Event = ();
#     type State = ();
#     type Progress = ();
#     fn init_state_and_progress(&self) -> (Self::State, Self::Progress) { ((), ()) }
# }
let (ctx, handle) = MyActor::channel((), ());
// ctx 交给你的驱动逻辑，handle 留在控制侧
# let _ = handle;
# let _ = ctx;
```

注意：`recv_cmd_timeout` / `recv_cmd` 这些**阻塞**方法只挂在 `A: ThreadActor` 的 `ActorContext` 上
（编译期就限制住了）。自己手动驱动时，只要把循环写成 `impl ThreadActor for MyActor { fn run(...) }`
就等价于"自定义壳"，那些方法自然可用；否则就用 `check_cmd()` / `wait_for_cmd().await`。

## 用 manager 管理多个子 actor

需要统一管理多个同类型 actor 时用 [`manager::ActorManager`]。它本身也是 [`AsyncActor`]：

```no_run
# use std::future::Future;
# use actor_lite::{ActorContext, ActorCore, AsyncActor};
# use actor_lite::manager::{ActorManager, ManagerCmd, ManagerEvent};
# struct MyActor;
# impl ActorCore for MyActor {
#     type Cmd = ();
#     type Event = ();
#     type State = ();
#     type Progress = ();
#     fn init_state_and_progress(&self) -> (Self::State, Self::Progress) { ((), ()) }
# }
# impl AsyncActor for MyActor {
#     fn run(self, ctx: ActorContext<Self>) -> impl Future<Output = ()> + Send + 'static { async {} }
# }
# async fn example() {
let handle = ActorManager::<MyActor>::new().spawn();

// 启动子 actor，id 通过 oneshot 立刻拿到
let (id_tx, id_rx) = tokio::sync::oneshot::channel();
handle
    .send_cmd(ManagerCmd::SpawnActor { actor: MyActor, id_tx: Some(id_tx) })
    .unwrap();
let id = id_rx.await.unwrap();

// 聚合状态
let state = handle.get_state();
println!("共 {} 个子 actor", state.len());

// 事件流（含子 actor 事件与管理器的启动/停止事件）
let mut events = handle.get_events_rx();
while let Ok(event) = events.recv().await {
    match event {
        ManagerEvent::Spawned { id } => println!("actor {id} 已启动"),
        ManagerEvent::Stopped { id } => println!("actor {id} 已停止"),
        ManagerEvent::ChildEvent { id, event } => { /* ... */ }
    }
}
# let _ = id;
# }
```

## feature

```toml
[dependencies]
actor-lite = "0.1"                     # 默认 = async + manager

# 只用核 + 线程壳（零 tokio runtime）
actor-lite = { version = "0.1", default-features = false }
```

| feature | 默认 | 作用 |
|---|---|---|
| `async` | ✅ | [`AsyncActor`] + `*_listen_async`，引入 `tokio/{rt,time,macros}` |
| `manager` | ✅ | [`manager`] 模块（隐含 `async`） |

`default-features = false` 时依赖只剩 `tokio/sync` —— 纯数据结构，编出来的二进制里**没有**
runtime、没有定时器线程。日志走 `tracing`，不需要额外配置。

## 同步 API 的三条铁律

1. **handle 侧的发送是同步的**：`send_cmd` 不阻塞、不返回 future，唯一失败原因是 actor 已停止
   （`SendCmdError`）。所以能在 GUI 回调、`Drop`、同步宿主里直接调。
2. **`blocking` 系列只在裸线程里调**：`recv_cmd_timeout` / `recv_cmd` / `cancelled_blocking` 会
   `park` 当前线程。在线程 actor 的 `run` 里 ✅；在 tokio worker 上 ❌（会拖死 runtime）。
3. **只有"要等到有数据"才需要 async**：`recv().await` / `changed().await` / `cancelled().await`。
   线程壳用 `recv_cmd_timeout` 代替，于是对外 API 可以做到零 async。

## 状态

早期开发中，API 可能随时变化。

- **0.1.0**：
  - 拆分：原 `Actor` → [`ActorCore`]（数据面）+ [`AsyncActor`]（异步壳），新增 [`ThreadActor`]（线程壳）；
  - `send_cmd` 由 `async` 改为同步，新增 [`SendCmdError`]；
  - 命令通道由有界改为无界（`ActorManager` 少一个 const 参数）；
  - 新增 `is_alive()`、`recv_cmd_timeout()` / `recv_cmd()`、`CmdWait`、`CancelToken::cancelled_blocking()`；
  - 修复：`CancelToken` 的丢唤醒竞态；`update_state` / `update_progress` 在没有订阅者时丢值；
    `stop()` 唤不醒阻塞等待中的线程 actor；
  - feature：新增 `async` / `manager`，`default-features = false` 即"核 + 线程壳"，零 runtime。
