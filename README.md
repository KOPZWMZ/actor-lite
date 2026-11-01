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
| [`ActorHandle`] | 控制端：同步发命令、读状态/进度、订阅事件、注册异步监听 |
| [`ActorContext`] | 后台端：写入接口全是同步的，只有 `wait_for_cmd()` 返回 future |
| [`CancelToken`] | 取消信号，幂等、可 clone 共享 |
| [`manager`] | 管理一组同类型子 actor，聚合它们的状态/进度/事件 |

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

## 状态

早期开发中，API 可能随时变化。

- 0.1.0（未发布）：原 `Actor` 拆为 [`ActorCore`] + [`AsyncActor`]；`send_cmd` 由 async 改为同步；
  命令通道改为无界；修复 `CancelToken` 的丢唤醒竞态。
