# actor-lite

基于 tokio 的轻量 actor 抽象：**核**只描述 actor 有什么，**壳**决定它怎么跑。

## 是什么

实现 [`ActorCore`] 会拿到一对通道端点：

- [`ActorHandle`]：控制端 —— 同步发命令、读状态/进度快照、订阅事件、注册监听；
- [`ActorContext`]：后台端 —— 收命令、更新状态/进度、广播事件。

通道、取消、监听只有一份实现；后台端交给 tokio 任务（[`AsyncActor`]）、独占线程（[`ThreadActor`]），
还是自己手写的循环（直接调 `channel()`），由你选。线程壳可以装 `!Send` 资源
（GPU 句柄、`Rc<RefCell<_>>`、阻塞式第三方库）。

| | [`AsyncActor`] | [`ThreadActor`] |
| --- | --- | --- |
| 怎么跑 | `spawn()` → `tokio::spawn` | `spawn_thread("名字", 工厂)` → `std::thread` |
| 需要 tokio runtime | 是 | 否 |
| 能用 `!Send` 资源 | 否 | 是 |

早期开发中（0.1.x），API 可能变化；变更见 [CHANGELOG.md](CHANGELOG.md)。

## 快速开始

```toml
[dependencies]
actor-lite = "0.1"
```

关联类型与初始值给 [`ActorCore`]，`run` 给壳。最小可跑的例子：

```rust
use std::future::Future;

use actor_lite::{ActorContext, ActorCore, AsyncActor};

struct Counter {
    target: u32,
}

impl ActorCore for Counter {
    type Cmd = ();
    type Event = u32;
    type State = u32;
    type Progress = f32;

    fn init_state_and_progress(&self) -> (u32, f32) {
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
                    // None 表示所有 handle 都不在了
                    cmd = ctx.wait_for_cmd() => if cmd.is_none() { break },
                }

                if count < self.target {
                    count += 1;
                    ctx.update_state(count);
                    ctx.update_progress(count as f32 / self.target as f32);
                    ctx.send_event(count);
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let handle = Counter { target: 10 }.spawn();

    handle.send_cmd(()).ok();       // 同步、不阻塞、不 panic
    let _state = handle.get_state(); // 最新快照
    let mut states = handle.state_rx(); // 要"变了再重绘"就订阅，不需要 runtime
    let mut events = handle.get_events_rx(); // broadcast::Receiver

    let _ = (&mut states, &mut events);
    handle.stop();
}
```

只想用线程壳（零 tokio runtime、可装 `!Send`）？把依赖改成 `default-features = false`，
用 `spawn_thread("名字", 工厂)` 启动：actor 在新线程内部构造，`!Send` 也没关系，
完整示例见下方文档。

## 重点注意

1. **`send_cmd` 是同步的**：不阻塞、不返回 future、不 panic，唯一失败原因是 actor 已停止
   （[`SendCmdError`]）。所以在 GUI 回调、`Drop`、同步宿主里都能直接调。
2. **状态/进度是 `watch`，只保留最新值**：连推 100 次，读者只看到最后一帧。要一条不漏，
   用 `get_events_rx()`（`broadcast`）。
3. **`*_listen_async` 内部 `tokio::spawn`**：调用线程必须有 tokio runtime，否则 panic。
   同步宿主改用 `state_rx()` / `progress_rx()` 每帧查，或 `get_events_rx()` 每帧 `try_recv()`。
4. **阻塞 API 只能在线程 actor 里用**：`recv_cmd_timeout` / `recv_cmd` / `cancelled_blocking`
   会 `park` 当前线程，在 tokio worker 上会拖死 runtime。
5. **线程壳的两个约定**：用工厂 `spawn_thread` 构造；`is_alive() == false` 表示
   **资源已释放**，可以安全退出或重建。

## 文档

完整用法（通道语义、监听、线程壳、manager、feature 与同步 API 规则）见
[`docs/guide.md`](docs/guide.md)；版本变更见 [CHANGELOG.md](CHANGELOG.md)。

## 许可证

MIT
