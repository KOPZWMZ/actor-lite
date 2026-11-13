//! 回归测试：`is_alive() == false` 必须意味着 **actor 持有的资源已经析构完**。
//!
//! 背景（0.1.0 的 bug）：`AliveGuard` 挂在 `ActorContext` 上，`run(self, ctx)` 一返回
//! ctx 就被 drop → 立刻发送 `false`；而 actor 自身的字段（Vulkan 设备、解码器、字体库……）
//! 要等闭包结束、`actor` 被 drop 时才释放。这中间几十～几百毫秒的窗口里，
//! 调用方会以为"可以安全退出进程了"，于是 `exit()` 与后台线程的 `free()` 赛跑 ——
//! 表现就是偶发的 glibc 堆损坏（`corrupted double-linked list` / `corrupted size vs. prev_size`）
//! 与 `SIGABRT`。真实案例见 CHANGELOG 0.1.1。
//!
//! 这里把 actor 的 Drop **故意拖慢 200ms**，把那个窗口放大到必然可观测：
//! 只要 `is_alive()` 在 Drop 完成前变 `false`，下面的断言就会红。
//!
//! 两个用例分别覆盖两条真实路径：
//! - `stop()` 之后轮询 `is_alive()`
//! - **drop 掉最后一个 handle**（nsh_erika / GUI 的真实退出路径），通过 `alive_rx()` 观测

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use actor_lite::{ActorContext, ActorCore, ActorHandle, ThreadActor, spawn_thread};

/// actor 字段的析构探针：进入 / 离开各打一次标记。
struct DropProbe {
    entered: Arc<AtomicBool>,
    finished: Arc<AtomicBool>,
}

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.entered.store(true, Ordering::SeqCst);
        // 故意慢：放大"is_alive 已变 false 但资源还没放完"的窗口。
        // 真实场景里这是 vkDestroyDevice / 解码线程 join / fontdb 释放。
        std::thread::sleep(Duration::from_millis(200));
        self.finished.store(true, Ordering::SeqCst);
    }
}

struct SlowDropActor {
    // 字段按声明顺序 drop；probe 是唯一字段，所以它代表"actor 的资源"
    _probe: DropProbe,
}

impl ActorCore for SlowDropActor {
    type Cmd = ();
    type Event = ();
    type State = ();
    type Progress = ();

    fn init_state_and_progress(&self) -> (Self::State, Self::Progress) {
        ((), ())
    }
}

impl ThreadActor for SlowDropActor {
    fn run(self, mut ctx: ActorContext<Self>) {
        // 空转直到 Closed（stop 或所有 handle 都没了）
        while ctx.recv_cmd().is_some() {}
        // 返回 → self 的字段在这里析构（探针会被触发）
    }
}

fn spawn_slow() -> (ActorHandle<SlowDropActor>, Arc<AtomicBool>, Arc<AtomicBool>) {
    let entered = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    let (e, f) = (entered.clone(), finished.clone());
    let handle = spawn_thread("slow-drop", move || SlowDropActor {
        _probe: DropProbe {
            entered: e,
            finished: f,
        },
    });
    (handle, entered, finished)
}

#[test]
fn stop_then_is_alive_false_implies_resources_released() {
    let (handle, entered, finished) = spawn_slow();

    handle.stop();

    // 轮询到 is_alive() 变 false —— 注意：**不等任何额外时间**
    let deadline = Instant::now() + Duration::from_secs(5);
    while handle.is_alive() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(!handle.is_alive(), "actor 应在 5 秒内结束");

    // ★ 核心断言：看到 false 的那一刻，析构必须已经开始且做完
    assert!(
        entered.load(Ordering::SeqCst),
        "is_alive() 已变 false，但 actor 的 Drop 还没开始 —— \
         调用方此时退出进程会与后台线程的 free() 赛跑（0.1.0 的 bug）"
    );
    assert!(
        finished.load(Ordering::SeqCst),
        "is_alive() 已变 false，但 actor 的 Drop 还没做完 —— \
         这正是 0.1.0 那个偶发 SIGABRT / 堆损坏的成因"
    );
}

#[test]
fn last_handle_drop_implies_resources_released() {
    let (handle, _entered, finished) = spawn_slow();

    // 真实退出路径：drop 掉最后一个 handle，通过 alive_rx 观测（receiver 不会拖住 actor）
    let mut alive = handle.alive_rx();
    drop(handle);

    let deadline = Instant::now() + Duration::from_secs(5);
    while *alive.borrow_and_update() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(!*alive.borrow(), "最后一个 handle drop 后 actor 应结束");

    assert!(
        finished.load(Ordering::SeqCst),
        "alive_rx 报 false 时 actor 的 Drop 还没做完 —— \
         这就是 drop 播放器后立刻退出进程时偶发崩溃的原因"
    );
}
