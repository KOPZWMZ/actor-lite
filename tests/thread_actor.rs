//! 线程壳测试：全部是普通 `#[test]`，**不依赖 tokio runtime**。

use std::cell::Cell;
use std::rc::Rc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use actor_lite::{
    ActorContext, ActorCore, ActorHandle, CmdWait, SpawnError, ThreadActor, spawn_thread,
    try_spawn_thread,
};

#[derive(Debug)]
enum Cmd {
    Inc,
    Boom,
}

#[derive(Clone, Debug, PartialEq)]
enum Ev {
    Count(u32),
    Tick,
}

/// `hits` 是 `Rc<Cell<_>>` —— `!Send`，正是线程壳存在的理由。
struct RcCounter {
    hits: Rc<Cell<u32>>,
    tx: mpsc::Sender<Ev>, // 测试侧的观察窗口（Send）
    idle: Duration,
}

impl ActorCore for RcCounter {
    type Cmd = Cmd;
    type Event = Ev;
    type State = u32;
    type Progress = ();

    fn init_state_and_progress(&self) -> (Self::State, Self::Progress) {
        (0, ())
    }
}

impl ThreadActor for RcCounter {
    fn run(self, mut ctx: ActorContext<Self>) {
        let mut count = 0u32;
        loop {
            match ctx.recv_cmd_timeout(self.idle) {
                Ok(Cmd::Inc) => {
                    count += 1;
                    self.hits.set(count);
                    ctx.update_state(count);
                    let _ = self.tx.send(Ev::Count(count));
                }
                Ok(Cmd::Boom) => panic!("boom"),
                Err(CmdWait::Timeout) => {
                    let _ = self.tx.send(Ev::Tick);
                }
                Err(CmdWait::Closed) => break,
                _ => break, // CmdWait 是 non_exhaustive，留个兜底
            }
        }
    }
}

fn wait_until(mut cond: impl FnMut() -> bool, limit: Duration) {
    let deadline = Instant::now() + limit;
    while !cond() {
        assert!(Instant::now() < deadline, "等待条件超时");
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// 起一个 actor，返回观察窗口和控制端。
fn spawn(idle: Duration, name: &str) -> (mpsc::Receiver<Ev>, ActorHandle<RcCounter>) {
    let (tx, rx) = mpsc::channel();
    let handle = spawn_thread(name, move || RcCounter {
        hits: Rc::new(Cell::new(0)),
        tx,
        idle,
    });
    (rx, handle)
}

#[test]
fn not_send_actor_runs_without_runtime() {
    let (rx, h) = spawn(Duration::from_millis(50), "t-basic");

    h.send_cmd(Cmd::Inc).unwrap();
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Ev::Count(1)
    );
    assert_eq!(h.get_state(), 1);

    h.send_cmd(Cmd::Inc).unwrap();
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Ev::Count(2)
    );
    assert_eq!(h.get_state(), 2);

    h.stop();
}

#[test]
fn handle_is_send_sync_even_for_not_send_actor() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ActorHandle<RcCounter>>();
}

#[test]
fn idle_timeout_fires() {
    let (rx, h) = spawn(Duration::from_millis(5), "t-tick");
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(500)).unwrap(),
        Ev::Tick
    );
    h.stop();
}

/// 关键用例：纯阻塞（`Duration::MAX`，平时 0 唤醒）时，`stop()` 必须能把它叫醒。
#[test]
fn stop_wakes_a_blocked_actor() {
    let (rx, h) = spawn(Duration::MAX, "t-stop");
    h.stop();
    wait_until(|| !h.is_alive(), Duration::from_secs(1));
    assert!(!h.is_alive(), "stop() 没能唤醒阻塞中的 actor");
    assert!(h.send_cmd(Cmd::Inc).is_err());
    drop(rx);
}

#[test]
fn panic_flips_is_alive() {
    let (_rx, h) = spawn(Duration::from_millis(50), "t-panic");
    h.send_cmd(Cmd::Boom).unwrap();
    wait_until(|| !h.is_alive(), Duration::from_secs(1));
    assert!(!h.is_alive(), "run panic 之后 is_alive 应该变 false");
    assert!(h.send_cmd(Cmd::Inc).is_err());
}

#[test]
fn dropping_all_handles_ends_the_actor() {
    let (rx, h) = spawn(Duration::MAX, "t-drop");
    drop(h);
    // actor 退出 ⇒ RcCounter 被 drop ⇒ self.tx 被 drop ⇒ rx 变成 Disconnected
    assert!(
        matches!(
            rx.recv_timeout(Duration::from_secs(1)),
            Err(mpsc::RecvTimeoutError::Disconnected)
        ),
        "所有 handle 都 drop 之后 actor 应该退出"
    );
}

#[test]
fn command_is_not_delayed_by_idle_timeout() {
    let (rx, h) = spawn(Duration::from_secs(10), "t-fast");
    let t0 = Instant::now();
    h.send_cmd(Cmd::Inc).unwrap();
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Ev::Count(1)
    );
    assert!(
        t0.elapsed() < Duration::from_millis(500),
        "命令响应被 idle 超时拖住了：{:?}",
        t0.elapsed()
    );
    h.stop();
}

/// `Send` 的 actor 可以直接用**方法形式**启动（与 `AsyncActor::spawn()` 对称）。
#[test]
fn send_actor_can_use_method_form() {
    struct SendWorker;

    impl ActorCore for SendWorker {
        type Cmd = u32;
        type Event = ();
        type State = u32;
        type Progress = ();

        fn init_state_and_progress(&self) -> (Self::State, Self::Progress) {
            (0, ())
        }
    }

    impl ThreadActor for SendWorker {
        fn run(self, mut ctx: ActorContext<Self>) {
            let mut sum = 0u32;
            while let Some(n) = ctx.recv_cmd() {
                sum += n;
                ctx.update_state(sum);
            }
        }
    }

    let handle = SendWorker.spawn_thread("t-method");
    assert!(handle.is_alive());

    handle.send_cmd(2).unwrap();
    wait_until(|| handle.get_state() == 2, Duration::from_secs(1));

    handle.send_cmd(3).unwrap();
    wait_until(|| handle.get_state() == 5, Duration::from_secs(1));

    handle.stop();
    wait_until(|| !handle.is_alive(), Duration::from_secs(1));
}

/// `alive_rx()`：actor 结束（stop / panic）后订阅端能观察到 `false`。
#[test]
fn alive_rx_observes_exit() {
    let (_rx, h) = spawn(Duration::MAX, "t-alive");
    let mut alive = h.alive_rx();
    assert!(*alive.borrow_and_update());

    h.stop();
    wait_until(|| !*alive.borrow_and_update(), Duration::from_secs(1));
}

/// 工厂闭包 panic → `SpawnError::ConstructorPanicked`，带 panic 消息，且**不**炸进程。
#[test]
fn factory_panic_is_reported() {
    let err = match try_spawn_thread::<NeverBuilt, _>("t-factory", || { panic!("打不开文件") }) {
        Ok(_) => panic!("构造 panic 时不该启动成功"),
        Err(e) => e,
    };
    match err {
        SpawnError::ConstructorPanicked(msg) => {
            assert!(msg.contains("打不开文件"), "panic 消息没传出来: {msg}");
        }
        other => panic!("期望 ConstructorPanicked，实际 {other:?}"),
    }
}

/// `init_state_and_progress` panic → 同样归类为 `ConstructorPanicked`。
#[test]
fn init_panic_is_reported() {
    let err = match try_spawn_thread::<BadInit, _>("t-badinit", || BadInit) {
        Ok(_) => panic!("初始化 panic 时不该启动成功"),
        Err(e) => e,
    };
    match err {
        SpawnError::ConstructorPanicked(msg) => {
            assert!(msg.contains("初始化失败"), "panic 消息没传出来: {msg}");
        }
        other => panic!("期望 ConstructorPanicked，实际 {other:?}"),
    }
}

/// 成功路径上 `try_spawn_thread` 也能用。
#[test]
fn try_spawn_thread_success() {
    let (tx, rx) = mpsc::channel();
    let h = try_spawn_thread("t-try-ok", move || RcCounter {
        hits: Rc::new(Cell::new(0)),
        tx,
        idle: Duration::from_millis(10),
    })
    .expect("应该能启动");

    h.send_cmd(Cmd::Inc).unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), Ev::Count(1));
    h.stop();
}

// ---- 用于失败路径的最小 actor ----

struct NeverBuilt;

impl ActorCore for NeverBuilt {
    type Cmd = ();
    type Event = ();
    type State = ();
    type Progress = ();

    fn init_state_and_progress(&self) -> (Self::State, Self::Progress) {
        ((), ())
    }
}

impl ThreadActor for NeverBuilt {
    fn run(self, _ctx: ActorContext<Self>) {}
}

struct BadInit;

impl ActorCore for BadInit {
    type Cmd = ();
    type Event = ();
    type State = ();
    type Progress = ();

    fn init_state_and_progress(&self) -> (Self::State, Self::Progress) {
        panic!("初始化失败: 设备被占用")
    }
}

impl ThreadActor for BadInit {
    fn run(self, _ctx: ActorContext<Self>) {}
}
