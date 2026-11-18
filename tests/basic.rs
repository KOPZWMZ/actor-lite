#![cfg(feature = "async")]

use std::time::Duration;

use actor_lite::{ActorContext, ActorCore, AsyncActor, CancelToken};

struct Counter {
    count: u32,
}

enum Cmd {
    Inc,
}

impl ActorCore for Counter {
    type Cmd = Cmd;
    type Event = u32;
    type State = u32;
    type Progress = ();

    fn init_state_and_progress(&self) -> (Self::State, Self::Progress) {
        (self.count, ())
    }
}

impl AsyncActor for Counter {
    async fn run(self, mut ctx: ActorContext<Self>) {
        let cancel = ctx.cancel_clone();
        let mut count = self.count;

        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                cmd = ctx.wait_for_cmd() => match cmd {
                    Some(Cmd::Inc) => {
                        count += 1;
                        ctx.update_state(count);
                        ctx.send_event(count);
                    }
                    None => break,          // 所有 handle 都没了
                },
            }
        }
    }
}

/// 状态监听：断言在测试主体里，失败会真的红；不用 sleep。
#[tokio::test]
async fn state_listener_sees_update() {
    let handle = Counter { count: 0 }.spawn();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let _listener = handle.state_listen_async(move |state| {
        let tx = tx.clone();
        async move {
            let _ = tx.send(state);
        }
    });

    handle.send_cmd(Cmd::Inc).unwrap();
    assert_eq!(rx.recv().await, Some(1));

    handle.send_cmd(Cmd::Inc).unwrap();
    assert_eq!(rx.recv().await, Some(2));

    handle.stop();
}

/// stop 之后发送必须直接失败（而不是"悄悄排队"）。
#[tokio::test]
async fn send_cmd_fails_after_stop() {
    let handle = Counter { count: 0 }.spawn();
    handle.stop();
    assert!(handle.send_cmd(Cmd::Inc).is_err());
}

/// watch 语义：状态只保留最新值，读到的就是最后一次写入。
#[tokio::test]
async fn state_keeps_latest_value() {
    let handle = Counter { count: 0 }.spawn();
    for _ in 0..5 {
        handle.send_cmd(Cmd::Inc).unwrap();
    }
    // 轮询等 actor 处理完，避免 sleep 拍脑袋
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        if handle.get_state() == 5 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "actor 没在 1s 内处理完命令"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    handle.stop();
}

/// 取消信号：等待中的任务必须被唤醒（时间上界，而不是 sleep 碰运气）。
#[tokio::test]
async fn cancel_token_wakes_waiter() {
    let token = CancelToken::new();
    let waiter = {
        let token = token.clone();
        tokio::spawn(async move { token.cancelled().await })
    };

    token.cancel();
    tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("cancelled() 没被唤醒 —— 丢唤醒回归了")
        .unwrap();
}

/// 已经取消过的 token，再等必须立即返回；cancel 幂等。
#[tokio::test]
async fn cancel_is_idempotent_and_sticky() {
    let token = CancelToken::new();
    token.cancel();
    token.cancel();

    tokio::time::timeout(Duration::from_millis(100), token.cancelled())
        .await
        .expect("已取消的 token 不该再等待");
    assert!(token.is_cancelled());
    assert!(token.clone().is_cancelled()); // 副本共享同一状态
}

/// 带真实进度值的 actor，专门喂 `progress_rx()`。
struct Worker;

impl ActorCore for Worker {
    type Cmd = ();
    type Event = ();
    type State = u32;
    type Progress = f32;

    fn init_state_and_progress(&self) -> (Self::State, Self::Progress) {
        (0, 0.0)
    }
}

impl AsyncActor for Worker {
    async fn run(self, mut ctx: ActorContext<Self>) {
        ctx.update_state(7);
        ctx.update_progress(0.75);
        // 所有 handle 都 drop 后命令通道关闭 → 返回 None → 正常退出
        while ctx.wait_for_cmd().await.is_some() {}
    }
}

/// `state_rx()` / `progress_rx()` 把 watch 接收端交出来；同时钉住文档里那两条语义：
/// clone 出来的接收端第一次 `changed()` 读到的是"当前值"，actor 结束后 `changed()` 返回 `Err`
/// 但 `borrow()` 仍能读到最后一个值。
#[tokio::test]
async fn state_rx_and_progress_rx_pull_latest() {
    let handle = Worker.spawn();

    // 等 actor 写出第一版值：轮询 + 时间上界，不靠 sleep 拍脑袋
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while handle.get_state() != 7 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "actor 没在 1s 内更新状态"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let mut states = handle.state_rx();
    let mut progress = handle.progress_rx();

    // 文档承诺：actor 启动后更新过，clone 出来的接收端第一次 has_changed 立刻为真，
    // 读到的是当前值（不是"又变了一次的增量"）。
    assert!(
        states.has_changed().unwrap(),
        "第一次 has_changed 应该报「当前值未读」"
    );
    assert_eq!(*states.borrow_and_update(), 7);
    assert!(
        !states.has_changed().unwrap(),
        "borrow_and_update 之后不该再有变化"
    );

    assert_eq!(*progress.borrow_and_update(), 0.75);

    // drop 掉最后一个 handle → actor 退出 → sender 消失。
    drop(handle);

    let closed = tokio::time::timeout(Duration::from_secs(1), states.changed())
        .await
        .expect("actor 结束后 changed() 不该继续等待");
    assert!(closed.is_err(), "actor 结束后 changed() 应该返回 Err");
    assert_eq!(*states.borrow(), 7, "结束后仍应读到最后一个状态值");
    assert_eq!(*progress.borrow(), 0.75, "结束后仍应读到最后一个进度值");
}
