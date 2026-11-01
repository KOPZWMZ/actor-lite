//! 线程壳用的最小异步驱动：只靠 `std::thread::park`，不需要 tokio runtime。
//!
//! 只服务于"在一个裸线程上阻塞等待 tokio::sync 的 future"这一种场景。

use std::future::Future;
use std::pin::pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::thread::Thread;
use std::time::Instant;

/// 用给定 `waker` 轮询 `fut`，直到就绪或到达 `deadline`（`None` = 一直等）。
///
/// 返回 `None` 表示超时（此时 `fut` 被 drop，**调用方负责确认 future 是 cancel-safe**：
/// `mpsc::recv` / `watch::changed` / `watch::wait_for` 都是）。
///
/// # 注意
///
/// 会**阻塞当前线程**（`park`）。只能在线程 actor 里用，不要在 tokio worker 上调用。
pub(crate) fn drive_until<F: Future>(
    fut: F,
    deadline: Option<Instant>,
    waker: &Waker,
) -> Option<F::Output> {
    let mut fut = pin!(fut);
    let mut cx = Context::from_waker(waker);

    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return Some(v);
        }
        match deadline {
            None => thread::park(),
            Some(deadline) => {
                let now = Instant::now();
                if now >= deadline {
                    return None;
                }
                thread::park_timeout(deadline - now);
            }
        }
    }
}

/// 无超时的阻塞等待。
pub(crate) fn block_on<F: Future>(fut: F, waker: &Waker) -> F::Output {
    match drive_until(fut, None, waker) {
        Some(v) => v,
        None => unreachable!("deadline 为 None 时 drive_until 不会返回 None"),
    }
}
/// [`ActorContext::recv_cmd_timeout`] 的等待结果。`Timeout` 不是错误，是正常的空闲 tick。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CmdWait {
    /// 这段时间内没有命令。
    Timeout,
    /// actor 已停止，或命令通道关闭 —— 该退出循环了。
    Closed,
}

/// actor 所在线程的唤醒句柄。异步壳没有专属线程 → 全部是 no-op。
#[derive(Clone, Default, Debug)]
pub(crate) struct SharedWaker {
    inner: Arc<WakerInner>,
}

#[derive(Default, Debug)]
struct WakerInner {
    thread: OnceLock<Thread>,
}

impl SharedWaker {
    /// 线程壳在 actor 线程里调用一次。
    pub(crate) fn attach_current(&self) {
        let _ = self.inner.thread.set(std::thread::current());
    }

    /// 唤醒 actor 线程；未绑定线程时什么都不做。
    pub(crate) fn wake(&self) {
        if let Some(t) = self.inner.thread.get() {
            t.unpark();
        }
    }

    /// 造 waker：tokio 触发它 = unpark 该线程。**每次现建**，不要缓存进 `WakerInner`（循环引用）。
    pub(crate) fn waker(&self) -> Waker {
        Waker::from(Arc::new(WakeRef(self.clone())))
    }
}

struct WakeRef(SharedWaker);

impl std::task::Wake for WakeRef {
    fn wake(self: Arc<Self>) {
        self.0.wake();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.wake();
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::future::pending;
    use std::time::Duration;

    #[test]
    fn drive_until_times_out() {
        let w = SharedWaker::default().waker();
        let out = drive_until(
            pending::<()>(),
            Some(Instant::now() + Duration::from_millis(20)),
            &w,
        );
        assert!(out.is_none());
    }

    #[test]
    fn shared_waker_unparks_thread() {
        let w = SharedWaker::default();
        let w2 = w.clone();
        let h = std::thread::spawn(move || {
            w2.attach_current();
            thread::park();
            true
        });
        std::thread::sleep(Duration::from_millis(10));
        w.wake();
        assert!(h.join().unwrap());
    }
}
