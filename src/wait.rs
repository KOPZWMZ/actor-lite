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

/// 两个 future 一起等，谁先就绪返回谁。
pub(crate) enum Either2<A, B> {
    First(A),
    Second(B),
}

/// 同时驱动 `a` 和 `b`，返回先就绪的那个；`None` = 超时。
///
/// 先 poll `a` ⇒ 有命令时命令优先于取消。
/// 两个 future 都要求 cancel-safe（`mpsc::recv` / `watch::wait_for` 满足）。
pub(crate) fn drive_either<A, B>(
    a: A,
    b: B,
    deadline: Option<Instant>,
    waker: &Waker,
) -> Option<Either2<A::Output, B::Output>>
where
    A: Future,
    B: Future,
{
    let mut a = pin!(a);
    let mut b = pin!(b);
    let mut cx = Context::from_waker(waker);

    loop {
        if let Poll::Ready(v) = a.as_mut().poll(&mut cx) {
            return Some(Either2::First(v));
        }
        if let Poll::Ready(v) = b.as_mut().poll(&mut cx) {
            return Some(Either2::Second(v));
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

/// 单 future 版本：等到就绪或 `deadline`（`None` = 一直等）。`None` = 超时。
///
/// 内部复用 [`drive_either`]：第二个 future 用一个永不就绪的 `pending()`，
/// 于是语义退化成"只等 `fut`"，park 逻辑一行都不用重复。
pub(crate) fn drive_until<F: Future>(
    fut: F,
    deadline: Option<Instant>,
    waker: &Waker,
) -> Option<F::Output> {
    use std::convert::Infallible;

    match drive_either(fut, std::future::pending::<Infallible>(), deadline, waker) {
        Some(Either2::First(v)) => Some(v),
        Some(Either2::Second(never)) => match never {}, // Infallible 不可构造
        None => None,
    }
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
