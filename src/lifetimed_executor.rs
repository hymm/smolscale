use std::{
    cell::RefCell,
    marker::PhantomData,
    panic::{RefUnwindSafe, UnwindSafe},
    pin::pin,
    rc::Rc,
    sync::Arc,
    task::{Poll, Waker},
    time::Duration,
};

use async_task::Runnable;
use futures_intrusive::sync::LocalManualResetEvent;
use futures_lite::{Future, FutureExt};
use os_thread_local::ThreadLocal;

use crate::lifetimed_queues::{ArcGlobalQueue, LocalQueue};

/// A Send and Sync executor from which [`LifetimedExecutor`]s can be constructed
// TODO: this global queue almost certainly needs a lifetime too as tasks from
// the local executor can end up on the global queue.
#[derive(Clone, Debug)]
pub struct Executor<'a> {
    queues: Queues,
    phantom_data: PhantomData<&'a ()>,
}

// TODO: figure out if this is true. I think it is, but needs more thought
impl UnwindSafe for Executor<'_> {}
impl RefUnwindSafe for Executor<'_> {}

impl<'a> Default for Executor<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> Executor<'a> {
    thread_local! {
        static LOCAL_EVT: Rc<LocalManualResetEvent> = Rc::new(LocalManualResetEvent::new(false));
    }

    pub fn new() -> Self {
        let global_queue = ArcGlobalQueue::new();

        Self {
            queues: Queues {
                global_queue: global_queue.clone(),
                local_queue: Arc::new(ThreadLocal::new(|| RefCell::new(None))),
            },
            phantom_data: PhantomData,
        }
    }

    /// call this in a separate thread to occationally rebalance the tasks
    pub fn global_rebalance(&self) -> ! {
        loop {
            self.queues.global_queue.0.rebalance();
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Runs a queue
    pub async fn run_local_queue(&self) {
        self.queues
            .set_local_queue(async {
                loop {
                    // subscribe
                    let local_evt = async {
                        let local = Self::LOCAL_EVT.with(|le| le.clone());
                        local.wait().await;
                log::debug!("local fired!");
                        local.reset();
                    };
                    let evt = local_evt.or(self.queues.global_queue.0.wait());

                    {
                        loop {
                            let r = self
                                .queues
                                .with_local_queue(|local_queue| local_queue.pop())
                                .flatten();
                            if let Some(r) = r {
                                r.run();
                            } else {
                                break;
                            }
                            if fastrand::usize(0..256) == 0 {
                                futures_lite::future::yield_now().await;
                            }
                        }
                    }
                    // wait now, so that when we get woken up, we *know* that something happened to the global queue.
                    evt.await;
                }
            })
            .await;
    }

    /// Spawns a task
    pub fn spawn<T: Send + 'a>(
        &self,
        future: impl Future<Output = T> + Send + 'a,
    ) -> async_task::Task<T> {
        // SAFETY:
        // * future is send
        // * Runnable is dropped when task is dropped. Task cannot outlive executor
        // * schedule closure is send and sync. LocalKey is Send and Sync, self is send and sync
        // * Runnable waker is dropped when executor is dropped.
        let (runnable, task) = unsafe { async_task::spawn_unchecked(future, self.schedule()) };
        runnable.schedule();
        task
    }

    #[inline]
    fn schedule(&self) -> impl Fn(Runnable) + Send + Sync + 'static {
        let queues = self.queues.clone();

        move |runnable| {
            let mut runnable = Some(runnable);

            // if the current thread is not processing tasks, we go to the global queue directly.
            if fastrand::usize(0..512) != 0 {
                if let Some(()) = queues.with_local_queue(|local_queue| {
                    local_queue.push(runnable.take().unwrap());
                }) {
                    Self::LOCAL_EVT.with(|le| le.set());
                    return;
                }
            }

            if let Some(runnable) = runnable {
                queues.global_queue.0.push(runnable);
            }
        }
    }
}

#[derive(Clone, Debug)]
struct Queues {
    global_queue: ArcGlobalQueue,
    // we can't use a static thread local, because each separate executor should have it's own set of local queues
    local_queue: Arc<ThreadLocal<RefCell<Option<LocalQueue>>>>,
}

impl Queues {
    #[inline]
    fn with_local_queue<R>(&self, f: impl FnOnce(&LocalQueue) -> R) -> Option<R> {
        self.local_queue
            .try_with(|local_queue| local_queue.borrow().as_ref().map(f))
            .ok()
            .flatten()
    }

    /// Run a future with a set local queue.
    #[inline]
    async fn set_local_queue<F>(&self, fut: F) -> F::Output
    where
        F: Future,
    {
        let mut old = with_waker(|waker| {
            self.local_queue.with(move |slot| {
                slot.borrow_mut()
                    .replace(self.global_queue.subscribe(waker.clone()))
            })
        })
        .await;

        // Restore the old local queue on drop.
        let _guard = CallOnDrop(move || {
            let old = old.take();
            let _ = self.local_queue.try_with(move |slot| {
                *slot.borrow_mut() = old;
            });
        });

        let mut fut = pin!(fut);
        futures_lite::future::poll_fn(move |cx| {
            self.local_queue
                .try_with({
                    let waker = cx.waker();
                    move |slot| {
                        let mut slot = slot.borrow_mut();
                        let local_queue = match slot.as_mut() {
                            None => {
                                *slot = Some(self.global_queue.subscribe(waker.clone()));
                                return;
                            }
                            Some(local_queue) => local_queue,
                        };

                        if !Arc::ptr_eq(&local_queue.global.0, &self.global_queue.0) {
                            return;
                        }

                        if !local_queue.waker.will_wake(waker) {
                            local_queue.waker = waker.clone();
                        }
                    }
                })
                .ok();

            fut.as_mut().poll(cx)
        })
        .await
    }
}

/// Runs a closure when dropped.
struct CallOnDrop<F: FnMut()>(F);

impl<F: FnMut()> Drop for CallOnDrop<F> {
    fn drop(&mut self) {
        (self.0)();
    }
}

/// Run a closure with the current waker.
fn with_waker<F: FnOnce(&Waker) -> R, R>(f: F) -> impl Future<Output = R> {
    let mut f = Some(f);
    futures_lite::future::poll_fn(move |cx| {
        let f = f.take().unwrap();
        Poll::Ready(f(cx.waker()))
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU16, Ordering};

    use futures_lite::{
        future::{block_on, yield_now},
        FutureExt,
    };

    use crate::Executor;

    #[test]
    fn global_executor_is_send_sync() {
        fn is_send_sync<T: Send + Sync>() {}

        is_send_sync::<Executor>();
    }

    #[test]
    fn can_run_a_task() {
        let executor = Executor::new();
        let count = AtomicU16::new(0);

        let task = executor.spawn(async {
            count.fetch_add(1, Ordering::Relaxed);
        });

        block_on(executor.run_local_queue().or(task));

        assert_eq!(count.into_inner(), 1);
    }

    #[test]
    fn can_yield() {
        let executor = Executor::new();
        let count = AtomicU16::new(0);

        let task = executor.spawn(async {
            yield_now().await;
            count.fetch_add(1, Ordering::Relaxed);
        });

        block_on(executor.run_local_queue().or(task));

        assert_eq!(count.into_inner(), 1);
    }
}
