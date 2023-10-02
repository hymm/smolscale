use std::{cell::Cell, marker::PhantomData, rc::Rc, sync::Arc, time::Duration, panic::{UnwindSafe, RefUnwindSafe}};

use async_task::Runnable;
use futures_intrusive::sync::LocalManualResetEvent;
use futures_lite::{Future, FutureExt};
use thread_local::ThreadLocal;

use crate::lifetimed_queues::{ArcGlobalQueue, LocalQueue};

/// A Send and Sync executor from which [`LifetimedExecutor`]s can be constructed
// TODO: this global queue almost certainly needs a lifetime too as tasks from
// the local executor can end up on the global queue.
#[derive(Clone, Debug)]
pub struct Executor<'a> {
    global_queue: ArcGlobalQueue,
    // we can't use a static thread local, because each separate executor should have it's own set of local queues
    local_queue: Arc<ThreadLocal<LocalQueue>>,
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

        static LOCAL_QUEUE_ACTIVE: Cell<bool> = Cell::new(false);
    }

    pub fn new() -> Self {
        Self {
            global_queue: ArcGlobalQueue::new(),
            local_queue: Arc::new(ThreadLocal::new()),
            phantom_data: PhantomData,
        }
    }

    /// call this in a separate thread to occationally rebalance the tasks
    pub fn global_rebalance(&self) -> ! {
        loop {
            self.global_queue.0.rebalance();
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Runs a queue
    pub async fn run_local_queue(&self) {
        Self::LOCAL_QUEUE_ACTIVE.with(|r| r.set(true));
        scopeguard::defer!(Self::LOCAL_QUEUE_ACTIVE.with(|r| r.set(false)));
        loop {
            // subscribe
            let local_evt = async {
                let local = Self::LOCAL_EVT.with(|le| le.clone());
                local.wait().await;
                log::debug!("local fired!");
                local.reset();
            };
            let evt = local_evt.or(self.global_queue.0.wait());
            {
                let local_queue = self.local_queue.get_or(|| self.global_queue.subscribe());
                while let Some(r) = local_queue.pop() {
                    r.run();
                    if fastrand::usize(0..256) == 0 {
                        futures_lite::future::yield_now().await;
                    }
                }
            }
            // wait now, so that when we get woken up, we *know* that something happened to the global queue.
            evt.await;
        }
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
        let global_queue = self.global_queue.clone();
        let local_queue = self.local_queue.clone();

        move |runnable| {
            // if the current thread is not processing tasks, we go to the global queue directly.
            if !Self::LOCAL_QUEUE_ACTIVE.with(|r| r.get()) || fastrand::usize(0..512) == 0 {
                global_queue.0.push(runnable);
            } else {
                let local_queue = local_queue.get_or(|| global_queue.subscribe());
                local_queue.push(runnable);
                Self::LOCAL_EVT.with(|le| le.set());
            }
        }
    }
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
