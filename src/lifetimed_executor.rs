use std::{cell::Cell, marker::PhantomData, rc::Rc, sync::Arc, time::Duration};

use async_task::Runnable;
use futures_intrusive::sync::LocalManualResetEvent;
use futures_lite::{Future, FutureExt};
use thread_local::ThreadLocal;

use crate::lifetimed_queues::{GlobalQueue, LocalQueue};

/// A Send and Sync executor from which [`LifetimedExecutor`]s can be constructed
// TODO: this global queue almost certainly needs a lifetime too as tasks from
// the local executor can end up on the global queue.
#[derive(Clone)]
pub struct GlobalExecutor<'a> {
    global_queue: GlobalQueue,
    // we can't use a static thread local, because each separate executor should have it's own set of local queues
    local_queue: Arc<ThreadLocal<LocalQueue>>,
    phantom_data: PhantomData<&'a ()>,
}

impl<'a> Default for GlobalExecutor<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> GlobalExecutor<'a> {
    pub fn new() -> Self {
        Self {
            global_queue: GlobalQueue::new(),
            local_queue: Arc::new(ThreadLocal::new()),
            phantom_data: PhantomData,
        }
    }

    pub fn get_local_executor<'b>(&self) -> LifetimedExecutor<'b>
    where
        'a: 'b,
    {
        LifetimedExecutor::from_global_queue(self)
    }

    /// call this in a separate thread to occationally rebalance the tasks
    pub fn global_rebalance(&self) -> ! {
        loop {
            self.global_queue.rebalance();
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

pub struct LifetimedExecutor<'a> {
    global_queue: GlobalQueue,
    local_queue: Arc<ThreadLocal<LocalQueue>>,
    phantom_data: PhantomData<&'a ()>,
}

impl<'a> LifetimedExecutor<'a> {
    thread_local! {
        static LOCAL_EVT: Rc<LocalManualResetEvent> = Rc::new(LocalManualResetEvent::new(false));

        static LOCAL_QUEUE_ACTIVE: Cell<bool> = Cell::new(false);
    }

    fn from_global_queue(global_executor: &GlobalExecutor) -> Self {
        LifetimedExecutor {
            global_queue: global_executor.global_queue.clone(),
            local_queue: global_executor.local_queue.clone(),
            phantom_data: PhantomData,
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
            let evt = local_evt.or(self.global_queue.wait());
            {
                let local_queue = self
                    .local_queue
                    .get_or(|| self.global_queue.subscribe());
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
    pub fn spawn<T: Send + 'static>(
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

    fn schedule(&self) -> impl Fn(Runnable) + Send + Sync + 'static {
        let global_queue = self.global_queue.clone();
        let local_queue = self.local_queue.clone();

        move |runnable| {
            // if the current thread is not processing tasks, we go to the global queue directly.
            if !Self::LOCAL_QUEUE_ACTIVE.with(|r| r.get()) || fastrand::usize(0..512) == 0 {
                global_queue.push(runnable);
            } else {
                let local_queue = local_queue
                    .get_or(|| global_queue.subscribe());
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

    use crate::{GlobalExecutor, LifetimedExecutor};

    #[test]
    fn global_executor_is_send_sync() {
        fn is_send_sync<T: Send + Sync>() {}

        is_send_sync::<GlobalExecutor>();
    }

    #[test]
    fn local_executor_is_send_sync() {
        fn is_send_sync<T: Send + Sync>() {}

        is_send_sync::<LifetimedExecutor<'static>>();
    }

    #[test]
    fn can_run_a_task() {
        let global_executor = GlobalExecutor::new();
        let count = AtomicU16::new(0);
        let executor = global_executor.get_local_executor();

        let task = executor.spawn(async {
            count.fetch_add(1, Ordering::Relaxed);
        });

        block_on(executor.run_local_queue().or(task));

        assert_eq!(count.into_inner(), 1);
    }

    #[test]
    fn can_yield() {
        let global_executor = GlobalExecutor::new();
        let count = AtomicU16::new(0);
        let executor = global_executor.get_local_executor();

        let task = executor.spawn(async {
            yield_now().await;
            count.fetch_add(1, Ordering::Relaxed);
        });

        block_on(executor.run_local_queue().or(task));

        assert_eq!(count.into_inner(), 1);
    }
}
