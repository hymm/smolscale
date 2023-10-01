use std::{cell::Cell, marker::PhantomData, rc::Rc, time::Duration};

use futures_intrusive::sync::LocalManualResetEvent;
use futures_lite::{Future, FutureExt};

use crate::lifetimed_queues::{GlobalQueue, LocalQueue};

/// A Send and Sync executor from which [`LifetimedExecutor`]s can be constructed
// TODO: this global queue almost certainly needs a lifetime too as tasks from
// the local executor can end up on the global queue.
#[derive(Clone)]
pub struct GlobalExecutor {
    global_queue: GlobalQueue,
}

impl Default for GlobalExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl GlobalExecutor {
    pub fn new() -> Self {
        Self {
            global_queue: GlobalQueue::new(),
        }
    }

    pub fn get_local_executor<'a>(&self) -> LifetimedExecutor<'a> {
        let global_queue = self.global_queue.clone();
        let local_queue = self.global_queue.subscribe();

        LifetimedExecutor {
            global_queue,
            local_queue,
            phantom_data: PhantomData,
        }
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
    local_queue: LocalQueue,
    phantom_data: PhantomData<&'a ()>,
}

impl<'a> LifetimedExecutor<'a> {
    thread_local! {
        static LOCAL_EVT: Rc<LocalManualResetEvent> = Rc::new(LocalManualResetEvent::new(false));

        static LOCAL_QUEUE_ACTIVE: Cell<bool> = Cell::new(false);
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
                while let Some(r) = self.local_queue.pop() {
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
    pub fn spawn<F>(&self, future: F) -> async_task::Task<F::Output>
    where
        F: Future + Send + 'a,
        F::Output: Send + 'a,
    {
        // SAFETY:
        // * future is send
        // * Runnable is dropped when task is dropped. Task cannot outlive executor
        // * schedule is send and sync. TODO: think about this harder
        // * Runnable waker is dropped when executor is dropped.
        let (runnable, task) = unsafe {
            async_task::spawn_unchecked(future, |runnable| {
                // if the current thread is not processing tasks, we go to the global queue directly.
                if !Self::LOCAL_QUEUE_ACTIVE.with(|r| r.get()) || fastrand::usize(0..512) == 0 {
                    self.global_queue.push(runnable);
                } else {
                    self.local_queue.push(runnable);
                    Self::LOCAL_EVT.with(|le| le.set());
                }
            })
        };
        runnable.schedule();
        task
    }
}

impl<'a> Clone for LifetimedExecutor<'a> {
    fn clone(&self) -> Self {
        let global_queue = self.global_queue.clone();
        let local_queue = global_queue.subscribe();

        Self {
            global_queue,
            local_queue,
            phantom_data: PhantomData,
        }
    }
}

// TODO: impl Drop for Executor to move tasks in local queue to global queue

#[cfg(test)]
mod tests {
    use crate::GlobalExecutor;

    #[test]
    fn global_executor_is_send_sync() {
        fn is_send_sync<T: Send + Sync>() {}

        is_send_sync::<GlobalExecutor>();
    }
}
