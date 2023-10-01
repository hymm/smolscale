use std::{cell::Cell, marker::PhantomData, rc::Rc, time::Duration};

use futures_intrusive::sync::LocalManualResetEvent;
use futures_lite::{Future, FutureExt};

use crate::lifetimed_queues::{GlobalQueue, LocalQueue};

pub struct LifetimedExecutor<'a> {
    global_queue: GlobalQueue,
    local_queue: LocalQueue,
    phantom_data: PhantomData<&'a ()>,
}

impl<'a> Default for LifetimedExecutor<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> LifetimedExecutor<'a> {
    thread_local! {
        static LOCAL_EVT: Rc<LocalManualResetEvent> = Rc::new(LocalManualResetEvent::new(false));

        static LOCAL_QUEUE_ACTIVE: Cell<bool> = Cell::new(false);
    }

    pub fn new() -> Self {
        let global_queue = GlobalQueue::new();
        let local_queue = global_queue.subscribe();
        Self {
            global_queue,
            local_queue,
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

    /// call this in a separate thread to occationally rebalance the tasks
    pub fn global_rebalance(&mut self) -> ! {
        loop {
            self.global_queue.rebalance();
            std::thread::sleep(Duration::from_millis(10));
        }
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
