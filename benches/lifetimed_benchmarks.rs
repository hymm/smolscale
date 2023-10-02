use criterion::{criterion_group, criterion_main, Criterion};
use std::thread::available_parallelism;

use async_executor::Task;
use futures_lite::future;

use smolscale::GlobalExecutor;

const TASKS: usize = 300;
const STEPS: usize = 300;
const LIGHT_TASKS: usize = 25_000;

fn spawn_one(b: &mut criterion::Bencher) {
    let executor = setup_executor();
    b.iter(move || {
        future::block_on(async { executor.spawn(async {}).await });
    });
}

fn spawn_many(b: &mut criterion::Bencher) {
    let executor = setup_executor();
    b.iter(move || {
        future::block_on(async {
            let mut tasks = Vec::new();
            for _ in 0..LIGHT_TASKS {
                tasks.push(executor.spawn(async {}));
            }
            for task in tasks {
                task.await;
            }
        });
    });
}

fn yield_now(b: &mut criterion::Bencher) {
    let executor = setup_executor();
    b.iter(move || {
        future::block_on(async {
            let mut tasks = Vec::new();
            for _ in 0..TASKS {
                tasks.push(executor.spawn(async move {
                    for _ in 0..STEPS {
                        future::yield_now().await;
                    }
                }));
            }
            for task in tasks {
                task.await;
            }
        });
    });
}

// fn busy_loops(b: &mut criterion::Bencher) {
//     b.iter(move || {
//         future::block_on(async {
//             let mut tasks = Vec::new();
//             for _ in 0..TASKS {
//                 tasks.push(spawn(async move {
//                     std::thread::sleep(Duration::from_millis(10));
//                     // let start = Instant::now();st
//                     // while start.elapsed() < Duration::from_millis(100) {}
//                 }));
//             }
//             for task in tasks {
//                 task.await;
//             }
//         });
//     });
// }

fn ping_pong(b: &mut criterion::Bencher) {
    const NUM_PINGS: usize = 1_000;
    let executor = setup_executor();

    let (send, recv) = async_channel::bounded::<async_oneshot::Sender<_>>(10);
    let _task: Task<Option<()>> = executor.spawn(async move {
        loop {
            let os = recv.recv().await.ok()?;
            os.send(0u8).ok()?;
        }
    });
    b.iter(|| {
        let send = send.clone();
        future::block_on(async move {
            for _ in 0..NUM_PINGS {
                let (os_send, os_recv) = async_oneshot::oneshot();
                send.send(os_send).await.unwrap();
                os_recv.await.unwrap();
            }
        });
    });
}

fn fanout(b: &mut criterion::Bencher) {
    const NUM_TASKS: usize = 1_000;
    const NUM_ITER: usize = 1_000;
    let executor = setup_executor();

    let (send, recv) = async_channel::bounded(1);
    let _tasks = (0..NUM_TASKS)
        .map(|_i| {
            let recv = recv.clone();
            executor.spawn(async move {
                for _ctr in 0.. {
                    if recv.recv().await.is_err() {
                        return;
                    }
                }
            })
        })
        .collect::<Vec<_>>();
    b.iter(|| {
        let send = send.clone();
        future::block_on(async move {
            for _ in 0..NUM_ITER {
                send.send(()).await.unwrap();
            }
        })
    })
}

fn context_switch_quiet(b: &mut criterion::Bencher) {
    let (send, mut recv) = async_channel::bounded::<usize>(1);
    let mut tasks: Vec<Task<Option<()>>> = vec![];
    let executor = setup_executor();
    for _ in 0..TASKS {
        let old_recv = recv.clone();
        let (new_send, new_recv) = async_channel::bounded(1);
        tasks.push(executor.spawn(async move {
            loop {
                new_send.send(old_recv.recv().await.ok()?).await.ok()?
            }
        }));
        recv = new_recv;
    }
    b.iter(move || {
        future::block_on(async {
            send.send(1).await.unwrap();
            recv.recv().await.unwrap();
        });
    });
}

fn context_switch_busy(b: &mut criterion::Bencher) {
    let (send, mut recv) = async_channel::bounded::<usize>(1);
    let mut tasks: Vec<Task<Option<()>>> = vec![];
    let executor = setup_executor();
    for _ in 0..TASKS / 10 {
        let old_recv = recv.clone();
        let (new_send, new_recv) = async_channel::bounded(1);
        tasks.push(executor.spawn(async move {
            loop {
                // eprintln!("forward {}", num);
                new_send.send(old_recv.recv().await.ok()?).await.ok()?;
            }
        }));
        recv = new_recv;
    }
    for _ in 0..TASKS {
        tasks.push(executor.spawn(async move {
            loop {
                future::yield_now().await;
            }
        }))
    }
    b.iter(move || {
        future::block_on(async {
            for _ in 0..10 {
                // eprintln!("send");
                send.send(1).await.unwrap();
                recv.recv().await.unwrap();
            }
        });
    });
}

fn setup_executor() -> GlobalExecutor<'static> {
    let executor = GlobalExecutor::new();
    for _ in 0..available_parallelism().unwrap().into() {
        let executor = executor.clone();
        std::thread::spawn(move || {
            let local_executor = executor;
            loop {
                futures_lite::future::block_on(local_executor.run_local_queue());
            }
        });
    }
    let executor_cloned = executor.clone();
    std::thread::spawn(move || {
        executor_cloned.global_rebalance();
    });
    executor
}

fn criterion_benchmark(c: &mut Criterion) {
    let _ = env_logger::try_init();
    let mut group = c.benchmark_group("lifetimed");
    group.bench_function("spawn_one", spawn_one);
    group.bench_function("spawn_many", spawn_many);
    group.bench_function("yield_now", yield_now);
    group.bench_function("fanout", fanout);
    // c.bench_function("busy_loops", busy_loops);
    group.bench_function("ping_pong", ping_pong);

    group.bench_function("context_switch_quiet", context_switch_quiet);
    group.bench_function("context_switch_busy", context_switch_busy);
}

criterion_group!(life_benches, criterion_benchmark);
criterion_main!(life_benches);
