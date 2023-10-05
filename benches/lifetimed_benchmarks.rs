use criterion::{
    criterion_group, criterion_main, measurement::WallTime, BenchmarkGroup, Criterion,
};
use std::thread::available_parallelism;

use async_executor::Task;
use futures_lite::future;

use smolscale::Executor;

const TASKS: usize = 300;
const STEPS: usize = 300;
const LIGHT_TASKS: usize = 25_000;

fn spawn_one(group: &mut BenchmarkGroup<WallTime>) {
    let executor = setup_executor();
    group.bench_function("spawn_one", |b| {
        b.iter(|| {
            future::block_on(async { executor.spawn(async {}).await });
        });
    });
}

fn spawn_many(group: &mut BenchmarkGroup<WallTime>) {
    let executor = setup_executor();
    group.bench_function("spawn_manu", |b| {
        b.iter(|| {
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
    });
}

fn yield_now(group: &mut BenchmarkGroup<WallTime>) {
    let executor = setup_executor();
    group.bench_function("yield_now", |b| {
        b.iter(|| {
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

fn ping_pong(group: &mut BenchmarkGroup<WallTime>) {
    let executor = setup_executor();
    group.bench_function("ping_pong", |b| {
        const NUM_PINGS: usize = 1_000;

        let (send, recv) = async_channel::bounded::<async_oneshot::Sender<_>>(10);
        let _task: Task<Option<()>> = executor.spawn(async move {
            loop {
                let os = recv.recv().await.ok()?;
                os.send(0u8).ok()?;
            }
        });
        b.iter(|| {
            let send = send.clone();
            let task = executor.spawn(async move {
                for _ in 0..NUM_PINGS {
                    let (os_send, os_recv) = async_oneshot::oneshot();
                    send.send(os_send).await.unwrap();
                    os_recv.await.unwrap();
                }
            });
            future::block_on(task);
        });
    });
}

fn fanout(group: &mut BenchmarkGroup<WallTime>) {
    let executor = setup_executor();
    group.bench_function("fanout", |b| {
        const NUM_TASKS: usize = 1_000;
        const NUM_ITER: usize = 1_000;

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
            let task = executor.spawn(async move {
                for _ in 0..NUM_ITER {
                    send.send(()).await.unwrap();
                }
            });
            future::block_on(task);
        })
    });
}

fn context_switch_quiet(group: &mut BenchmarkGroup<WallTime>) {
    let executor = setup_executor();
    group.bench_function("context_switch_quiet", |b| {
        let (send, mut recv) = async_channel::bounded::<usize>(1);
        let mut tasks: Vec<Task<Option<()>>> = vec![];

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
    });
}

fn context_switch_busy(group: &mut BenchmarkGroup<WallTime>) {
    let executor = setup_executor();
    group.bench_function("context_switch_busy", |b| {
        let (send, mut recv) = async_channel::bounded::<usize>(1);
        let mut tasks: Vec<Task<Option<()>>> = vec![];
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
    });
}

fn setup_executor() -> Executor<'static> {
    let executor = Executor::new();

    for _ in 0..available_parallelism().unwrap().into() {
        let executor = executor.clone();
        std::thread::spawn(move || {
            let local_executor = executor;

            futures_lite::future::block_on(local_executor.run_local_queue());
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
    spawn_one(&mut group);
    spawn_many(&mut group);
    yield_now(&mut group);
    fanout(&mut group);
    // c.bench_function("busy_loops", busy_loops);
    ping_pong(&mut group);

    context_switch_quiet(&mut group);
    context_switch_busy(&mut group);
}

criterion_group!(life_benches, criterion_benchmark);
criterion_main!(life_benches);
