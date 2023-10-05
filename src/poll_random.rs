use pin_project_lite::pin_project;
use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// Polls a future a random number of times before returning Pending.
/// `odds` is the inverted odds that it will stop polling;
pub fn poll_random<T, F>(odds: usize, f: F) -> PollRandom<F>
where
    F: Future<Output = T>,
{
    PollRandom { odds, f }
}

pin_project! {
    /// Future for the [`poll_once()`] function.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct PollRandom<F> {
        odds: usize,
        #[pin]
        f: F,
    }
}

impl<F> fmt::Debug for PollRandom<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PollOnce").finish()
    }
}

impl<T, F> Future for PollRandom<F>
where
    F: Future<Output = T>,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let odds = self.odds;
        let mut this = self.project();
        while fastrand::usize(0..odds) != 0 {
            match this.f.as_mut().poll(cx) {
                Poll::Ready(t) => return Poll::Ready(t),
                Poll::Pending => {}
            }
        }
        Poll::Pending
    }
}
