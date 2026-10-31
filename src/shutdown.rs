//! Shutdown signal shared by the HTTP server and the consumers.

use tokio::sync::watch;

/// Receiver side of the shutdown flag; `true` means stop.
pub type Receiver = watch::Receiver<bool>;

/// Resolves once shutdown is requested. Never resolves if the sender is dropped without requesting it.
pub async fn requested(shutdown: &mut Receiver) {
    if shutdown.wait_for(|stop| *stop).await.is_err() {
        std::future::pending::<()>().await;
    }
}
