//! Bound accepted HTTP connections independently from request and stream slots.

use axum::serve::Listener;
use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
};

const MAX_CONNECTIONS: usize = 128;

pub(super) struct BoundedListener {
    listener: TcpListener,
    slots: Arc<Semaphore>,
}

impl BoundedListener {
    pub(super) fn new(listener: TcpListener) -> Self {
        Self {
            listener,
            slots: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        }
    }
}

pub(super) struct Connection {
    stream: TcpStream,
    _permit: OwnedSemaphorePermit,
}

impl Listener for BoundedListener {
    type Io = Connection;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Connection, SocketAddr) {
        // The private budget is never closed; shutdown cancels accept.
        let Ok(permit) = Arc::clone(&self.slots).acquire_owned().await else {
            return std::future::pending().await;
        };
        loop {
            match self.listener.accept().await {
                Ok((stream, peer)) => {
                    return (
                        Connection {
                            stream,
                            _permit: permit,
                        },
                        peer,
                    );
                }
                Err(error) => {
                    tracing::warn!(?error, "bounded HTTP accept failed");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }
}

impl AsyncRead for Connection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buffer)
    }
}

impl AsyncWrite for Connection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn a_live_connection_holds_the_accept_budget_until_drop()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let mut listener = BoundedListener {
            listener,
            slots: Arc::new(Semaphore::new(1)),
        };
        let _first_peer = TcpStream::connect(address).await?;
        let (first, _) = listener.accept().await;
        let _second_peer = TcpStream::connect(address).await?;
        assert!(
            tokio::time::timeout(Duration::from_millis(10), listener.accept())
                .await
                .is_err()
        );
        drop(first);
        let (_second, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept()).await?;
        Ok(())
    }
}
