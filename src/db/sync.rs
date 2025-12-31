use crate::db::option::{OptionError, OptionExt};
use iroh::{
    Endpoint, EndpointAddr,
    endpoint::{BindError, Connection},
    protocol::{AcceptError, ProtocolHandler},
};
use redb::{ReadableDatabase, TransactionError};
use thiserror::Error;

use crate::db::Db;

pub const ALPN: &[u8] = b"pika/sync/0";

#[derive(Debug, Clone)]
struct DbSyncHandler;

#[derive(Error, Debug)]
pub enum DbSyncError {
    #[error("could not bind to endpoint")]
    BindError(#[from] BindError),

    #[error("IO error")]
    IoError(#[from] std::io::Error),

    #[error("error waiting for task to shutdown")]
    JoinError(#[from] tokio::task::JoinError),

    #[error("could not connect to endpoint")]
    ConnectError(#[from] iroh::endpoint::ConnectError),

    #[error("error on connection")]
    ConnectionError(#[from] iroh::endpoint::ConnectionError),

    #[error("error writing on connection")]
    WriteError(#[from] iroh::endpoint::WriteError),

    #[error("error reading on connection")]
    ReadToEndError(#[from] iroh::endpoint::ReadToEndError),

    #[error("stream is closed")]
    StreamClosed(#[from] iroh::endpoint::ClosedStream),

    #[error("redb transaction error")]
    TransactionError(#[from] TransactionError),

    #[error("redb storage error")]
    StorageError(#[from] redb::StorageError),

    #[error("option error")]
    OptionError(#[from] OptionError),

    #[error("redb generic error")]
    RedbGeneric(#[from] redb::Error),
}

pub trait DbSync {
    async fn serve(&self) -> Result<(), DbSyncError>;
    async fn fetch(&self, remote_name: &str) -> Result<(), DbSyncError>;
}

impl ProtocolHandler for DbSyncHandler {
    /// The `accept` method is called for each incoming connection for our ALPN.
    ///
    /// The returned future runs on a newly spawned tokio task, so it can run as long as
    /// the connection lasts without blocking other connections.
    fn accept(
        &self,
        connection: Connection,
    ) -> impl futures::Future<Output = Result<(), AcceptError>> + std::marker::Send {
        Box::pin(async move {
            let endpoint_id = connection.remote_id();
            println!("accepted connection from {endpoint_id}");

            let (mut send, mut recv) = connection.accept_bi().await?;

            let bytes_sent = tokio::io::copy(&mut recv, &mut send).await?;
            println!("Copied over {bytes_sent} byte(s)");

            send.finish()?;

            connection.closed().await;

            Ok(())
        })
    }
}

impl DbSync for Db {
    async fn serve(&self) -> Result<(), DbSyncError> {
        let mdns = iroh::discovery::mdns::MdnsDiscovery::builder();

        let read_txn = self.redb.begin_read()?;
        let secret = read_txn.option_table()?.get_secret_key()?;

        let endpoint = Endpoint::builder()
            .discovery(mdns)
            .secret_key(secret)
            .bind()
            .await?;
        println!("endpoint id: {:?}", endpoint.id());

        let router = iroh::protocol::Router::builder(endpoint)
            .accept(ALPN, DbSyncHandler)
            .spawn();

        tokio::signal::ctrl_c().await?;

        router.shutdown().await?;

        Ok(())
    }

    async fn fetch(&self, remote_name: &str) -> Result<(), DbSyncError> {
        let read_txn = self.redb.begin_read()?;
        let options = read_txn.option_table()?;
        let endpoint_id = options.get_remote(remote_name)?;

        let endpoint = Endpoint::bind().await?;

        // Open a connection to the accepting endpoint
        let conn = endpoint
            .connect(EndpointAddr::new(endpoint_id), ALPN)
            .await?;

        // Open a bidirectional QUIC stream
        let (mut send, mut recv) = conn.open_bi().await?;

        // Send some data to be echoed
        send.write_all(b"Hello, world!").await?;

        // Signal the end of data for this particular stream
        send.finish()?;

        // Receive the echo, but limit reading up to maximum 1000 bytes
        let response = recv.read_to_end(1000).await?;
        assert_eq!(&response, b"Hello, world!");

        // Explicitly close the whole connection.
        conn.close(0u32.into(), b"bye!");

        // The above call only queues a close message to be sent (see how it's not async!).
        // We need to actually call this to make sure this message is sent out.
        endpoint.close().await;
        // If we don't call this, but continue using the endpoint, we then the queued
        // close call will eventually be picked up and sent.
        // But always try to wait for endpoint.close().await to go through before dropping
        // the endpoint to ensure any queued messages are sent through and connections are
        // closed gracefully.
        Ok(())
    }
}
