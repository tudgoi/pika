use anyhow::Result;
use iroh::Endpoint;
use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler};

pub const ALPN: &[u8] = b"pika/sync/0";

#[derive(Debug, Clone)]
struct DbSync;

impl ProtocolHandler for DbSync {
    /// The `accept` method is called for each incoming connection for our ALPN.
    ///
    /// The returned future runs on a newly spawned tokio task, so it can run as long as
    /// the connection lasts without blocking other connections.
    fn accept(&self, connection: Connection) -> impl futures::Future<Output = Result<(), AcceptError>> + std::marker::Send {
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
#[tokio::main]
pub async fn run() -> Result<()> {
    let mdns = iroh::discovery::mdns::MdnsDiscovery::builder();
    let endpoint = Endpoint::builder().discovery(mdns).bind().await?;
    println!("endpoint id: {:?}", endpoint.id());

    let router = iroh::protocol::Router::builder(endpoint)
        .accept(ALPN, DbSync)
        .spawn();

    tokio::signal::ctrl_c().await?;

    router.shutdown().await?;

    Ok(())
}
