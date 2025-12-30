use iroh::{Endpoint, EndpointAddr, EndpointId};
use crate::serve::ALPN;
use anyhow::Result;

#[tokio::main]
pub async fn run(endpoint_id: EndpointId) -> Result<()> {
    let endpoint = Endpoint::bind().await?;

    // Open a connection to the accepting endpoint
    let conn = endpoint.connect(EndpointAddr::new(endpoint_id), ALPN).await?;

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