use super::client::{RpcError, RpcHttpClient, RpcHttpLimits};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::test]
async fn request_budget_fails_before_network_io() {
    let client = RpcHttpClient::new("http://127.0.0.1:1")
        .unwrap()
        .with_limits(RpcHttpLimits {
            max_request_bytes: 1,
            ..RpcHttpLimits::default()
        })
        .unwrap();

    assert!(matches!(client.chain_id().await, Err(RpcError::Limit(_))));
}

#[tokio::test]
async fn chunked_response_body_is_capped_before_deserialization() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0_u8; 1024];
        let _ = socket.read(&mut request).await.unwrap();
        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n80\r\n",
            )
            .await
            .unwrap();
        socket.write_all(&[b'x'; 128]).await.unwrap();
        socket.write_all(b"\r\n0\r\n\r\n").await.unwrap();
    });
    let client = RpcHttpClient::new(format!("http://{address}"))
        .unwrap()
        .with_limits(RpcHttpLimits {
            max_response_bytes: 64,
            ..RpcHttpLimits::default()
        })
        .unwrap();

    let error = client.chain_id().await.unwrap_err();
    assert!(matches!(error, RpcError::Limit(_)));
    server.await.unwrap();
}
