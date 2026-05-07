//! Minimal raw TCP proxy — no frame inspection, just forward bytes.
//! Uses copy_bidirectional to shuttle bytes both ways simultaneously.
use std::net::SocketAddr;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listen: SocketAddr =
        std::env::args().nth(1).unwrap_or("127.0.0.1:9192".into()).parse()?;
    let upstream: SocketAddr =
        std::env::args().nth(2).unwrap_or("127.0.0.1:9092".into()).parse()?;

    let listener = TcpListener::bind(listen).await?;
    eprintln!("raw proxy on {listen} -> {upstream}");

    loop {
        let (mut client, addr) = listener.accept().await?;
        eprintln!("accepted {addr}");
        let up = upstream;
        tokio::spawn(async move {
            let mut broker = TcpStream::connect(up).await.unwrap();
            let _ = tokio::io::copy_bidirectional(&mut client, &mut broker).await;
        });
    }
}
