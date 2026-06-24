//! weDocs crdt-engine 바이너리 — tonic 서버 부트스트랩.

use std::net::SocketAddr;

use tonic::transport::Server;
use wedocs_crdt_engine::crdt::crdt_engine_server::CrdtEngineServer;
use wedocs_crdt_engine::service::CrdtEngineService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = std::env::var("ENGINE_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50051".to_string())
        .parse()?;

    let service = CrdtEngineService::new();
    println!("weDocs crdt-engine listening on {addr}");

    Server::builder()
        .add_service(CrdtEngineServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
