//! weDocs crdt-engine 바이너리 — tonic 서버 부트스트랩.

use std::net::SocketAddr;

use tonic::transport::Server;
use wedocs_crdt_engine::crdt::crdt_engine_server::CrdtEngineServer;
use wedocs_crdt_engine::service::CrdtEngineService;
use wedocs_crdt_engine::telemetry;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 트레이싱 먼저 — 이후 로그가 subscriber/OTel 레이어를 타게.
    let provider = telemetry::init();

    let addr: SocketAddr = std::env::var("ENGINE_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50051".to_string())
        .parse()?;

    let service = CrdtEngineService::new();
    tracing::info!(%addr, "weDocs crdt-engine listening");

    // ctrl_c로 graceful shutdown → 종료 전 batch span을 flush(provider.shutdown).
    Server::builder()
        .add_service(CrdtEngineServer::new(service))
        .serve_with_shutdown(addr, async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutdown signal received");
        })
        .await?;

    // shutdown()은 blocking(배치 스레드 join) — async 컨텍스트에서 직접 호출하면 워커 스레드를
    // 막거나(멀티스레드) 데드락(현재스레드) 위험. OTel 공식 권장대로 spawn_blocking으로 격리.
    if let Some(provider) = provider {
        let _ = tokio::task::spawn_blocking(move || {
            if let Err(error) = provider.shutdown() {
                tracing::warn!(error = %error, "tracer provider shutdown 실패(일부 span 미flush 가능)");
            }
        })
        .await;
    }

    Ok(())
}
