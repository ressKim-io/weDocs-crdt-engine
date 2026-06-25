//! `CrdtEngine` gRPC 서비스 구현 (proto: crdt.proto).
//!
//! 게이트웨이 ↔ 엔진 bidi 브리지. 게이트웨이가 y-websocket 와이어를 ClientFrame으로 번역하면
//! 엔진은 yrs 권위 머지 + 같은 doc 구독자 fan-out을 수행한다(설계: docs/plans/2026-06-25-m1-convergence-impl.md §C/§D).

use std::pin::Pin;

use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::common::DocRef;
use crate::crdt::crdt_engine_server::CrdtEngine;
use crate::crdt::{ClientFrame, ServerFrame, Snapshot};
use crate::engine::DocRegistry;

/// 엔진 → 클라이언트 서버 스트림 타입(bidi `Sync`의 응답 측).
type ServerFrameStream = Pin<Box<dyn Stream<Item = Result<ServerFrame, Status>> + Send>>;

/// 아웃바운드 채널 버퍼 — 한 세션의 백프레셔 경계.
const OUTBOUND_BUFFER: usize = 64;

#[derive(Clone, Default)]
pub struct CrdtEngineService {
    registry: DocRegistry,
}

impl CrdtEngineService {
    pub fn new() -> Self {
        Self::default()
    }
}

#[tonic::async_trait]
impl CrdtEngine for CrdtEngineService {
    type SyncStream = ServerFrameStream;

    /// 게이트웨이 ↔ 엔진 양방향 스트림. docId는 gRPC 메타데이터 `doc-id`로 전달(§D-1) —
    /// open 시점에 ClientFrame이 아직 없어도 doc를 식별할 수 있어야 하기 때문.
    async fn sync(
        &self,
        request: Request<Streaming<ClientFrame>>,
    ) -> Result<Response<Self::SyncStream>, Status> {
        let doc_id = request
            .metadata()
            .get("doc-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
            .ok_or_else(|| Status::invalid_argument("missing doc-id metadata"))?;

        let mut inbound = request.into_inner();
        let registry = self.registry.clone();
        let subscription = registry.open(&doc_id).await;
        let mut fanout = subscription.receiver;

        let (out_tx, out_rx) = mpsc::channel::<Result<ServerFrame, Status>>(OUTBOUND_BUFFER);

        tokio::spawn(async move {
            // 엔진 → 클라 SyncStep1: 클라가 가진 오프라인분을 pull하게 한다.
            let step1 = ServerFrame {
                update: Vec::new(),
                state_vector: subscription.state_vector,
            };
            if out_tx.send(Ok(step1)).await.is_err() {
                return;
            }

            loop {
                tokio::select! {
                    incoming = inbound.message() => {
                        match incoming {
                            Ok(Some(frame)) => {
                                if !frame.state_vector.is_empty() {
                                    // 클라 SyncStep1 → SyncStep2 diff 응답(late-join 핵심).
                                    match registry.diff_v1(&doc_id, &frame.state_vector).await {
                                        Ok(diff) => {
                                            let reply = ServerFrame { update: diff, state_vector: Vec::new() };
                                            if out_tx.send(Ok(reply)).await.is_err() {
                                                break;
                                            }
                                        }
                                        Err(e) => {
                                            let _ = out_tx.send(Err(Status::internal(e.to_string()))).await;
                                            break;
                                        }
                                    }
                                }
                                if !frame.update.is_empty() {
                                    // 손상 프레임은 로그만 — 스트림/타 클라 유지.
                                    if let Err(e) = registry.apply_v1(&doc_id, &frame.update).await {
                                        eprintln!("apply_v1 failed doc={doc_id}: {e}");
                                    }
                                }
                            }
                            Ok(None) => break, // 클라 정상 종료
                            Err(status) => {
                                eprintln!("inbound stream error doc={doc_id}: {status}");
                                break;
                            }
                        }
                    }
                    broadcasted = fanout.recv() => {
                        match broadcasted {
                            Ok(update) => {
                                let frame = ServerFrame { update, state_vector: Vec::new() };
                                if out_tx.send(Ok(frame)).await.is_err() {
                                    break;
                                }
                            }
                            // §D-5: 유실분 복구 불가 → 전체 상태 재전송으로 재수렴.
                            Err(RecvError::Lagged(skipped)) => {
                                eprintln!("fan-out lagged doc={doc_id} skipped={skipped}; resyncing");
                                let full = registry.full_state_v1(&doc_id).await;
                                let frame = ServerFrame { update: full, state_vector: Vec::new() };
                                if out_tx.send(Ok(frame)).await.is_err() {
                                    break;
                                }
                            }
                            Err(RecvError::Closed) => break,
                        }
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(out_rx))))
    }

    /// 스냅샷 조회(복원/디버그용) — 전체 상태를 v1로 인코드.
    async fn get_snapshot(&self, request: Request<DocRef>) -> Result<Response<Snapshot>, Status> {
        let doc_id = request.into_inner().doc_id;
        let data = self.registry.full_state_v1(&doc_id).await;
        Ok(Response::new(Snapshot { doc_id, data }))
    }
}
