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

/// per-session 아웃바운드 mpsc 버퍼. `engine::FANOUT_CAPACITY`(256)보다 작게 두어
/// slow consumer가 broadcast `Lagged` 전에 여기서 먼저 백프레셔를 받도록 의도(§D-6).
const OUTBOUND_BUFFER: usize = 64;

/// 송신 채널 타입 — 한 세션의 아웃바운드 프레임.
type Outbound = mpsc::Sender<Result<ServerFrame, Status>>;

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

            // cancel-safety: `inbound.message()`의 디코드 상태는 `Streaming`에 보존되고
            // broadcast `recv()`는 문서상 cancel-safe → 어느 select 브랜치가 취소돼도 프레임 무손실.
            // (config-contract-audit: tonic 마이너 업그레이드 시 재확인.)
            loop {
                tokio::select! {
                    incoming = inbound.message() => match incoming {
                        Ok(Some(frame)) => {
                            if !handle_inbound(&registry, &doc_id, frame, &out_tx).await {
                                break;
                            }
                        }
                        Ok(None) => break, // 클라 정상 종료
                        Err(status) => {
                            eprintln!("inbound stream error doc={doc_id}: {status}");
                            break;
                        }
                    },
                    received = fanout.recv() => {
                        if !handle_broadcast(&registry, &doc_id, received, &out_tx).await {
                            break;
                        }
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(out_rx))))
    }

    /// 스냅샷 조회(복원/디버그용) — 전체 상태를 v1로 인코드. 없는 doc는 빈 바이트.
    async fn get_snapshot(&self, request: Request<DocRef>) -> Result<Response<Snapshot>, Status> {
        let doc_id = request.into_inner().doc_id;
        let data = self.registry.full_state_v1(&doc_id).await;
        Ok(Response::new(Snapshot { doc_id, data }))
    }
}

/// 인바운드 ClientFrame 한 개 처리. `false` 반환 시 세션 종료.
async fn handle_inbound(
    registry: &DocRegistry,
    doc_id: &str,
    frame: ClientFrame,
    out_tx: &Outbound,
) -> bool {
    // §D-1: 메타데이터 room과 프레임 doc_id 불일치 = 게이트웨이 오라우팅 →
    // 엉뚱한 doc 교차오염 방지 위해 세션 종료(소프트 무시 아님).
    if !frame.doc_id.is_empty() && frame.doc_id != doc_id {
        let _ = out_tx
            .send(Err(Status::invalid_argument(format!(
                "doc_id mismatch: stream={doc_id}, frame={}",
                frame.doc_id
            ))))
            .await;
        return false;
    }

    // 클라 SyncStep1 → SyncStep2 diff(late-join 핵심). 손상 SV는 그 프레임만 무시(update와 대칭).
    if !frame.state_vector.is_empty() {
        match registry.diff_v1(doc_id, &frame.state_vector).await {
            Ok(diff) => {
                let reply = ServerFrame {
                    update: diff,
                    state_vector: Vec::new(),
                };
                if out_tx.send(Ok(reply)).await.is_err() {
                    return false;
                }
            }
            // TODO(M1.5): eprintln → tracing::warn!(doc_id, %e)
            Err(e) => eprintln!("diff_v1 failed doc={doc_id}: {e}"),
        }
    }

    // 클라 update → 머지 + broadcast. 손상 프레임은 로그만, 스트림/타 클라 유지.
    // TODO(M1.5): eprintln → tracing::warn!(doc_id, %e)
    if !frame.update.is_empty()
        && let Err(e) = registry.apply_v1(doc_id, &frame.update).await
    {
        eprintln!("apply_v1 failed doc={doc_id}: {e}");
    }

    true
}

/// broadcast 수신 한 개를 아웃바운드로 중계. `false` 반환 시 세션 종료.
async fn handle_broadcast(
    registry: &DocRegistry,
    doc_id: &str,
    received: Result<Vec<u8>, RecvError>,
    out_tx: &Outbound,
) -> bool {
    match received {
        Ok(update) => {
            let frame = ServerFrame {
                update,
                state_vector: Vec::new(),
            };
            out_tx.send(Ok(frame)).await.is_ok()
        }
        // §D-5: 유실분 복구 불가 → 전체 상태 재전송으로 재수렴.
        Err(RecvError::Lagged(skipped)) => {
            // TODO(M1.5): lagged 빈발 시 cap 부족 신호 → metric(lagged_total)
            eprintln!("fan-out lagged doc={doc_id} skipped={skipped}; resyncing");
            let full = registry.full_state_v1(doc_id).await;
            let frame = ServerFrame {
                update: full,
                state_vector: Vec::new(),
            };
            out_tx.send(Ok(frame)).await.is_ok()
        }
        Err(RecvError::Closed) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// §D-1: 프레임 doc_id가 스트림 doc_id와 다르면 세션 종료 + InvalidArgument.
    #[tokio::test]
    async fn inbound_rejects_doc_id_mismatch() {
        let registry = DocRegistry::new();
        let (tx, mut rx) = mpsc::channel(4);
        let frame = ClientFrame {
            doc_id: "other-room".into(),
            update: Vec::new(),
            state_vector: Vec::new(),
        };

        let keep = handle_inbound(&registry, "room-1", frame, &tx).await;

        assert!(!keep, "mismatch must end session");
        match rx.recv().await {
            Some(Err(status)) => assert_eq!(status.code(), tonic::Code::InvalidArgument),
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    /// 빈 doc_id(검증 스킵) + 빈 페이로드 → 세션 유지.
    #[tokio::test]
    async fn inbound_accepts_empty_doc_id() {
        let registry = DocRegistry::new();
        registry.open("room-1").await;
        let (tx, _rx) = mpsc::channel(4);
        let frame = ClientFrame {
            doc_id: String::new(),
            update: Vec::new(),
            state_vector: Vec::new(),
        };

        assert!(handle_inbound(&registry, "room-1", frame, &tx).await);
    }
}
