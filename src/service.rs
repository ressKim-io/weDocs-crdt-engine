//! `CrdtEngine` gRPC 서비스 구현 (proto: crdt.proto).
//!
//! M1 골격: 트레이트 시그니처를 생성 코드와 맞추고 빈 스텁 반환.
//! 실제 머지/브로드캐스트는 M1 본 구현.

use std::pin::Pin;

use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

use crate::common::DocRef;
use crate::crdt::crdt_engine_server::CrdtEngine;
use crate::crdt::{ClientFrame, ServerFrame, Snapshot};
use crate::engine::DocRegistry;

/// 엔진 → 클라이언트 서버 스트림 타입(bidi `Sync`의 응답 측).
type ServerFrameStream = Pin<Box<dyn Stream<Item = Result<ServerFrame, Status>> + Send>>;

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

    /// 게이트웨이 ↔ 엔진 양방향 스트림.
    async fn sync(
        &self,
        _request: Request<Streaming<ClientFrame>>,
    ) -> Result<Response<Self::SyncStream>, Status> {
        // TODO(M1): inbound ClientFrame.update를 DocRegistry에 머지하고,
        // 같은 docId 구독자에게 ServerFrame fan-out. 현재는 빈 스트림 스텁.
        let stream = tokio_stream::empty::<Result<ServerFrame, Status>>();
        Ok(Response::new(Box::pin(stream)))
    }

    /// 스냅샷 조회(복원/디버그용).
    async fn get_snapshot(
        &self,
        request: Request<DocRef>,
    ) -> Result<Response<Snapshot>, Status> {
        let doc_id = request.into_inner().doc_id;
        self.registry.ensure(&doc_id).await;
        // TODO(M1): encode_state_as_update_v1 결과를 data로 채운다.
        Ok(Response::new(Snapshot {
            doc_id,
            data: Vec::new(),
        }))
    }
}
