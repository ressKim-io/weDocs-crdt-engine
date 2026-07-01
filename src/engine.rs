//! docId별 yrs 문서 레지스트리 + fan-out.
//!
//! 전제: 같은 docId = 같은 엔진 인스턴스(Istio waypoint consistent-hash) → 인메모리 머지로 충분.
//! 모든 인코딩은 lib0 **v1**(Yjs 호환) 고정 — v2 사용 시 브라우저 클라가 디코드 불가.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{Mutex, broadcast};
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;
use yrs::{Doc, ReadTxn, StateVector, Transact, Update};

/// per-doc broadcast 채널 용량. per-session 아웃바운드 버퍼(`service::OUTBOUND_BUFFER`=64)보다
/// 크게 잡아, 느린 소비자가 broadcast `Lagged`(→ full resync, §D-5)를 트리거하기 전에
/// 아웃바운드 mpsc에서 먼저 자연 백프레셔가 걸리도록 의도.
const FANOUT_CAPACITY: usize = 256;

/// 엔진 경계 에러. yrs 내부 에러 타입을 메시지로 흡수해 상위(tonic)에서 `Status`로 매핑.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// 스트림이 열리기 전(open 미경유)에 도달한 논리 오류.
    #[error("unknown doc: {0}")]
    UnknownDoc(String),
    /// v1 update/state-vector 디코드 또는 머지 실패(손상된 프레임).
    #[error("v1 codec error: {0}")]
    Codec(String),
}

/// docId 하나의 권위 상태: yrs `Doc` + 구독자 broadcast.
struct DocEntry {
    doc: Doc,
    tx: broadcast::Sender<Vec<u8>>,
}

impl DocEntry {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(FANOUT_CAPACITY);
        Self {
            doc: Doc::new(),
            tx,
        }
    }
}

/// 신규 스트림 open 결과: 구독 수신기 + open 시점 상태벡터(클라에 보낼 SyncStep1).
pub struct Subscription {
    pub receiver: broadcast::Receiver<Vec<u8>>,
    /// v1-encoded state vector — 엔진의 SyncStep1(클라의 오프라인분 pull용).
    pub state_vector: Vec<u8>,
}

/// docId → `DocEntry` 매핑. 다중 커넥션이 공유하므로 `Arc<Mutex<..>>`.
#[derive(Clone, Default)]
pub struct DocRegistry {
    docs: Arc<Mutex<HashMap<String, DocEntry>>>,
}

impl DocRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 신규 스트림 진입점. **구독-후-스냅샷을 같은 락 안에서**(§D-2) 수행해
    /// 구독 전 update가 유실되는 lost-update 윈도를 없앤다.
    pub async fn open(&self, doc_id: &str) -> Subscription {
        let mut docs = self.docs.lock().await;
        let entry = docs.entry(doc_id.to_string()).or_insert_with(DocEntry::new);
        let receiver = entry.tx.subscribe();
        // 락 보유 중 .await 금지 — transact()는 동기.
        let state_vector = entry.doc.transact().state_vector().encode_v1();
        Subscription {
            receiver,
            state_vector,
        }
    }

    /// 인바운드 v1 update를 머지하고, **원본 바이트 그대로** 구독자에게 broadcast.
    /// (yrs 멱등·교환 → 재인코딩 불필요. self-echo는 클라에서 no-op, §D-3.)
    pub async fn apply_v1(&self, doc_id: &str, update: &[u8]) -> Result<(), EngineError> {
        let docs = self.docs.lock().await;
        let entry = docs
            .get(doc_id)
            .ok_or_else(|| EngineError::UnknownDoc(doc_id.to_string()))?;

        let decoded = Update::decode_v1(update).map_err(|e| EngineError::Codec(e.to_string()))?;
        entry
            .doc
            .transact_mut()
            .apply_update(decoded)
            .map_err(|e| EngineError::Codec(e.to_string()))?;

        // 수신자 없음(Err)은 정상 — 무시.
        let _ = entry.tx.send(update.to_vec());
        Ok(())
    }

    /// 클라 state vector(v1)를 받아 클라가 누락한 diff(SyncStep2)를 계산.
    pub async fn diff_v1(&self, doc_id: &str, client_sv: &[u8]) -> Result<Vec<u8>, EngineError> {
        let docs = self.docs.lock().await;
        let entry = docs
            .get(doc_id)
            .ok_or_else(|| EngineError::UnknownDoc(doc_id.to_string()))?;

        let sv =
            StateVector::decode_v1(client_sv).map_err(|e| EngineError::Codec(e.to_string()))?;
        Ok(entry.doc.transact().encode_state_as_update_v1(&sv))
    }

    /// 전체 상태(v1) — Lagged resync(§D-5) / `GetSnapshot` 복원용.
    /// 존재하지 않는 doc는 빈 바이트 — 조회가 빈 Doc를 생성하는 부작용을 두지 않는다.
    /// (Lagged resync 경로는 항상 open 이후라 `None`이 아니다.)
    pub async fn full_state_v1(&self, doc_id: &str) -> Vec<u8> {
        let docs = self.docs.lock().await;
        docs.get(doc_id).map_or_else(Vec::new, |entry| {
            entry
                .doc
                .transact()
                .encode_state_as_update_v1(&StateVector::default())
        })
    }

    /// 현재 보유 중인 문서 수(디버그/관측용).
    pub async fn len(&self) -> usize {
        self.docs.lock().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.docs.lock().await.is_empty()
    }
}
