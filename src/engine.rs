//! docId별 yrs 문서 레지스트리 + fan-out.
//!
//! 전제: 같은 docId = 같은 엔진 인스턴스(Istio waypoint consistent-hash) → 인메모리 머지로 충분.
//! 모든 인코딩은 lib0 **v1**(Yjs 호환) 고정 — v2 사용 시 브라우저 클라가 디코드 불가.

use std::fmt;
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::sync::broadcast;
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;
use yrs::{Doc, ReadTxn, StateVector, Transact, Update};

/// docId 도메인 식별자 — raw `String` 남용(primitive obsession, layering-readability.md P3) 방지.
/// 검증 로직 없이 순수 wrap만 한다(과잉설계 금지) — 필요해지면 그때 추가.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DocId(String);

impl DocId {
    /// 참조 접근(할당 없음) — 로깅/비교 등에 사용.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// 소유권 반환 — proto 응답 필드(`doc_id: String`) 구성 등 경계 unwrap 전용.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Display for DocId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for DocId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for DocId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

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

/// docId → `DocEntry` 매핑. `DashMap` = 버킷 단위 동시 접근(문서 간 병렬 open/apply),
/// 문서별 `parking_lot::Mutex` = 짧은 동기 임계구역(한 문서 안에서만 직렬화).
/// 두 락 모두 임계구역 안에 `.await` 없음(yrs `transact()`는 동기) → 동기락이 옳다(concurrency.md P5).
#[derive(Clone, Default)]
pub struct DocRegistry {
    docs: Arc<DashMap<DocId, Arc<Mutex<DocEntry>>>>,
}

impl DocRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 신규 스트림 진입점. **구독-후-스냅샷을 문서별 락 안에서**(§D-2) 수행해
    /// 구독 전 update가 유실되는 lost-update 윈도를 없앤다.
    ///
    /// 2단계 락: (1) DashMap 샤드 가드로 `Arc<Mutex<DocEntry>>` 핸들을 얻어 즉시 clone·drop(짧음),
    /// (2) 그 핸들의 문서별 락으로 subscribe+snapshot을 원자적으로 수행. 다른 문서의 동시 open은
    /// 영향받지 않는다 — 이 문서만 잠근다. (샤드 가드를 임계구역 동안 붙들면 샤딩이 무력화되므로 금지.)
    pub fn open(&self, doc_id: &DocId) -> Subscription {
        let handle = self
            .docs
            .entry(doc_id.clone())
            .or_insert_with(|| Arc::new(Mutex::new(DocEntry::new())))
            .value()
            .clone(); // Arc::clone — DashMap 샤드 가드는 이 문장 끝에서 drop
        let entry = handle.lock();
        let receiver = entry.tx.subscribe();
        let state_vector = entry.doc.transact().state_vector().encode_v1();
        Subscription {
            receiver,
            state_vector,
        }
    }

    /// 인바운드 v1 update를 머지하고, **원본 바이트 그대로** 구독자에게 broadcast.
    /// (yrs 멱등·교환 → 재인코딩 불필요. self-echo는 클라에서 no-op, §D-3.)
    pub fn apply_v1(&self, doc_id: &DocId, update: &[u8]) -> Result<(), EngineError> {
        let handle = self
            .docs
            .get(doc_id)
            .ok_or_else(|| EngineError::UnknownDoc(doc_id.to_string()))?
            .value()
            .clone();
        let entry = handle.lock();

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
    pub fn diff_v1(&self, doc_id: &DocId, client_sv: &[u8]) -> Result<Vec<u8>, EngineError> {
        let handle = self
            .docs
            .get(doc_id)
            .ok_or_else(|| EngineError::UnknownDoc(doc_id.to_string()))?
            .value()
            .clone();
        let entry = handle.lock();

        let sv =
            StateVector::decode_v1(client_sv).map_err(|e| EngineError::Codec(e.to_string()))?;
        Ok(entry.doc.transact().encode_state_as_update_v1(&sv))
    }

    /// 전체 상태(v1) — Lagged resync(§D-5) / `GetSnapshot` 복원용.
    /// 존재하지 않는 doc는 빈 바이트 — 조회가 빈 Doc를 생성하는 부작용을 두지 않는다.
    /// (Lagged resync 경로는 항상 open 이후라 `None`이 아니다.)
    pub fn full_state_v1(&self, doc_id: &DocId) -> Vec<u8> {
        let Some(handle) = self.docs.get(doc_id).map(|r| r.value().clone()) else {
            return Vec::new();
        };
        let entry = handle.lock();
        entry
            .doc
            .transact()
            .encode_state_as_update_v1(&StateVector::default())
    }

    /// 현재 보유 중인 문서 수(디버그/관측용).
    pub fn len(&self) -> usize {
        self.docs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.docs.is_empty()
    }
}
