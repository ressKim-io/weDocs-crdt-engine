//! docId별 yrs 문서 레지스트리.
//!
//! 전제: 같은 docId = 같은 엔진 인스턴스(Istio waypoint consistent-hash) → 인메모리 머지로 충분.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;
use yrs::Doc;

/// docId → yrs `Doc` 매핑. 다중 커넥션이 공유하므로 `Arc<Mutex<..>>`.
#[derive(Clone, Default)]
pub struct DocRegistry {
    docs: Arc<Mutex<HashMap<String, Doc>>>,
}

impl DocRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// docId의 yrs 문서가 없으면 생성한다(존재 보장).
    pub async fn ensure(&self, doc_id: &str) {
        let mut docs = self.docs.lock().await;
        docs.entry(doc_id.to_string()).or_insert_with(Doc::new);
    }

    /// 현재 보유 중인 문서 수(디버그/관측용).
    pub async fn len(&self) -> usize {
        self.docs.lock().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.docs.lock().await.is_empty()
    }
}

// TODO(M1): apply_update(doc_id, &[u8]) — Yjs binary update를 yrs 트랜잭션에 머지.
// TODO(M1): encode_state_as_update(doc_id, state_vector) — 신규 접속 시 최소 diff.
// TODO(M1): 머지 구현 전 proptest 수렴(commutative/associative/idempotent) 통과 필수(가드레일).
