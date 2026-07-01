//! 엔진 fan-out 통합 테스트 — gRPC 전송 없이 `DocRegistry` 핵심 경로 검증.
//! (gRPC end-to-end는 Phase 3 프론트 E2E에서 실제 게이트웨이로 검증.)

use wedocs_crdt_engine::engine::{DocRegistry, EngineError};
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;
use yrs::{Doc, GetString, ReadTxn, StateVector, Text, Transact, Update};

fn insert_update(ch: char) -> Vec<u8> {
    let doc = Doc::new();
    let text = doc.get_or_insert_text("doc");
    let mut txn = doc.transact_mut();
    text.push(&mut txn, &ch.to_string());
    txn.encode_state_as_update_v1(&StateVector::default())
}

fn text_of(update: &[u8]) -> String {
    let doc = Doc::new();
    let text = doc.get_or_insert_text("doc");
    doc.transact_mut()
        .apply_update(Update::decode_v1(update).unwrap())
        .unwrap();
    text.get_string(&doc.transact())
}

#[tokio::test]
async fn fanout_delivers_update_to_all_subscribers() {
    let registry = DocRegistry::new();
    let doc_id = "room-1";

    // 두 세션(두 브라우저) open → 각자 구독.
    let mut a = registry.open(doc_id);
    let mut b = registry.open(doc_id);

    // 세션 A가 'x' update 전송 → 머지 + broadcast.
    let ux = insert_update('x');
    registry.apply_v1(doc_id, &ux).unwrap();

    // 양쪽 수신기 모두 동일 바이트 수신(self-echo 포함, §D-3).
    assert_eq!(a.receiver.recv().await.unwrap(), ux);
    assert_eq!(b.receiver.recv().await.unwrap(), ux);
}

#[tokio::test]
async fn diff_returns_full_state_for_empty_state_vector() {
    let registry = DocRegistry::new();
    let doc_id = "room-2";
    registry.open(doc_id);

    registry.apply_v1(doc_id, &insert_update('y')).unwrap();

    // 빈 state vector → 전체 상태 diff. 적용 시 'y' 복원.
    let empty_sv = StateVector::default().encode_v1();
    let snapshot = registry.diff_v1(doc_id, &empty_sv).unwrap();
    assert_eq!(text_of(&snapshot), "y");
}

#[tokio::test]
async fn corrupt_update_is_rejected_not_panicked() {
    let registry = DocRegistry::new();
    let doc_id = "room-3";
    registry.open(doc_id);

    // 손상된 v1 바이트 → Err(Codec) 구체 변종, 패닉 금지.
    let err = registry.apply_v1(doc_id, &[0xff, 0xff, 0xff]).unwrap_err();
    assert!(
        matches!(err, EngineError::Codec(_)),
        "expected Codec, got {err:?}"
    );
}
