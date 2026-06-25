//! M1 머지 가드레일 — CRDT 수렴 속성 테스트 (SDD §14.6: 머지 전 통과 필수).
//!
//! yrs는 설계상 수렴을 보장하므로, 이 테스트는 **우리의 apply/encode 사용이 v1 경로에서
//! 교환·결합·멱등을 깨지 않는지**를 증명한다. 각 update는 신선한 Doc(고유 client id)에서
//! 만들어 서로 동시(concurrent)하게 만든다.

use proptest::prelude::*;
use yrs::updates::decoder::Decode;
use yrs::{Doc, GetString, ReadTxn, StateVector, Text, Transact, Update};

/// 신선한 Doc에서 한 글자를 append → 그 단일 삽입을 표현하는 v1 update 블롭.
fn single_insert_update(ch: char) -> Vec<u8> {
    let doc = Doc::new();
    let text = doc.get_or_insert_text("doc");
    let mut txn = doc.transact_mut();
    text.push(&mut txn, &ch.to_string());
    txn.encode_state_as_update_v1(&StateVector::default())
}

/// update 블롭들을 주어진 순서로 빈 Doc에 적용한 뒤 최종 텍스트.
fn apply_in_order(updates: &[Vec<u8>]) -> String {
    let doc = Doc::new();
    let text = doc.get_or_insert_text("doc");
    for bytes in updates {
        let update = Update::decode_v1(bytes).expect("v1 decode");
        doc.transact_mut().apply_update(update).expect("apply_update");
    }
    text.get_string(&doc.transact())
}

fn updates_from(indices: &[u8]) -> Vec<Vec<u8>> {
    indices
        .iter()
        .map(|&i| single_insert_update((b'a' + i) as char))
        .collect()
}

proptest! {
    /// 교환·결합: 같은 update 집합을 어떤 순서로 적용해도 최종 상태 동일.
    #[test]
    fn convergence_is_order_independent(indices in proptest::collection::vec(0u8..26, 0..16)) {
        let updates = updates_from(&indices);
        let forward = apply_in_order(&updates);

        let mut reversed = updates.clone();
        reversed.reverse();
        prop_assert_eq!(&forward, &apply_in_order(&reversed));

        let mut sorted = updates.clone();
        sorted.sort(); // 삽입 순서와 무관한 제3의 순서
        prop_assert_eq!(&forward, &apply_in_order(&sorted));
    }

    /// 멱등: 각 update를 두 번 적용해도 한 번과 동일.
    #[test]
    fn convergence_is_idempotent(indices in proptest::collection::vec(0u8..26, 0..16)) {
        let updates = updates_from(&indices);
        let once = apply_in_order(&updates);

        let mut doubled = Vec::with_capacity(updates.len() * 2);
        for u in &updates {
            doubled.push(u.clone());
            doubled.push(u.clone());
        }
        prop_assert_eq!(once, apply_in_order(&doubled));
    }

    /// 수렴: 두 복제본이 update를 서로 다른 도착 순서로 받아도 동일 상태로 수렴(두 브라우저 모델).
    #[test]
    fn two_replicas_converge(indices in proptest::collection::vec(0u8..26, 1..16)) {
        let updates = updates_from(&indices);
        let mid = updates.len() / 2;

        let mut a = updates[..mid].to_vec();
        a.extend_from_slice(&updates[mid..]);
        let mut b = updates[mid..].to_vec();
        b.extend_from_slice(&updates[..mid]);

        prop_assert_eq!(apply_in_order(&a), apply_in_order(&b));
    }
}
