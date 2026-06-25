//! 머지 처리량 벤치마크 (criterion).
//!
//! 가드레일 5: crdt-engine은 "엔진" — 머지 최적화는 criterion 벤치로 증명.
//! 핫패스 = `Update::decode_v1` + `apply_update`(머지) 배치.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use yrs::updates::decoder::Decode;
use yrs::{Doc, ReadTxn, StateVector, Text, Transact, Update};

/// n개의 독립(서로 다른 client id) 단일 삽입 update를 미리 생성.
fn make_updates(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("doc");
            let mut txn = doc.transact_mut();
            text.push(&mut txn, &((b'a' + (i % 26) as u8) as char).to_string());
            txn.encode_state_as_update_v1(&StateVector::default())
        })
        .collect()
}

fn bench_merge(c: &mut Criterion) {
    let updates = make_updates(256);

    c.bench_function("apply_256_concurrent_updates", |b| {
        b.iter(|| {
            // apply_update가 update에서 "doc" 텍스트 타입을 정의 → 사전 등록 불필요.
            let doc = Doc::new();
            for bytes in &updates {
                let update = Update::decode_v1(bytes).unwrap();
                doc.transact_mut().apply_update(update).unwrap();
            }
            black_box(doc.transact().state_vector());
        });
    });
}

criterion_group!(benches, bench_merge);
criterion_main!(benches);
