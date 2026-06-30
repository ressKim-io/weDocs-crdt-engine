//! 머지 마이크로벤치 (criterion) — Tier 1.
//!
//! 가드레일 5: crdt-engine은 "엔진" — 머지 최적화는 criterion 벤치로 증명.
//! 방법론: controller `docs/design/benchmark-methodology.md` §4(Tier1) · §7(측정 위생).
//!
//! 두 그룹으로 decode/merge 비용을 분리(§4):
//!   - `merge`     — alloc·decode 를 setup(타이밍 밖)으로 → **순수 머지 핫패스**만 측정.
//!   - `build_doc` — `Doc::new()`+decode+merge 전부 타이밍 안 → `build_doc - merge` 델타 = decode+alloc.
//!
//! 4개 워크로드(§3 대표성)를 두 그룹에 동일 적용. throughput = ops/sec + MiB/s(§7).

use std::hint::black_box;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use yrs::updates::decoder::Decode;
use yrs::{Doc, ReadTxn, StateVector, Text, Transact, Update};

// ── 워크로드 생성기 ──────────────────────────────────────────────────────────
// 각 블롭 = lib0 v1 update 1개(실서비스 프레임 1개에 대응). 생성은 벤치 타이밍 밖에서만 호출.

/// 동시 편집: n개의 독립(서로 다른 client id) 단일 삽입 update.
fn concurrent_inserts(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("doc");
            let mut txn = doc.transact_mut();
            text.push(&mut txn, &ascii_char(i));
            txn.encode_state_as_update_v1(&StateVector::default())
        })
        .collect()
}

/// 순차 타이핑(가장 흔함): 같은 client 가 n글자를 연속 삽입 → clock 증가.
/// 각 update = 직전 상태벡터 대비 diff(1글자).
fn sequential_typing(n: usize) -> Vec<Vec<u8>> {
    let doc = Doc::new();
    let text = doc.get_or_insert_text("doc");
    let mut prev_sv = StateVector::default();
    let mut blobs = Vec::with_capacity(n);
    for i in 0..n {
        let blob = {
            let mut txn = doc.transact_mut();
            text.push(&mut txn, &ascii_char(i));
            txn.encode_state_as_update_v1(&prev_sv)
        };
        blobs.push(blob);
        prev_sv = doc.transact().state_vector();
    }
    blobs
}

/// 삭제 포함: n글자 삽입 후 절반(n/2)을 단일 문자 삭제 → tombstone 경로(§3.5).
fn with_deletes(n: usize) -> Vec<Vec<u8>> {
    let doc = Doc::new();
    let text = doc.get_or_insert_text("doc");
    let mut prev_sv = StateVector::default();
    let mut blobs = Vec::with_capacity(n + n / 2);
    for i in 0..n {
        let blob = {
            let mut txn = doc.transact_mut();
            text.push(&mut txn, &ascii_char(i));
            txn.encode_state_as_update_v1(&prev_sv)
        };
        blobs.push(blob);
        prev_sv = doc.transact().state_vector();
    }
    for _ in 0..n / 2 {
        let blob = {
            let mut txn = doc.transact_mut();
            text.remove_range(&mut txn, 0, 1);
            txn.encode_state_as_update_v1(&prev_sv)
        };
        blobs.push(blob);
        prev_sv = doc.transact().state_vector();
    }
    blobs
}

/// 큰 붙여넣기: len글자를 한 번에 삽입한 단일 큰 update(배치 머지).
fn large_paste(len: usize) -> Vec<Vec<u8>> {
    let doc = Doc::new();
    let text = doc.get_or_insert_text("doc");
    let mut txn = doc.transact_mut();
    text.push(&mut txn, &"x".repeat(len));
    vec![txn.encode_state_as_update_v1(&StateVector::default())]
}

fn ascii_char(i: usize) -> String {
    ((b'a' + (i % 26) as u8) as char).to_string()
}

// ── 머지 경로 ────────────────────────────────────────────────────────────────

/// 블롭 묶음을 미리 디코드 — 순수 머지 측정에서 제외할 setup 비용.
fn decode_all(blobs: &[Vec<u8>]) -> Vec<Update> {
    blobs
        .iter()
        .map(|bytes| Update::decode_v1(bytes).unwrap())
        .collect()
}

/// 디코드된 update 들을 **프레임당 1 트랜잭션**으로 적용 — `service::apply_v1` 경로 대표.
fn apply_all(doc: &Doc, updates: Vec<Update>) {
    for update in updates {
        doc.transact_mut().apply_update(update).unwrap();
    }
}

/// ops/sec(머지 호출 수) + MiB/s(update 바이트) 동시 보고(§7 throughput).
fn throughput_of(blobs: &[Vec<u8>]) -> Throughput {
    Throughput::ElementsAndBytes {
        elements: blobs.len() as u64,
        bytes: blobs.iter().map(|b| b.len() as u64).sum(),
    }
}

fn bench_merge(c: &mut Criterion) {
    let workloads: [(&str, Vec<Vec<u8>>); 4] = [
        ("sequential_typing_256", sequential_typing(256)),
        ("concurrent_inserts_256", concurrent_inserts(256)),
        ("with_deletes_50pct", with_deletes(256)),
        ("large_paste_10k", large_paste(10_000)),
    ];

    // 순수 머지: alloc·decode 를 setup 으로 빼 머지 핫패스만 측정(§7 위생).
    let mut merge = c.benchmark_group("merge");
    for (name, blobs) in &workloads {
        merge.throughput(throughput_of(blobs));
        merge.bench_function(*name, |b| {
            b.iter_batched(
                || (Doc::new(), decode_all(blobs)),
                |(doc, updates)| {
                    apply_all(&doc, updates);
                    black_box(doc.transact().state_vector());
                },
                BatchSize::SmallInput,
            );
        });
    }
    merge.finish();

    // 전체 구성: Doc::new()+decode+merge 전부 타이밍 안 → merge 그룹과의 델타가 decode+alloc.
    let mut build = c.benchmark_group("build_doc");
    for (name, blobs) in &workloads {
        build.throughput(throughput_of(blobs));
        build.bench_function(*name, |b| {
            b.iter(|| {
                let doc = Doc::new();
                for bytes in blobs {
                    let update = Update::decode_v1(bytes).unwrap();
                    doc.transact_mut().apply_update(update).unwrap();
                }
                black_box(doc.transact().state_vector());
            });
        });
    }
    build.finish();
}

criterion_group!(benches, bench_merge);
criterion_main!(benches);
