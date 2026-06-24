//! 수렴/머지 처리량 벤치마크 (criterion).
//!
//! 골격: 하니스만 연결. M1 본 구현에서 yrs `apply_update` 머지 처리량을 측정한다.
//! (SDD 가드레일: crdt-engine은 "엔진" — 최적화는 criterion 벤치로 증명.)

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_merge_placeholder(c: &mut Criterion) {
    c.bench_function("merge_placeholder", |b| {
        b.iter(|| {
            // TODO(M1): yrs Doc에 update 배치를 apply하고 머지 시간을 측정.
            let mut acc = 0u64;
            for i in 0..256u64 {
                acc = acc.wrapping_add(i);
            }
            std::hint::black_box(acc)
        });
    });
}

criterion_group!(benches, bench_merge_placeholder);
criterion_main!(benches);
