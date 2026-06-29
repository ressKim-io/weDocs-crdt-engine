//! weDocs CRDT 엔진 — yrs 머지 + tonic gRPC(bidi `Sync`).
//!
//! M1 본 구현: docId별 yrs 권위 머지(`engine::DocRegistry`) + broadcast fan-out,
//! gRPC bidi 브리지(`service`). 모든 인코딩은 lib0 v1(Yjs 호환) 고정.
//! 가드레일: 수렴(commutative/idempotent)은 `tests/convergence_proptest.rs`에서 증명.

// proto SSOT(controller) → tonic-prost-build 생성물. 패키지별 모듈.
// crdt.proto가 common.proto를 import → crdt 모듈 내부에서 `super::common` 참조.
pub mod common {
    tonic::include_proto!("common");
}
pub mod crdt {
    tonic::include_proto!("crdt");
}

pub mod engine;
pub mod service;
pub mod telemetry;
