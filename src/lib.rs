//! weDocs CRDT 엔진 — yrs 머지 + tonic gRPC(bidi `Sync`).
//!
//! M1 골격: 빌드 배선(proto codegen · tonic 서비스 트레이트 · yrs 의존)을 확립한다.
//! 실제 수렴 로직(`apply_update`/`encode_state`/fan-out)과 proptest 수렴 통과는
//! M1 본 구현에서 채운다 — 가드레일: 머지 전 commutative/associative/idempotent 통과.

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
