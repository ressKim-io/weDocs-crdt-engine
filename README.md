# weDocs-crdt-engine

weDocs의 CRDT 엔진 — **yrs**(Yjs wire 호환) 머지를 **tonic gRPC bidi 스트림**으로 노출한다.
폴리레포 trace 체인 `Java(ws-gateway) → Rust(engine) → …`의 CPU·정확성 핵심 노드.

> 상태: **M1 골격**. 빌드 배선(proto codegen · tonic 서비스 · yrs 의존)만 확립.
> 수렴 로직(`apply_update`/스냅샷/fan-out)과 proptest 수렴 통과는 M1 본 구현에서.

## 스택 (verified 2026-06-25)
- yrs 0.27 · tonic 0.14 + prost 0.14 (hyper 1.0 계열) · tokio 1.x
- proptest(수렴 속성) · criterion(벤치) · edition 2024

## proto (controller SSOT, ADR-0010)
proto는 `weDocs-controller`가 SSOT. 이 레포는 **buf 원격 git input**으로 vendoring한다(submodule 아님).

```sh
make proto-sync     # buf export → proto/ (gitignored)
# 기본: 로컬 ../weDocs-controller/proto
# canonical(CI/재현, 태그 핀):
#   buf export 'https://github.com/ressKim-io/weDocs-controller.git#subdir=proto,ref=proto-v0.1.0' -o proto
```

## 빌드 / 실행
```sh
make check          # proto-sync + cargo check
make build          # proto-sync + cargo build
make test           # proptest 수렴 골격
make run            # 0.0.0.0:50051 (ENGINE_ADDR 로 변경)
```

## 가드레일 (SDD §14)
- 엔진은 "엔진"이다 — 단순 yrs 래퍼 PR 반려, **최적화 + criterion 벤치** 동반.
- **M1 머지 전 proptest 수렴(commutative/associative/idempotent) 통과 필수.**
- 서비스 간 호출은 gRPC + OTel propagator(W3C `traceparent`).
