// proto stub 생성 — controller SSOT에서 vendoring한 proto/ 를 tonic-prost-build로 컴파일.
// buf 원격 git input(ADR-0010) → `make proto-sync` 로 proto/ 채운 뒤 빌드.
// 엔진은 서버 전용(게이트웨이가 클라이언트) → build_client(false).
fn main() {
    // tonic-prost-build → prost-build 는 protoc 바이너리를 호출한다(ADR-0010 codegen 경로).
    // 시스템 protoc 설치 의존을 피하려 vendored protoc 를 PROTOC 로 주입.
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc 경로 획득 실패");
    // SAFETY: 빌드 스크립트 시작 시점(단일 스레드) — 다른 스레드가 env 를 읽기 전.
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(
            &["proto/crdt/crdt.proto", "proto/common/common.proto"],
            &["proto"],
        )
        .expect("proto 컴파일 실패 — `make proto-sync` 로 proto/ 를 먼저 채웠는지 확인");

    println!("cargo:rerun-if-changed=proto/crdt/crdt.proto");
    println!("cargo:rerun-if-changed=proto/common/common.proto");
}
