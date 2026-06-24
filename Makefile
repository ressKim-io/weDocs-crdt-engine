# proto-sync: controller(SSOT)의 proto를 vendoring (proto/ 는 gitignored).
# 로컬 개발 = 로컬 경로(기본), CI/재현 = 원격 git input(태그 핀, ADR-0010).
CONTROLLER ?= ../weDocs-controller/proto

.PHONY: proto-sync build check test bench run clean

proto-sync:
	rm -rf proto
	buf export "$(CONTROLLER)" -o proto
# 원격(canonical, 태그 핀):
#   buf export 'https://github.com/ressKim-io/weDocs-controller.git#subdir=proto,ref=proto-v0.1.0' -o proto

build: proto-sync
	cargo build

check: proto-sync
	cargo check

test: proto-sync
	cargo test

bench: proto-sync
	cargo bench

run: proto-sync
	cargo run

clean:
	cargo clean
	rm -rf proto
