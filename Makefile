all: test fmt

test:
	cargo test

fmt:
	rustup override set nightly
	cargo fmt --all

clippy:
	rustup override set stable
	cargo clippy
	rustup override set nightly

coverage:
	docker run --security-opt seccomp=unconfined -v "${PWD}:/volume" xd009642/tarpaulin
