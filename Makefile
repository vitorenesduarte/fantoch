all: test fmt

test:
	cargo test

fmt:
	rustup override set nightly
	rustfmt src/*.rs
	rustfmt src/newt/*.rs
	rustfmt src/bote/*.rs
	rustup override set stable

coverage:
	docker run --security-opt seccomp=unconfined -v "${PWD}:/volume" xd009642/tarpaulin
