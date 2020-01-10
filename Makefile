all: test fmt

test:
	cargo test

fmt:
	rustup override set nightly
	cargo fmt --all
	# rustup override set stable

coverage:
	docker run --security-opt seccomp=unconfined -v "${PWD}:/volume" xd009642/tarpaulin
