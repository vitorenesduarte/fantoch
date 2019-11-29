all: test fmt

test:
	cargo test

fmt:
	rustup override set nightly
	rustfmt src/*.rs
	rustfmt src/newt/*.rs
	rustfmt src/bote/*.rs
	rustup override set stable

# `cargo install cargo-travis`
coverage:
	cargo coverage
	open target/kcov/index.html
