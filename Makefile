all: test fmt

test:
	cargo test

fmt:
	rustfmt src/*.rs
	rustfmt src/newt/*.rs

# `cargo install cargo-travis`
coverage:
	cargo coverage
	open target/kcov/index.html
