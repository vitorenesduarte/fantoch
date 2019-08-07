all: test fmt

test:
	cargo test

fmt:
	rustfmt src/*

# `cargo install cargo-travis`
coverage:
	cargo coverage
	open target/kcov/index.html
