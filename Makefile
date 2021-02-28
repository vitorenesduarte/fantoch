t: test

test:
	cargo test --lib -p fantoch -p fantoch_ps -p fantoch_bote
	cargo hack check --feature-powerset --no-dev-deps

fmt:
	rustup override set nightly
	cargo fmt --all
	rustup override set stable

