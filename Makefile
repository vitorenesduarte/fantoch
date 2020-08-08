t: test

test:
	cd fantoch && cargo test --release && cd ..
	cd fantoch_ps && cargo test --release && cd ..
	cd fantoch_plot && cargo test --release && cd ..
	cargo hack check --feature-powerset --no-dev-deps

fmt:
	rustup override set nightly
	cargo fmt --all
	rustup override set stable

