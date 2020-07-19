fmt:
	rustup override set nightly
	cargo fmt --all
	rustup override set stable
