test:
	cargo test
.PHONY: test

bench:
	RUST_BACKTRACE=1 cargo test tests::bench::bench_10000_sets --release --features=bench -- --nocapture
.PHONY: bench

flame:
	CARGO_PROFILE_RELEASE_DEBUG=true cargo flamegraph --features=bench --root --unit-test -- tests::bench::bench_10000_sets --nocapture
.PHONY: flame