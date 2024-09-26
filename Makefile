librhp:
	@cd lib/rhp && cargo build --release
	@cp lib/rhp/target/release/librhp.a internal/rhp/
