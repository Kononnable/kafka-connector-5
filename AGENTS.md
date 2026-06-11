# Repository
Rust Kafka client + protocol codegen + transparent proxy. Workspace (edition 2024, resolver 3).

Crates:
- **client/** — main crate. Single-threaded mio event loop (NOT tokio)
- **protocol/** — code-gen from JSON schemas. Rarely changed.
- **proxy/** — tokio-based TCP proxy. Binary at `proxy/src/bin/proxy.rs`.

**Do not edit protocol/ or proxy/ without direct user approval.**

# Commands
build:     cargo build -p <crate>
test all:  cargo test
test pkg:  cargo test -p <crate>
codegen:   cargo test test_codegen_generated_structs -- --nocapture
regenerate: UPDATE_EXPECT=1 cargo test test_codegen_generated_structs -- --nocapture
check:     cargo clippy --all-targets
fmt:       cargo +nightly fmt
           rustfmt.toml: imports_granularity = "Module", group_imports = "StdExternalCrate"

# Architecture
- Event loop: `loop { epoll_wait → drain channel commands → tick state machines → io }`
- State machines are pure-logic structs (`tick(now)` / `on_response(resp)`) — no I/O.
- See `client/architecture.md` and `client/design-decisions.md`.
- Full KIP reference at `../knowledge/kip/README.md`.

# Tests
- Codegen test writes to `src/generated/`, runs clippy `--deny warnings`, checks expect_test.
- Proxy e2e: `proxy/tests/test_multi_broker.sh` (needs `docker-compose up` + `kcat`).
- Kafka cluster: `docker-compose.yml` (3 KRaft brokers, no ZK, ports 19092-39092).

# Planning template
1. [Step] → verify: [check]
