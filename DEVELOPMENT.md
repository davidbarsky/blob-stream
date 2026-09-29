# Development Guide

This guide is the canonical workflow reference for contributors. See the
[repository overview](README.md) for the full documentation map and [Design](docs/design/README.md)
before changing architecture-sensitive behavior.

## Choose Your Environment

Use the workflow that matches how the repository was checked out.

| Environment | Build, lint, and test | Formatting |
| --- | --- | --- |
| Standalone `blob-stream` clone | Cargo and Cargo Nextest | `cargo +nightly fmt` |
| Blob Stream in the monorepo | Bazel from the monorepo root or `../bazelw` from this directory | Root `just rustfmt`; use `just format` and `just check-format` for TOML |

The monorepo's [AGENTS.md](AGENTS.md) adds mandatory agent constraints, especially for deterministic
integration tests. It does not replace this guide.

## Prerequisites

- Rust toolchain for edition 2024
- Docker and Docker Compose for local dependencies
- AWS CLI when using optional local-cloud emulation commands
- Bazel wrapper dependencies when working in the monorepo

## Build, Lint, And Test

### Standalone Clone

Run from the `blob-stream` root:

```bash
cargo build --workspace
cargo clippy --workspace --bins --examples --tests -- --no-deps
cargo nextest run
```

Run only the integration-test crate when needed:

```bash
RUST_LOG=off cargo nextest run -p blob-stream-integration-tests
```

Use `RUST_LOG=blob_stream=trace,bd=trace` only for targeted investigation.

Run the simulation tests, which need no Docker services:

```bash
cargo nextest run -p blob-stream-sim
```

These tests run production S3 client code against an in-process S3 whose hosts talk TCP over
[turmoil-net](https://crates.io/crates/turmoil-net) on a paused tokio clock, so a scenario that
waits out a 15-second AWS operation timeout completes in milliseconds of wall time. Tests can
partition or hold links and crash or restart hosts. The property test draws its workload and every
server fault, network fault, and host crash from [hegel](https://github.com/hegeldev/hegel-rust)
and shrinks a failure to its fewest faults. Raise its case count or replay a seed with the
`HEGEL_TEST_CASES` and `HEGEL_SEED` environment variables; CI runs it derandomized.

### Monorepo Worktree

Run Bazel tests and Clippy from the monorepo root, or use `../bazelw` from the `blob-stream`
directory. For example:

```bash
../bazelw test //blob-stream/blob-stream-metadata-store:unit-test
../bazelw test --config=clippy //blob-stream/blob-stream-metadata-store:blob-stream-metadata-store__clippy
```

Service-backed Blob Stream integration tests must use their generated Nextest wrappers, not raw
`__libtest` targets. See [AGENTS.md](AGENTS.md) and
[plans/TEST_AUDIT.md](plans/TEST_AUDIT.md) for deterministic-test requirements and focused commands.

## Format And Verify Changes

For a standalone clone, format Rust from this directory:

```bash
cargo +nightly fmt
```

For a monorepo checkout, follow the root execution profile: format Rust with the root `just
rustfmt` workflow. When TOML changes, also run from the monorepo root:

```bash
just format
just check-format
```

Before submitting a change, run the narrowest relevant build, lint, and test command for the
checkout context, inspect editor diagnostics, and run:

```bash
git diff --check
```

## Local Infrastructure

Start S3 and DynamoDB emulation:

```bash
docker compose up -d
```

This starts DynamoDB Local at `http://localhost:8000` and LocalStack S3 at
`http://localhost:4566`.

## Local End-To-End Walkthrough

For an interactive local system with two static-discovery brokers, a text producer, and a shared
consumer group, use the [local end-to-end walkthrough](examples/local-e2e/README.md). It owns its
own Docker Compose resources and uses Cargo commands for every user-facing process. The walkthrough
is for observing normal producer, broker, and consumer behavior; use the stress runner below for
an automated load and correctness check.

## Local Stress Runner

The optional stress runner exercises in-process TCP brokers, LocalStack S3, DynamoDB Local, the
producer client, and production consumer bootstrap. Each run creates and cleans up an isolated
bucket and DynamoDB table set.

Start local dependencies, then run a small workload:

```bash
RUST_LOG=off cargo run -p blob-stream-integration-tests --bin blob-stream-stress -- \
  --brokers 1 --producers 1 --consumers 1 --partitions 4 --records 100
```

### Monorepo Worktree

From the monorepo root, use the itest-backed Bazel launcher instead of starting Compose. It starts
and reuses the isolated DynamoDB and LocalStack pool required by the stress runner, then passes the
pool endpoints to the process. Separate invocations can run concurrently; each run uses isolated
bucket and DynamoDB table names.

Small workload:

```bash
RUST_LOG=off ./bazelw run //blob-stream/blob-stream-integration-tests:blob-stream-stress-itest -- \
  --brokers 1 --producers 1 --consumers 1 --partitions 4 --records 100
```

A larger example:

```bash
RUST_LOG=off cargo run -p blob-stream-integration-tests --bin blob-stream-stress -- \
  --brokers 3 --producers 4 --consumers 3 --partitions 16 --records 100000 \
  --payload-bytes 1024 --overall-timeout-seconds 600 --producer-timeout-seconds 600 \
  --drain-timeout-seconds 120
```

Run the same larger workload through Bazel with:

```bash
RUST_LOG=off ./bazelw run //blob-stream/blob-stream-integration-tests:blob-stream-stress-itest -- \
  --brokers 3 --producers 4 --consumers 3 --partitions 16 --records 100000 \
  --payload-bytes 1024 --overall-timeout-seconds 600 --producer-timeout-seconds 600 \
  --drain-timeout-seconds 120
```

Use `./bazelw run //tools/itest:pool-stop` when no itest invocation is active and a fresh local
service pool is needed.

The runner validates every acknowledged record and intentionally treats duplicate delivery as a
stress-test failure, even though the service delivery contract is at least once. It reports active
stages and live counters to stderr. Use its timeout and batching flags to investigate a specific
phase rather than treating a larger overall deadline as a fix.

## Rustdocs And Doctests

Build local API documentation:

```bash
cargo doc --workspace --no-deps --open
```

Run doctests for public crates:

```bash
cargo test -p blob-stream-producer --doc
cargo test -p blob-stream-consumer --doc
cargo test -p blob-stream-broker-discovery --doc
cargo test -p blob-stream-types --doc
cargo test -p blob-stream-blob-store --doc
cargo test -p blob-stream-metadata-store --doc
```

## Related Guides

- [Design](docs/design/README.md): system behavior and correctness contracts
- [Infrastructure setup](docs/infrastructure.md): local and production resources
- [Operations guide](docs/operations.md): runtime controls and diagnostics
- [Integration-test audit](plans/TEST_AUDIT.md): deterministic test rules and hardening work
- [TLA+ model](tla/README.md): formal model and verification workflow
