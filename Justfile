# Run `just` to see all available recipes

set shell := ["bash", "-euo", "pipefail", "-c"]

# List available recipes
default:
    @just --list

# ─── Development ──────────────────────────────────────────────

# Build library only
build:
    cargo build

# Build release with CLI binary
build-release:
    cargo build --release

# Run all tests
test:
    cargo test

# Run unit tests only
test-unit:
    cargo test --lib

# Run tests matching a filter
test-filter filter:
    cargo test {{ filter }}

# Run clippy lints on all targets
lint:
    cargo clippy --all-targets

# Format code with dprint
fmt:
    dprint fmt Cargo.toml src/

# Check formatting without writing
fmt-check:
    dprint check Cargo.toml src/

# Run cargo-deny checks (licenses, advisories, bans)
audit:
    cargo deny check

# Run full local CI: format check, lint, test, audit
check: fmt-check lint test audit

