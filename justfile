default:
    @just --list

install-hooks:
    git config core.hooksPath .githooks

build *ARGS:
    cargo build {{ARGS}}

# Types only, no codegen, no lints. Add `-p <crate>` to make it cheaper still.
check *ARGS:
    cargo check --workspace --tests --benches {{ARGS}}

run *ARGS:
    cargo run {{ARGS}}

test *ARGS:
    cargo nextest run --workspace {{ARGS}}

lint:
    cargo clippy --all --tests --benches -- -D warnings

lint-fix:
    cargo clippy --all --tests --benches --fix

fmt-check:
    cargo fmt --all -- --check
    stylua --check plugins/

fmt:
    cargo fmt --all
    stylua plugins/

pylint:
    ruff check scripts/
    ty check scripts/

gen-docs:
    cargo run -p maki-docgen

gen-docs-check:
    cargo run -p maki-docgen -- --check

machete:
    cargo machete

# Criterion benches (maki-lua: luau_perf, splash_perf). Pass a filter to pick
# one, e.g. `just bench -- pull_roundtrip`; slow meters are sample_size 10.
bench *ARGS:
    cargo bench -p maki-lua {{ARGS}}

# Full CI check
ci: fmt-check lint pylint test gen-docs-check machete
