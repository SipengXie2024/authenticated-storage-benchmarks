# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Run Commands

```bash
# Build the project (release mode required for benchmarks)
cargo build --release

# Run with parameters
cargo run --release -- --no-stat -k 1m -a mpt

# Run with specific features
cargo build --release --features asb-authdb/light-hash    # Use blake2b instead of keccak256
cargo build --release --features asb-authdb/thread-safe   # Thread-safe RainBlock MPT
cargo build --release --features asb-authdb/lmpts         # Enable LMPTs (requires backend changes)

# Run preconfigured benchmarks (requires 300GB storage, ~8GB memory recommended)
python3 run.py
```

## Project Architecture

This is a modular benchmarking tool for authenticated storage systems, structured as a Cargo workspace:

```
benchmarks/        -> Main binary (asb-main), entry point in src/main.rs
asb-options/       -> CLI argument parsing (structopt-based Options struct)
asb-backend/       -> Key-value database backends (RocksDB, MDBX, in-memory)
asb-authdb/        -> Authenticated storage implementations
asb-tasks/         -> Task generators (random workloads, real Ethereum traces)
asb-profile/       -> Metrics collection and reporting
```

### Data Flow

1. `main.rs` parses CLI options via `asb-options::Options`
2. `asb-backend::backend()` creates the KV storage backend
3. `asb-authdb::new()` wraps the backend with an authenticated storage implementation
4. `asb_tasks::tasks()` generates the workload
5. `run_tasks()` executes benchmarks and collects metrics

### Authenticated Storage Implementations (asb-authdb/src/)

- `raw.rs` - Direct backend writes, no authentication
- `mpt.rs` - OpenEthereum's Merkle Patricia Trie
- `lvmt.rs` - Multi-Layer Versioned Multipoint Trie (main research contribution)
- `amt.rs` - Single Authenticated Multipoint evaluation Tree
- `rain_mpt.rs` - Modified RainBlock MPT variant
- `lmpts.rs` - Conflux's Layered MPTs (feature-gated)

### Key CLI Options

- `-a <algo>`: Algorithm selection (raw, mpt, lvmt, rain, amt20-amt28, lmpts)
- `-b <backend>`: Backend (rocksdb, memory, mdbx)
- `-k <num>`: Number of distinct keys (supports k/m/g suffixes: 1m = 1 million)
- `--real-trace`: Use Ethereum traces from `./trace` directory
- `--no-stat`: Disable backend statistics for accurate timing
- `--shards <n>`: Proof sharding for LVMT (power of 2, 1-65536)

## Special Setup Requirements

- Rust 1.67.0 (pinned in rust-toolchain)
- Create `./pp` directory for cryptography parameters (LVMT/AMT)
- First run of LVMT/AMT generates crypto params (can take hours)
- For LMPTs: manually modify `asb-backend/Cargo.toml` to switch RocksDB dependencies due to version conflicts
