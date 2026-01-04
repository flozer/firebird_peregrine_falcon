# CLAUDE.md - AI Assistant Guide for Firebird Peregrine Falcon

This document provides comprehensive guidance for AI assistants working with the Firebird Peregrine Falcon codebase.

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture & Design Philosophy](#architecture--design-philosophy)
- [Codebase Structure](#codebase-structure)
- [Key Components](#key-components)
- [Development Workflows](#development-workflows)
- [Performance Considerations](#performance-considerations)
- [Coding Conventions](#coding-conventions)
- [Dependencies](#dependencies)
- [Common Tasks](#common-tasks)
- [Important Notes](#important-notes)

---

## Project Overview

**Firebird Peregrine Falcon** is an ultra-fast Rust-based Firebird database to Parquet file extractor with aggressive performance optimizations.

### Key Metrics
- **Target Performance**: 156,000-270,000 rows/second
- **Speedup**: 100-200x faster than naive approaches
- **Default Parallelism**: 2x CPU cores (40-60 workers typical)
- **Batch Sizes**: 500K-1M rows per batch
- **Cross-Platform**: Windows and Linux compatible

### Core Purpose
Extract large Firebird database tables to Parquet format with maximum speed by leveraging:
- Parallel PK partitioning
- Multiple writer threads
- Large batch sizes
- Aggressive prefetching
- No ORDER BY clauses (speed over ordering)

---

## Architecture & Design Philosophy

### Design Principles

1. **Performance First**: Every design decision prioritizes speed
2. **Parallel Everything**: Extraction, writing, and processing parallelized
3. **Memory Efficiency**: Large batches balanced with memory constraints
4. **No Ordering**: Skip ORDER BY clauses unless absolutely necessary
5. **Cross-Platform**: Use Rust stdlib, avoid platform-specific code

### Execution Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    1. Metadata Loading                      │
│  • Detect Primary Key (PK)                                  │
│  • Load column definitions                                  │
│  • Estimate row count                                       │
│  • Calculate MIN/MAX for PK columns                         │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                  2. Partitioning Strategy                   │
│  • If PK exists: Parallel PK partitioning                   │
│  • If no PK: Optimized sequential extraction                │
│  • Calculate partition boundaries                           │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│               3. Parallel Extraction (Rayon)                │
│  • N workers extract partitions simultaneously              │
│  • Each writes to temporary parquet file                    │
│  • No ORDER BY for maximum speed                            │
│  • Large batch sizes (500K-1M rows)                         │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                   4. Merging & Cleanup                      │
│  • Merge all temp files into final parquet                  │
│  • Delete temporary files                                   │
│  • Report statistics                                        │
└─────────────────────────────────────────────────────────────┘
```

### Thread Model

1. **Main Thread**: Coordinates extraction, handles CLI
2. **Worker Threads (Rayon)**: Parallel partition extraction (N = parallelism)
3. **Fetcher Threads**: Prefetch data from Firebird (sequential mode)
4. **Writer Threads**: Write Arrow batches to Parquet files

---

## Codebase Structure

```
firebird_peregrine_falcon/
├── .github/
│   └── workflows/
│       └── ci.yml              # GitHub Actions CI pipeline
├── src/
│   ├── main.rs                 # CLI entry point, argument parsing
│   ├── lib.rs                  # Library exports
│   ├── config.rs               # ExtractorConfig struct
│   └── extractor.rs            # Core extraction logic (830+ lines)
├── Cargo.toml                  # Rust dependencies & build config
├── .gitignore                  # Git ignore rules
├── LICENSE                     # MIT License
├── README.md                   # User-facing documentation
├── GITHUB_SETUP.md             # GitHub setup instructions
├── run_agile_log_obrigacao.ps1 # Example PowerShell script
└── CLAUDE.md                   # This file
```

---

## Key Components

### 1. `main.rs` (CLI Entry Point)

**Location**: `src/main.rs:1-79`

**Responsibilities**:
- Parse CLI arguments using `clap`
- Calculate default parallelism (2x CPU cores)
- Create `ExtractorConfig`
- Instantiate `Extractor` and run extraction
- Display extraction statistics

**Key CLI Arguments**:
```rust
--database <path>        // Firebird .fdb file path
--out-dir <path>         // Output directory for .parquet files
--table <name>           // Table name to extract
--parallelism <n>        // Number of parallel workers (default: 2x CPU cores)
--pool-size <n>          // Connection pool size (default: parallelism * 2)
--user <username>        // Firebird user (default: SYSDBA)
--password <password>    // Firebird password (default: masterkey)
--use-compression        // Enable compression (default: false)
```

### 2. `config.rs` (Configuration)

**Location**: `src/config.rs:1-13`

**Struct**: `ExtractorConfig`
```rust
pub struct ExtractorConfig {
    pub database_path: String,
    pub out_dir: PathBuf,
    pub parallelism: usize,      // Number of parallel workers
    pub pool_size: usize,         // Firebird connection pool size
    pub user: String,
    pub password: String,
    pub use_compression: bool,    // Parquet compression (off for speed)
}
```

### 3. `extractor.rs` (Core Extraction Logic)

**Location**: `src/extractor.rs:1-842`

This is the **heart** of the application. Key components:

#### a. `Extractor` (Main Orchestrator)
- **Location**: `src/extractor.rs:43-558`
- Manages connection pool
- Loads table metadata
- Chooses extraction strategy (parallel PK vs sequential)
- Creates Parquet writer properties

#### b. `ConnectionPool` (Database Connections)
- **Location**: `src/extractor.rs:48-126`
- Thread-safe connection pool using `Arc<Mutex<Vec<SimpleConnection>>>`
- Auto-creates connections when pool is empty
- Returns `PooledConnection` that auto-returns to pool on drop

#### c. Metadata Structs
```rust
TableMetadata       // src/extractor.rs:128-135
ColumnMetadata      // src/extractor.rs:137-142
PrimaryKeyInfo      // src/extractor.rs:144-150
```

#### d. Key Methods

**`extract_table`** (`src/extractor.rs:159-188`)
- Entry point for extraction
- Loads metadata
- Dispatches to parallel or sequential extraction

**`load_metadata`** (`src/extractor.rs:190-213`)
- Queries Firebird system tables
- Detects primary key
- Loads column definitions
- Gets row count

**`detect_pk`** (`src/extractor.rs:215-303`)
- Finds primary key index from `rdb$indices`
- Validates all PK columns are numeric (INTEGER/BIGINT)
- Calculates MIN/MAX values for partitioning
- **Optimization**: Skips MIN/MAX for huge tables with composite keys

**`extract_parallel_pk`** (`src/extractor.rs:342-437`)
- Partitions PK range into N chunks
- Uses Rayon's `par_iter` to extract partitions in parallel
- Each partition writes to temp file
- Merges temp files into final output

**`extract_sequential`** (`src/extractor.rs:439-546`)
- Fallback for tables without numeric PK
- Uses prefetch pipeline (fetcher thread + writer thread)
- Aggressive queue sizes (10 for fetch, 8 for batch)
- No ORDER BY clause

**`extract_partition`** (`src/extractor.rs:564-627`)
- Extracts a single PK partition
- Queries: `WHERE pk >= start AND pk <= end`
- No ORDER BY clause
- Writes to temp parquet file

**`merge_parquet_files`** (`src/extractor.rs:629-677`)
- Merges multiple parquet files into one
- Reads batches and writes to unified output
- Optimized with large buffer (128MB)

#### e. Helper Functions

**`calculate_batch_size`** (`src/extractor.rs:679-697`)
- Adaptive batch sizing based on row count
- 500K-1M rows for large tables
- Reduces by 33% if BLOBs present

**`build_arrow_batch`** (`src/extractor.rs:699-720`)
- Converts Firebird rows to Arrow RecordBatch
- **Parallelized**: Uses Rayon to build columns in parallel

**`build_column_array`** (`src/extractor.rs:722-792`)
- Converts a single column from Firebird to Arrow array
- Handles type conversions (Int64, Float64, Utf8, Binary)

**`fb_to_arrow_type`** (`src/extractor.rs:794-813`)
- Maps Firebird types to Arrow types
- Type mappings:
  - `7, 8, 16` → Int64 (SMALLINT, INTEGER, BIGINT)
  - `10, 27, 23` → Float64 (FLOAT, DOUBLE)
  - `12` → Utf8 (BLOB subtype TEXT) or Binary (BLOB)
  - `14, 37` → Utf8 (CHAR, VARCHAR)

---

## Development Workflows

### Building the Project

```bash
# Debug build
cargo build

# Release build (with optimizations)
cargo build --release

# Check without building
cargo check
```

### Running Locally

```bash
# Basic usage
./target/release/firebird_peregrine_falcon \
  --database "/path/to/database.fdb" \
  --out-dir "/output/directory" \
  --table "TABLE_NAME"

# With custom parallelism
./target/release/firebird_peregrine_falcon \
  --database "path/to/db.fdb" \
  --out-dir "output" \
  --table "MY_TABLE" \
  --parallelism 60 \
  --pool-size 120

# With authentication
./target/release/firebird_peregrine_falcon \
  --database "db.fdb" \
  --out-dir "output" \
  --table "USERS" \
  --user "admin" \
  --password "secret"
```

### Using the PowerShell Helper Script

The `run_agile_log_obrigacao.ps1` script demonstrates production usage:

```powershell
# Edit paths in script first
.\run_agile_log_obrigacao.ps1
```

It handles:
- Auto-building if binary doesn't exist
- Setting parallelism to 40 workers
- Setting pool size to 80 connections
- Success/failure reporting

### GitHub Actions CI

**File**: `.github/workflows/ci.yml`

Automatically runs on:
- Pushes to `main` or `master`
- Pull requests to `main` or `master`

Steps:
1. Checkout code
2. Install Rust stable toolchain
3. `cargo build --release`
4. `cargo check`

---

## Performance Considerations

### Performance Optimizations Implemented

1. **Parallel PK Partitioning**
   - Default: 2x CPU cores
   - Range: 40-60 workers typical
   - Each worker extracts independent partition
   - **File**: `src/extractor.rs:342-437`

2. **Multiple Writer Threads**
   - Each partition writes to temp file in parallel
   - Final merge combines all temp files
   - **File**: `src/extractor.rs:589-627`

3. **Large Batch Sizes**
   - 500K-1M rows per batch
   - Adaptive sizing based on row count
   - **File**: `src/extractor.rs:679-697`

4. **No ORDER BY**
   - Skipped in all queries for speed
   - **File**: `src/extractor.rs:577-580` (parallel), `src/extractor.rs:455` (sequential)

5. **Aggressive Prefetching**
   - Queue size 10 for fetch, 8 for batch
   - **File**: `src/extractor.rs:450-451`

6. **Parallelized Column Building**
   - Uses Rayon to build Arrow columns in parallel
   - **File**: `src/extractor.rs:703-709`

### Performance Tuning Guidance

**For Small Tables (<1M rows)**:
- Default parallelism is fine
- Consider reducing if overhead is too high

**For Medium Tables (1-50M rows)**:
- Default parallelism (2x CPU cores)
- Batch size: 500K-750K

**For Huge Tables (>50M rows)**:
- Increase parallelism to 60-80 workers
- Batch size: 1M rows
- Ensure fast storage (NVMe SSD)

**Memory Considerations**:
- Memory usage ≈ batch_size × parallelism × avg_row_size
- Reduce batch size if OOM occurs

---

## Coding Conventions

### Rust Style

1. **Edition**: Rust 2021
2. **Formatting**: Standard `rustfmt` (run `cargo fmt`)
3. **Linting**: Use `cargo clippy` for warnings
4. **Error Handling**: Use `anyhow::Result` for all fallible functions

### Code Organization Patterns

1. **Modules**:
   - Keep modules focused (config, extractor)
   - Re-export public API via `lib.rs`

2. **Structs**:
   - Use `#[derive(Clone)]` when structs need to cross thread boundaries
   - Prefer owned types (`String`) over references in config structs

3. **Concurrency**:
   - Use `Arc<Mutex<T>>` for shared state
   - Use `crossbeam-channel` for inter-thread communication
   - Use Rayon's `par_iter()` for data parallelism

4. **Database Connections**:
   - Always use connection pool
   - Return connections to pool via `Drop` trait

### Naming Conventions

- **Functions**: `snake_case` (e.g., `extract_table`, `load_metadata`)
- **Structs**: `PascalCase` (e.g., `Extractor`, `TableMetadata`)
- **Constants**: `SCREAMING_SNAKE_CASE` (none currently, but follow convention)
- **Files**: `snake_case.rs` (e.g., `config.rs`, `extractor.rs`)

### Documentation

- Use `///` for public API docs
- Use `//!` for module-level docs (see `src/extractor.rs:1-9`)
- Document performance implications
- Include examples in docstrings when helpful

---

## Dependencies

### Core Dependencies (from `Cargo.toml`)

```toml
[dependencies]
anyhow = "1.0"              # Error handling
rayon = "1.10"              # Data parallelism
arrow = "53"                # Apache Arrow data structures
parquet = "53"              # Apache Parquet file format
num_cpus = "1.0"            # CPU count detection
sha2 = "0.10"               # Hashing (currently unused)
crossbeam-channel = "0.5"   # Thread-safe channels
rsfbclient = "0.26"         # Firebird database client
clap = "4.5"                # CLI argument parsing
memmap2 = "0.9"             # Memory-mapped files (currently unused)
```

### Release Profile Optimizations

```toml
[profile.release]
opt-level = 3          # Maximum optimization
lto = true             # Link-time optimization
codegen-units = 1      # Single codegen unit for better optimization
panic = "abort"        # Abort on panic (smaller binary)
```

### Dependency Notes

- **`arrow` & `parquet`**: Must use same version (currently 53)
- **`rsfbclient`**: Uses `native_client` feature for Firebird native client library
- **`rayon`**: Critical for parallel extraction performance
- **`crossbeam-channel`**: Used for bounded channels (better than `std::sync::mpsc`)

---

## Common Tasks

### Adding a New CLI Argument

1. Add field to `Args` struct in `src/main.rs:8-40`
2. Pass to `ExtractorConfig` in `src/main.rs:57-65`
3. Add field to `ExtractorConfig` in `src/config.rs:4-12`
4. Use in extractor logic in `src/extractor.rs`

### Modifying Batch Size Calculation

**File**: `src/extractor.rs:679-697`

Adjust thresholds in `calculate_batch_size`:
```rust
let base_batch = if row_count < 200_000 {
    250_000
} else if row_count < 10_000_000 {
    500_000  // ← Adjust this
} else if row_count < 50_000_000 {
    750_000  // ← Or this
} else {
    1_000_000
};
```

### Adding Support for New Firebird Data Type

1. **Map Firebird type to Arrow type** in `fb_to_arrow_type` (`src/extractor.rs:794-813`)
2. **Handle conversion** in `build_column_array` (`src/extractor.rs:722-792`)

Example for DATE type:
```rust
// In fb_to_arrow_type
35 => (DataType::Date32, false),  // DATE

// In build_column_array
DataType::Date32 => {
    let mut builder = Date32Builder::with_capacity(row_count);
    for row in rows {
        match row.cols.get(col_index).map(|c| &c.value) {
            Some(rsfbclient::SqlType::Date(d)) => {
                builder.append_value(d.timestamp() as i32 / 86400)
            }
            _ => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}
```

### Adjusting Parallelism Strategy

**Default Calculation**: `src/main.rs:45`
```rust
let parallelism = args.parallelism.unwrap_or_else(|| num_cpus::get() * 2);
```

To change default to 3x CPU cores:
```rust
let parallelism = args.parallelism.unwrap_or_else(|| num_cpus::get() * 3);
```

### Debugging Performance Issues

1. **Add timing logs**:
   ```rust
   let start = Instant::now();
   // ... code ...
   println!("Operation took: {:.2}s", start.elapsed().as_secs_f64());
   ```

2. **Check batch sizes**: Look for batch size in output
3. **Monitor parallelism**: Check "Partitions: N" in output
4. **Profile with `perf`** (Linux):
   ```bash
   cargo build --release
   perf record ./target/release/firebird_peregrine_falcon <args>
   perf report
   ```

---

## Important Notes

### For AI Assistants

1. **Performance is Priority #1**: Any code changes must not regress performance
2. **Preserve Parallelism**: The parallel PK partitioning is critical
3. **No ORDER BY**: Do not add ORDER BY clauses unless absolutely necessary
4. **Test with Large Data**: Performance only matters at scale (10M+ rows)
5. **Cross-Platform**: Avoid platform-specific code (Windows/Linux both supported)

### Firebird System Tables Used

The code queries these Firebird system tables:
- `rdb$indices` - Index definitions (for PK detection)
- `rdb$index_segments` - Index columns (for PK columns)
- `rdb$relation_fields` - Table columns
- `rdb$fields` - Field type definitions

### Known Limitations

1. **Composite PKs**: Only first column used for partitioning
2. **Non-Numeric PKs**: Fall back to sequential extraction
3. **No ORDER BY**: Output rows are unordered
4. **Firebird Native Client Required**: Must have Firebird client library installed

### Firebird Client Library

**Windows**: Usually in `C:\Program Files\Firebird\Firebird_X_X\bin\fbclient.dll`
**Linux**: Install `firebird-dev` or `firebird-devel` package

Set environment variable if needed:
```bash
export LD_LIBRARY_PATH=/usr/lib/firebird/3.0
```

### Testing Checklist

When making changes, test with:
- [ ] Small table (<1K rows)
- [ ] Medium table (1M rows)
- [ ] Large table (10M+ rows)
- [ ] Table with BLOBs
- [ ] Table with no PK
- [ ] Table with composite PK
- [ ] Windows platform
- [ ] Linux platform

---

## Quick Reference

### File Locations

| File                  | Lines | Purpose                              |
|-----------------------|-------|--------------------------------------|
| `src/main.rs`         | 79    | CLI entry point                      |
| `src/lib.rs`          | 7     | Library exports                      |
| `src/config.rs`       | 13    | Configuration struct                 |
| `src/extractor.rs`    | 842   | Core extraction logic                |
| `Cargo.toml`          | 24    | Dependencies & build config          |
| `.github/workflows/ci.yml` | 28 | CI pipeline                     |

### Key Function Reference

| Function                | Location                  | Purpose                          |
|-------------------------|---------------------------|----------------------------------|
| `extract_table`         | `extractor.rs:159-188`    | Main extraction entry point      |
| `load_metadata`         | `extractor.rs:190-213`    | Load table metadata              |
| `detect_pk`             | `extractor.rs:215-303`    | Detect primary key               |
| `extract_parallel_pk`   | `extractor.rs:342-437`    | Parallel PK partitioning         |
| `extract_sequential`    | `extractor.rs:439-546`    | Sequential extraction (no PK)    |
| `extract_partition`     | `extractor.rs:564-627`    | Extract single partition         |
| `merge_parquet_files`   | `extractor.rs:629-677`    | Merge temp parquet files         |
| `calculate_batch_size`  | `extractor.rs:679-697`    | Adaptive batch sizing            |
| `build_arrow_batch`     | `extractor.rs:699-720`    | Convert rows to Arrow batch      |
| `build_column_array`    | `extractor.rs:722-792`    | Build single Arrow column        |
| `fb_to_arrow_type`      | `extractor.rs:794-813`    | Map Firebird to Arrow types      |

---

## Version History

- **v0.1.0** (2025-01): Initial release
  - Parallel PK partitioning
  - 2x CPU core parallelism
  - 500K-1M batch sizes
  - Cross-platform support

---

## License

MIT License - See `LICENSE` file for details.

---

*Last Updated: 2025-01-04*
*Maintained by: Firebird Peregrine Falcon Team*
