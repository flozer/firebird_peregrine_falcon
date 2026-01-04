# Quick Setup Guide for Peregrine Falcon Miramar

## Easy Configuration (3 Methods)

### Method 1: Environment Variables (Recommended for Scripts)

```bash
# Set these in your shell or .env file
export PFM_DATABASE="/path/to/database.fdb"
export PFM_OUTPUT_DIR="/path/to/output"
export PFM_USER="SYSDBA"
export PFM_PASSWORD="masterkey"
export PFM_PARALLELISM=40

# Then run with just the table name
./peregrine_falcon_miramar --table MY_TABLE
```

### Method 2: Configuration File (Recommended for Permanent Settings)

```bash
# 1. Copy the example config
cp config.example.toml config.toml

# 2. Edit with your settings
nano config.toml

# 3. Run with config file
./peregrine_falcon_miramar --config config.toml --table MY_TABLE
```

### Method 3: Command Line Arguments (Quick One-Off Runs)

```bash
./peregrine_falcon_miramar \
  --database "/path/to/database.fdb" \
  --out-dir "/path/to/output" \
  --table "MY_TABLE" \
  --user "SYSDBA" \
  --password "masterkey" \
  --parallelism 40 \
  --pool-size 80
```

## All Available Options

```
USAGE:
    peregrine_falcon_miramar [OPTIONS]

OPTIONS:
    --database <PATH>          Firebird database path (.fdb file)
    --out-dir <PATH>           Output directory for Parquet files
    --table <NAME>             Table name to extract
    --config <PATH>            Load settings from config file
    --user <USERNAME>          Firebird username [default: SYSDBA]
    --password <PASSWORD>      Firebird password [default: masterkey]
    --parallelism <N>          Parallel workers [default: 2x CPU cores]
    --pool-size <N>            Connection pool size [default: parallelism * 2]
    --use-compression          Enable Snappy compression [default: false]
    -h, --help                 Print help information
    -V, --version              Print version information
```

## Quick Start Examples

### Extract a Single Table
```bash
./peregrine_falcon_miramar \
  --database "MYDB.FDB" \
  --out-dir "output" \
  --table "CUSTOMERS"
```

### High-Performance Extraction (Large Table)
```bash
./peregrine_falcon_miramar \
  --database "BIGDB.FDB" \
  --out-dir "output" \
  --table "TRANSACTIONS" \
  --parallelism 60 \
  --pool-size 120
```

### With Custom Credentials
```bash
./peregrine_falcon_miramar \
  --database "SECURE.FDB" \
  --out-dir "output" \
  --table "SENSITIVE_DATA" \
  --user "ADMIN" \
  --password "SecurePass123"
```

### With Compression (Slower but Smaller Files)
```bash
./peregrine_falcon_miramar \
  --database "DATA.FDB" \
  --out-dir "output" \
  --table "LARGE_TABLE" \
  --use-compression
```

## Building from Source

### Prerequisites
- Rust toolchain (https://rustup.rs/)
- Firebird client library

**Windows:**
```powershell
# Firebird client usually in:
# C:\Program Files\Firebird\Firebird_X_X\bin\fbclient.dll
```

**Linux:**
```bash
# Install Firebird dev package
sudo apt-get install firebird-dev  # Debian/Ubuntu
sudo yum install firebird-devel    # RHEL/CentOS

# Or set library path
export LD_LIBRARY_PATH=/usr/lib/firebird/3.0
```

### Build Commands

**Standard Build:**
```bash
cargo build --release
```

**With Profile-Guided Optimization (PGO) - 5-15% Faster:**
```bash
# Step 1: Build with instrumentation
RUSTFLAGS="-Cprofile-generate=/tmp/pgo-data" cargo build --release

# Step 2: Run with typical workload
./target/release/peregrine_falcon_miramar \
  --database "sample.fdb" \
  --out-dir "output" \
  --table "SAMPLE_TABLE"

# Step 3: Rebuild with optimizations
RUSTFLAGS="-Cprofile-use=/tmp/pgo-data/merged.profdata" cargo build --release
```

**Binary Location:**
```
./target/release/peregrine_falcon_miramar
```

## Performance Tips

### For Maximum Speed
- Use NVMe SSD for output directory
- Set parallelism to 40-60 for large tables (>10M rows)
- Disable compression (default)
- Ensure adequate RAM (50 MB per worker)

### For Large Tables (>50M rows)
```bash
--parallelism 60 --pool-size 120
```

### For Small Tables (<1M rows)
```bash
--parallelism 10 --pool-size 20
```

### Memory Constrained
```bash
--parallelism 10  # Fewer workers = less memory
```

## Expected Performance

| Table Size | Expected Speed | Memory Usage |
|------------|----------------|--------------|
| < 1M rows  | 300K-500K rows/s | 100-200 MB |
| 1M-50M rows | 600K-900K rows/s | 200-400 MB |
| > 50M rows | 1M-1.2M rows/s | 300-500 MB |

## Troubleshooting

### Error: "cannot find -lfbclient"
**Solution:** Install Firebird client library
```bash
# Linux
sudo apt-get install firebird-dev

# Windows
# Download and install Firebird from firebirdsql.org
```

### Error: "Out of Memory"
**Solution:** Reduce parallelism
```bash
--parallelism 10  # Instead of default 40+
```

### Slow Performance
**Solutions:**
1. Increase parallelism (if you have CPU cores available)
2. Use faster storage (NVMe SSD)
3. Increase pool size
4. Check network latency (if database is remote)

## Support

For issues or questions, see:
- **README.md** - Complete documentation
- **CLAUDE.md** - Detailed architecture guide
- **PERFORMANCE_OPTIMIZATION_REPORT.md** - Expert analysis

---

**Peregrine Falcon Miramar - World's Fastest Firebird Extractor** 🚀
