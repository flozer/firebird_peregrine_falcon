use clap::Parser;
use peregrine_falcon_miramar::Extractor;
use peregrine_falcon_miramar::ExtractorConfig;

#[derive(Parser)]
#[command(name = "peregrine_falcon_miramar")]
#[command(about = "World's fastest Firebird-to-Parquet extractor with 23 expert optimizations (v1.0 Miramar)")]
struct Args {
    /// Firebird database path
    #[arg(long)]
    database: String,

    /// Output directory for Parquet files
    #[arg(long)]
    out_dir: String,

    /// Table name to extract
    #[arg(long)]
    table: String,

    /// Number of parallel workers (default: 2x CPU cores)
    #[arg(long)]
    parallelism: Option<usize>,

    /// Connection pool size (default: parallelism * 2)
    #[arg(long)]
    pool_size: Option<usize>,

    /// Firebird username
    #[arg(long, default_value = "SYSDBA")]
    user: String,

    /// Firebird password
    #[arg(long, default_value = "masterkey")]
    password: String,

    /// Use compression (default: false for speed)
    #[arg(long, default_value_t = false)]
    use_compression: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let parallelism = args.parallelism.unwrap_or_else(|| num_cpus::get() * 2);
    let pool_size = args.pool_size.unwrap_or_else(|| parallelism * 2);

    println!("=== PEREGRINE FALCON MIRAMAR v1.0 - WORLD'S FASTEST EXTRACTOR ===");
    println!("Database: {}", args.database);
    println!("Output: {}", args.out_dir);
    println!("Table: {}", args.table);
    println!("Parallelism: {} workers", parallelism);
    println!("Pool size: {} connections", pool_size);
    println!("Optimizations: 23 expert-level strategies (Firebird + Rust + Arrow)");
    println!("  • Lock-free connection pool • Streaming extraction • Parallel merge");
    println!("  • Batched metadata • Zero-copy arrays • Adaptive sizing");
    println!();

    let config = ExtractorConfig {
        database_path: args.database,
        out_dir: std::path::PathBuf::from(&args.out_dir),
        parallelism,
        pool_size,
        user: args.user,
        password: args.password,
        use_compression: args.use_compression,
    };

    let extractor = Extractor::new(config)?;
    let stats = extractor.extract_table(&args.table)?;

    println!();
    println!("=== EXTRACTION COMPLETE ===");
    println!("Rows: {}", stats.rows_extracted);
    println!("Duration: {:.1}s", stats.duration_secs);
    println!("File size: {:.2} MB", stats.file_size_mb);
    println!("Speed: {:.0} rows/s", stats.rows_extracted as f64 / stats.duration_secs);

    Ok(())
}

