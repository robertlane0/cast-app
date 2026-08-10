#![forbid(unsafe_code)]

//! cast-app entrypoint: pre-logging startup banner, tracing initialization,
//! and (from Phase 10) the tokio runtime plus eframe launch.

use std::process::ExitCode;

fn main() -> ExitCode {
    println!("cast-app v{}", env!("CARGO_PKG_VERSION"));

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    tracing::info!("cast-app v{} starting", env!("CARGO_PKG_VERSION"));

    // Phase 10: build the tokio runtime and launch the eframe GUI here.
    ExitCode::SUCCESS
}
