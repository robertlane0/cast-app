#![forbid(unsafe_code)]

//! cast-app entrypoint: pre-logging startup banner, tracing initialization
//! (console + file layers, `CAST_APP_LOG` env filter), tokio runtime +
//! backend supervisor startup, and the eframe GUI launch.
//! After the window closes the backend performs the coordinated shutdown
//! (HTTP listener → Cast session → mDNS → capture/ffmpeg; `06-concurrency.md`
//! §5).

use std::io::IsTerminal;
use std::path::PathBuf;

use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

fn main() -> eframe::Result {
    println!("cast-app v{}", env!("CARGO_PKG_VERSION"));

    let (log_dir, _log_guard) = init_logging();

    tracing::info!(
        log_dir = %log_dir.display(),
        "cast-app v{} starting",
        env!("CARGO_PKG_VERSION")
    );

    cast_app::cast::tls::install_crypto_provider();

    let (backend, command_tx, event_rx) = cast_app::runtime::Backend::start();

    let dashboard = cast_app::app::CastDashboard::new(command_tx, event_rx);
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1100.0, 700.0])
            .with_min_inner_size([800.0, 480.0]),
        ..Default::default()
    };

    let result = eframe::run_native(
        "cast-app",
        options,
        Box::new(move |_cc| Ok(Box::new(dashboard))),
    );

    // The GUI (and its command sender) is gone: stop the backend and wait
    // for every task and thread to wind down.
    backend.shutdown();
    tracing::info!("cast-app exited cleanly");

    result
}

/// Init-only global subscriber: a console layer (ANSI only when the stdout is
/// a terminal, capped at INFO so debug noise stays out of the console) and a
/// non-blocking file layer writing `cast-app.log` in the platform log
/// directory (AGENTS.md §12, Phase 12).
fn init_logging() -> (PathBuf, tracing_appender::non_blocking::WorkerGuard) {
    let log_dir = log_file_dir();
    let filter = env_filter();
    let file_appender = tracing_appender::rolling::never(&log_dir, "cast-app.log");
    let (file_writer, file_guard) = tracing_appender::non_blocking(file_appender);
    let stdout_writer = std::io::stdout.with_max_level(tracing::Level::INFO);

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(std::io::stdout().is_terminal())
                .with_writer(stdout_writer)
                .with_filter(filter.clone()),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(file_writer)
                .with_filter(filter),
        )
        .init();
    (log_dir, file_guard)
}

/// The `CAST_APP_LOG` filter, falling back to `RUST_LOG` and finally `info`
/// (AGENTS.md §12; spec `01-architecture.md` §9).
fn env_filter() -> EnvFilter {
    for name in ["CAST_APP_LOG", "RUST_LOG"] {
        if let Ok(value) = std::env::var(name) {
            if let Ok(filter) = EnvFilter::try_new(&value) {
                return filter;
            }
        }
    }
    EnvFilter::new("info")
}

/// Platform log directory (`std::env::temp_dir()` fallback per AGENTS.md
/// §12). The directory is created here (init path) so the rolling appender
/// never sees a missing parent.
fn log_file_dir() -> PathBuf {
    let dir = platform_log_dir();
    match std::fs::create_dir_all(&dir) {
        Ok(()) => dir,
        Err(error) => {
            println!(
                "warning: cannot create log dir {} ({error}); using the temp dir",
                dir.display()
            );
            std::env::temp_dir().join("cast-app").join("logs")
        }
    }
}

/// Per-platform preference order, mirroring OS conventions:
/// Windows `%LOCALAPPDATA%\cast-app\logs`; macOS `~/Library/Logs/cast-app`;
/// Linux `$XDG_STATE_HOME/cast-app/logs` or `~/.local/state/cast-app/logs`;
/// and the temp dir as the last resort.
fn platform_log_dir() -> PathBuf {
    if let Some(base) = std::env::var_os("LOCALAPPDATA") {
        return PathBuf::from(base).join("cast-app").join("logs");
    }
    if let Some(home) = home_dir() {
        if home.join("Library").is_dir() {
            return home.join("Library").join("Logs").join("cast-app");
        }
    }
    if let Some(base) = std::env::var_os("XDG_STATE_HOME") {
        return PathBuf::from(base).join("cast-app").join("logs");
    }
    if let Some(home) = home_dir() {
        return home
            .join(".local")
            .join("state")
            .join("cast-app")
            .join("logs");
    }
    std::env::temp_dir().join("cast-app").join("logs")
}

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}
