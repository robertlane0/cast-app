#![forbid(unsafe_code)]

//! cast-app entrypoint: pre-logging startup banner, tracing initialization,
//! and the eframe GUI launch. The backend task wiring lives in Phase 10
//! (`runtime.rs`); until then the dashboard runs against inert channels.

fn main() -> eframe::Result {
    println!("cast-app v{}", env!("CARGO_PKG_VERSION"));

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    tracing::info!("cast-app v{} starting", env!("CARGO_PKG_VERSION"));

    cast_app::cast::tls::install_crypto_provider();

    let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel();
    let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();
    // Phase 10: hand these to the runtime supervisor's tasks. Until then the
    // channel endpoints stay alive so the GUI dispatches and drains cleanly.
    let _command_rx = command_rx;
    let _event_tx = event_tx;

    let dashboard = cast_app::app::CastDashboard::new(command_tx, event_rx);
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1100.0, 700.0])
            .with_min_inner_size([800.0, 480.0]),
        ..Default::default()
    };

    eframe::run_native(
        "cast-app",
        options,
        Box::new(move |_cc| Ok(Box::new(dashboard))),
    )
}
