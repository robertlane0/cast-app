#![forbid(unsafe_code)]

//! cast-app entrypoint: pre-logging startup banner, tracing initialization,
//! tokio runtime + backend supervisor startup, and the eframe GUI launch.
//! After the window closes the backend performs the coordinated shutdown
//! (HTTP listener → Cast session → mDNS → capture/ffmpeg; `06-concurrency.md`
//! §5).

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
