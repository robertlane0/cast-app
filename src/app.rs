// SPDX-License-Identifier: MIT OR Apache-2.0
//! `CastDashboard` eframe application: UI rendering, command dispatch, and
//! per-frame `try_recv` draining of backend events (`02-gui.md` §3–§4).
//!
//! The GUI thread never blocks: no `await`, no blocking I/O, no `recv()`;
//! `event_rx` is drained with `try_recv` at the start of every frame and the
//! `rfd::AsyncFileDialog` future is polled with a noop waker (`06-concurrency.md`
//! §2, GUI constraint).

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::Poll;
use std::time::{Duration, Instant};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::screen::ffmpeg_discover;
use crate::state::{AppCommand, BackendEvent, CastDevice, SourceTab};

/// Left-panel width (`02-gui.md` §3.0).
pub const RECEIVER_PANEL_WIDTH: f32 = 250.0;
/// Bottom-bar height (`02-gui.md` §3.0).
pub const CONTROLS_BAR_HEIGHT: f32 = 48.0;

/// Left-panel discovery rendering state (`02-gui.md` §3.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscoveryState {
    /// Discovery started, no snapshot received yet.
    Scanning,
    /// First snapshot arrived and was empty.
    NoReceivers,
    /// Fatal discovery/connection error with a retry action.
    Error(String),
    /// At least one receiver is listed.
    Ready,
}

/// Connection state rendered in the status strip (`02-gui.md` §3.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionState {
    /// No connect/disconnect event seen yet.
    Scanning,
    /// `BackendEvent::ReceiverConnected`.
    Connected(CastDevice),
    /// `BackendEvent::ReceiverDisconnected` or `ConnectionError`.
    Disconnected,
}

/// Playback state rendered in the status strip (`02-gui.md` §3.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaybackState {
    /// No `MediaStatus` event received yet.
    Idle,
    Playing,
    Paused,
    Buffering,
}

/// Volume-slider throttle (`02-gui.md` §3.3): at most one `SET_VOLUME`
/// message per 100 ms, with a trailing edge so the last position always
/// wins. Tested directly with injected `Instant`s.
#[derive(Debug, Default)]
pub struct VolumeThrottle {
    last_send: Option<Instant>,
    pending: Option<f32>,
}

impl VolumeThrottle {
    /// Minimum interval between two dispatched volume messages.
    pub const MIN_INTERVAL: Duration = Duration::from_millis(100);

    pub fn new() -> Self {
        Self::default()
    }

    /// Record a slider change; the latest value replaces any pending one.
    pub fn note_change(&mut self, level: f32) {
        self.pending = Some(level);
    }

    /// Return the value that may be dispatched now (`None` when throttled or
    /// idle). Sends immediately on the leading edge, then at most once per
    /// `MIN_INTERVAL` with the trailing value.
    pub fn poll(&mut self, now: Instant) -> Option<f32> {
        let pending = self.pending?;
        let due = self
            .last_send
            .is_none_or(|last| now.saturating_duration_since(last) >= Self::MIN_INTERVAL);
        if due {
            self.last_send = Some(now);
            self.pending = None;
            Some(pending)
        } else {
            None
        }
    }

    /// Whether a value is queued for a future frame.
    pub fn is_pending(&self) -> bool {
        self.pending.is_some()
    }
}

/// The in-flight `rfd` native file picker, polled each frame (`02-gui.md`
/// §3.2). A noop waker is fine: we re-poll on every repaint.
type FilePickerFuture = Pin<Box<dyn Future<Output = Option<rfd::FileHandle>> + Send>>;

/// Dashboard state and rendering (`02-gui.md` §4.2). The GUI owns a mirror of
/// backend state; the authoritative copies live in the backend tasks.
pub struct CastDashboard {
    available_receivers: Vec<CastDevice>,
    selected_receiver: Option<CastDevice>,
    source_tab: SourceTab,
    displays: Vec<String>,
    command_tx: UnboundedSender<AppCommand>,
    event_rx: UnboundedReceiver<BackendEvent>,

    discovery: DiscoveryState,
    connection: ConnectionState,
    playback: PlaybackState,
    volume: u8,
    muted: bool,
    volume_throttle: VolumeThrottle,
    volume_dragging: bool,
    error_banner: Option<String>,
    has_active_source: bool,
    ffmpeg_available: bool,
    selected_display: Option<String>,
    url_input: String,
    settings_open: bool,
    proxy_port: u16,
    port_draft: u16,
    file_picker: Option<FilePickerFuture>,
}

impl CastDashboard {
    /// Repaint cadence used to drain `event_rx` while the window is idle.
    const REPAINT_INTERVAL: Duration = Duration::from_millis(200);
    /// Faster cadence while the discovery spinner is animating.
    const REPAINT_SCANNING_INTERVAL: Duration = Duration::from_millis(100);

    /// Create the dashboard. `command_tx` is the GUI→backend command channel
    /// and `event_rx` the backend→GUI event channel (`02-gui.md` §2).
    pub fn new(
        command_tx: UnboundedSender<AppCommand>,
        event_rx: UnboundedReceiver<BackendEvent>,
    ) -> Self {
        Self {
            available_receivers: Vec::new(),
            selected_receiver: None,
            source_tab: SourceTab::Display,
            displays: Vec::new(),
            command_tx,
            event_rx,
            discovery: DiscoveryState::Scanning,
            connection: ConnectionState::Scanning,
            playback: PlaybackState::Idle,
            volume: 0,
            muted: false,
            volume_throttle: VolumeThrottle::new(),
            volume_dragging: false,
            error_banner: None,
            has_active_source: false,
            ffmpeg_available: ffmpeg_discover::ffmpeg_available(),
            selected_display: None,
            url_input: String::new(),
            settings_open: false,
            proxy_port: 8080,
            port_draft: 8080,
            file_picker: None,
        }
    }

    // -----------------------------------------------------------------------
    // Event handling (`02-gui.md` §4.3)
    // -----------------------------------------------------------------------

    /// Drain `event_rx` with `try_recv` to exhaustion, applying every pending
    /// event to the dashboard state. Returns the number of events applied.
    /// Never blocks; call once per frame before rendering.
    pub fn handle_events(&mut self) -> usize {
        let mut count = 0;
        while let Ok(event) = self.event_rx.try_recv() {
            count += 1;
            self.apply_event(event);
        }
        count
    }

    fn apply_event(&mut self, event: BackendEvent) {
        match event {
            BackendEvent::ReceiversUpdated(devices) => {
                self.available_receivers = devices;
                self.discovery = if self.available_receivers.is_empty() {
                    DiscoveryState::NoReceivers
                } else {
                    DiscoveryState::Ready
                };
                self.dismiss_error();
            }
            BackendEvent::DisplaysUpdated(displays) => {
                self.displays = displays;
                self.dismiss_error();
            }
            BackendEvent::ReceiverConnected(device) => {
                self.selected_receiver = Some(device.clone());
                self.connection = ConnectionState::Connected(device);
                self.dismiss_error();
            }
            BackendEvent::ReceiverDisconnected(device) => {
                if self.selected_receiver.as_ref() == Some(&device) {
                    self.selected_receiver = None;
                }
                self.connection = ConnectionState::Disconnected;
                self.playback = PlaybackState::Idle;
                self.dismiss_error();
            }
            BackendEvent::ConnectionError(message) => {
                self.connection = ConnectionState::Disconnected;
                self.playback = PlaybackState::Idle;
                self.show_error(message.clone());
                if self.available_receivers.is_empty() {
                    self.discovery = DiscoveryState::Error(message);
                }
            }
            BackendEvent::StreamError(message) => {
                self.show_error(message);
            }
            BackendEvent::MediaStatus { playing, buffering } => {
                self.playback = if buffering {
                    PlaybackState::Buffering
                } else if playing {
                    PlaybackState::Playing
                } else {
                    PlaybackState::Paused
                };
                self.dismiss_error();
            }
            BackendEvent::Volume { level, muted } => {
                if !self.volume_dragging {
                    self.volume = (level.clamp(0.0, 1.0) * 100.0).round() as u8;
                }
                self.muted = muted;
                self.dismiss_error();
            }
        }
    }

    fn show_error(&mut self, message: String) {
        self.error_banner = Some(message);
    }

    /// Manually dismiss the transient error banner (`02-gui.md` §3.4).
    pub fn dismiss_error(&mut self) {
        self.error_banner = None;
    }

    // -----------------------------------------------------------------------
    // Command dispatch (`02-gui.md` §4.1)
    // -----------------------------------------------------------------------

    fn dispatch(&mut self, command: AppCommand) {
        if let Err(error) = self.command_tx.send(command) {
            tracing::warn!("command channel closed, command dropped: {error:?}");
        }
    }

    /// Select a receiver and dispatch `SelectReceiver` (`02-gui.md` §3.1).
    pub fn select_receiver(&mut self, device: &CastDevice) {
        self.selected_receiver = Some(device.clone());
        self.dispatch(AppCommand::SelectReceiver(device.clone()));
    }

    /// Re-arm discovery after an Error state and request an immediate mDNS
    /// re-query (`02-gui.md` §3.1).
    pub fn retry_discovery(&mut self) {
        self.discovery = DiscoveryState::Scanning;
        self.dispatch(AppCommand::Rescan);
    }

    /// Switch the source tab and dispatch `SelectSource` (`02-gui.md` §3.2).
    pub fn select_tab(&mut self, tab: SourceTab) {
        self.source_tab = tab;
        self.dispatch(AppCommand::SelectSource(tab));
    }

    /// Select a display and dispatch `SelectDisplay` (`02-gui.md` §3.2).
    pub fn select_display(&mut self, name: &str) {
        self.selected_display = Some(name.to_string());
        self.has_active_source = true;
        self.dispatch(AppCommand::SelectDisplay(name.to_string()));
    }

    /// Dispatch `SelectFile` for a picked media file (`02-gui.md` §3.2).
    pub fn select_file(&mut self, path: PathBuf) {
        self.has_active_source = true;
        self.dispatch(AppCommand::SelectFile(path));
    }

    /// Dispatch `SelectUrl` for a validated remote URL (`02-gui.md` §3.2).
    pub fn select_url(&mut self, url: &str) {
        self.has_active_source = true;
        self.dispatch(AppCommand::SelectUrl(url.trim().to_string()));
    }

    /// Dispatch `Play` (`02-gui.md` §3.3).
    pub fn play(&mut self) {
        self.dispatch(AppCommand::Play);
    }

    /// Dispatch `Pause` (`02-gui.md` §3.3).
    pub fn pause(&mut self) {
        self.dispatch(AppCommand::Pause);
    }

    /// Dispatch `Stop` (`02-gui.md` §3.3).
    pub fn stop(&mut self) {
        self.dispatch(AppCommand::Stop);
    }

    /// Dispatch `Mute` (`02-gui.md` §3.3).
    pub fn toggle_mute(&mut self, muted: bool) {
        self.muted = muted;
        self.dispatch(AppCommand::Mute(muted));
    }

    /// Save the proxy port and dispatch `SetProxyPort` (`02-gui.md` §3.5).
    pub fn save_proxy_port(&mut self, port: u16) {
        self.proxy_port = port;
        self.port_draft = port;
        self.dispatch(AppCommand::SetProxyPort(port));
    }

    // -----------------------------------------------------------------------
    // Volume (`02-gui.md` §3.3)
    // -----------------------------------------------------------------------

    /// Record a slider position change (`0..=100`); the throttled dispatch
    /// happens on a later `poll_and_dispatch_volume`.
    pub fn set_volume(&mut self, level: u8) {
        self.volume_throttle.note_change(level as f32 / 100.0);
    }

    /// Poll the throttle and dispatch the queued `SetVolume` when due
    /// (`02-gui.md` §3.3). Returns true when a command was dispatched.
    pub fn poll_and_dispatch_volume(&mut self, now: Instant) -> bool {
        let Some(level) = self.volume_throttle.poll(now) else {
            return false;
        };
        self.dispatch(AppCommand::SetVolume(level));
        true
    }

    /// Whether a throttled volume value is still queued.
    pub fn volume_pending(&self) -> bool {
        self.volume_throttle.is_pending()
    }

    /// Set while the volume slider is being dragged; `BackendEvent::Volume`
    /// corrections are skipped during a drag so the slider does not jump.
    pub fn set_volume_dragging(&mut self, dragging: bool) {
        self.volume_dragging = dragging;
    }

    // -----------------------------------------------------------------------
    // Enablement rules (`02-gui.md` §3.3)
    // -----------------------------------------------------------------------

    /// Play/Pause additionally require an active source.
    pub fn can_play(&self) -> bool {
        self.selected_receiver.is_some() && self.has_active_source
    }

    pub fn can_pause(&self) -> bool {
        self.can_play()
    }

    /// Stop only requires a selected receiver.
    pub fn can_stop(&self) -> bool {
        self.selected_receiver.is_some()
    }

    /// Display dropdown is enabled when monitors exist and `ffmpeg` is on
    /// `PATH` (`02-gui.md` §3.2).
    pub fn display_dropdown_enabled(&self) -> bool {
        self.ffmpeg_available && !self.displays.is_empty()
    }

    // -----------------------------------------------------------------------
    // URL validation (`02-gui.md` §3.2)
    // -----------------------------------------------------------------------

    /// A URL is valid when it parses as absolute `http://` or `https://`
    /// with a host.
    pub fn validate_url(input: &str) -> bool {
        let Ok(parsed) = url::Url::parse(input.trim()) else {
            return false;
        };
        matches!(parsed.scheme(), "http" | "https") && parsed.host_str().is_some()
    }

    // -----------------------------------------------------------------------
    // File picker (`02-gui.md` §3.2)
    // -----------------------------------------------------------------------

    /// Open the native media file picker (`rfd::AsyncFileDialog`); the
    /// returned future is polled each frame, never awaited on the GUI thread.
    pub fn start_file_picker(&mut self) {
        if self.file_picker.is_some() {
            return;
        }
        let dialog = rfd::AsyncFileDialog::new()
            .set_title("Select a media file")
            .add_filter("Video", &["mp4", "mkv", "mov", "webm"])
            .add_filter("Audio", &["mp3", "aac", "m4a", "flac", "wav"])
            .add_filter(
                "All media",
                &[
                    "mp4", "mkv", "mov", "webm", "mp3", "aac", "m4a", "flac", "wav",
                ],
            )
            .pick_file();
        self.file_picker = Some(Box::pin(dialog));
    }

    /// Poll the in-flight picker with a noop waker; dispatch `SelectFile` when
    /// the dialog completes. A noop waker is fine: the picker is re-polled on
    /// every frame until the dialog thread delivers its result.
    fn poll_file_picker(&mut self, ctx: &egui::Context) {
        let Some(future) = &mut self.file_picker else {
            return;
        };
        let waker = std::task::Waker::noop();
        let mut task_ctx = std::task::Context::from_waker(waker);
        let result = Pin::new(&mut *future).poll(&mut task_ctx);
        match result {
            Poll::Ready(Some(handle)) => {
                let path = handle.path().to_path_buf();
                self.file_picker = None;
                self.select_file(path);
            }
            Poll::Ready(None) => {
                self.file_picker = None;
            }
            Poll::Pending => {
                ctx.request_repaint_after(Self::REPAINT_INTERVAL);
            }
        }
    }

    /// Whether the native picker is currently open.
    pub fn file_picker_open(&self) -> bool {
        self.file_picker.is_some()
    }

    // -----------------------------------------------------------------------
    // Read-only accessors (GUI state mirror, `02-gui.md` §4.2)
    // -----------------------------------------------------------------------

    pub fn receivers(&self) -> &[CastDevice] {
        &self.available_receivers
    }

    pub fn selected_receiver(&self) -> Option<&CastDevice> {
        self.selected_receiver.as_ref()
    }

    pub fn source_tab(&self) -> SourceTab {
        self.source_tab
    }

    pub fn displays(&self) -> &[String] {
        &self.displays
    }

    pub fn discovery(&self) -> &DiscoveryState {
        &self.discovery
    }

    pub fn connection(&self) -> &ConnectionState {
        &self.connection
    }

    pub fn playback(&self) -> PlaybackState {
        self.playback
    }

    pub fn volume(&self) -> u8 {
        self.volume
    }

    pub fn muted(&self) -> bool {
        self.muted
    }

    pub fn error_banner(&self) -> Option<&str> {
        self.error_banner.as_deref()
    }

    pub fn proxy_port(&self) -> u16 {
        self.proxy_port
    }

    pub fn has_active_source(&self) -> bool {
        self.has_active_source
    }

    pub fn ffmpeg_available(&self) -> bool {
        self.ffmpeg_available
    }

    pub fn selected_display(&self) -> Option<&str> {
        self.selected_display.as_deref()
    }

    // -----------------------------------------------------------------------
    // Rendering
    // -----------------------------------------------------------------------

    fn top_bar(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.heading("cast-app");
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui.button("Settings").clicked() {
                    self.settings_open = true;
                }
            });
        });
    }

    fn receivers_panel(&mut self, ui: &mut egui::Ui) {
        ui.heading("Receivers");
        ui.add_space(4.0);
        match &self.discovery {
            DiscoveryState::Scanning => {
                ui.horizontal(|ui| {
                    ui.add(egui::Spinner::new());
                    ui.label("Scanning…");
                });
            }
            DiscoveryState::NoReceivers => {
                ui.label("No receivers found");
            }
            DiscoveryState::Error(message) => {
                ui.colored_label(egui::Color32::RED, format!("Discovery error: {message}"));
                if ui.button("Retry").clicked() {
                    self.retry_discovery();
                }
            }
            DiscoveryState::Ready => {
                let mut clicked: Option<CastDevice> = None;
                egui::ScrollArea::vertical()
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        for device in &self.available_receivers {
                            let selected = self.selected_receiver.as_ref() == Some(device);
                            let label = self.device_row_text(ui, device);
                            if ui.selectable_label(selected, label).clicked() {
                                clicked = Some(device.clone());
                            }
                        }
                    });
                if let Some(device) = clicked {
                    self.select_receiver(&device);
                }
            }
        }
    }

    /// Two-line receiver row: friendly name (14 px) over IP:port (10 px,
    /// dimmed), per `02-gui.md` §3.1.
    fn device_row_text(&self, ui: &egui::Ui, device: &CastDevice) -> egui::WidgetText {
        let mut job = egui::text::LayoutJob::default();
        job.append(
            &device.name,
            0.0,
            egui::TextFormat::simple(
                egui::FontId::proportional(14.0),
                ui.visuals().strong_text_color(),
            ),
        );
        job.append(
            "\n",
            0.0,
            egui::TextFormat::simple(
                egui::FontId::proportional(10.0),
                ui.visuals().weak_text_color(),
            ),
        );
        job.append(
            &device.addr.to_string(),
            0.0,
            egui::TextFormat::simple(
                egui::FontId::proportional(10.0),
                ui.visuals().weak_text_color(),
            ),
        );
        egui::WidgetText::from(job)
    }

    fn source_panel(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            for (tab, label) in [
                (SourceTab::Display, "Display"),
                (SourceTab::LocalFile, "Local File"),
                (SourceTab::WebUrl, "Web URL"),
            ] {
                let selected = self.source_tab == tab;
                if ui.selectable_label(selected, label).clicked() {
                    self.select_tab(tab);
                }
            }
        });
        ui.separator();
        match self.source_tab {
            SourceTab::Display => self.display_tab(ui),
            SourceTab::LocalFile => self.local_file_tab(ui),
            SourceTab::WebUrl => self.web_url_tab(ui),
        }
    }

    fn display_tab(&mut self, ui: &mut egui::Ui) {
        if !self.ffmpeg_available {
            ui.colored_label(
                egui::Color32::RED,
                "ffmpeg not found on PATH — Display capture is unavailable.",
            );
        } else if self.displays.is_empty() {
            ui.colored_label(
                egui::Color32::from_rgb(255, 176, 0),
                "No monitors detected.",
            );
        }
        let enabled = self.display_dropdown_enabled();
        let current = self.selected_display.clone().unwrap_or_default();
        let mut chosen: Option<String> = None;
        ui.add_enabled_ui(enabled, |ui| {
            egui::ComboBox::from_id_salt("monitor_select")
                .selected_text(current)
                .show_ui(ui, |ui| {
                    for name in &self.displays {
                        ui.selectable_value(&mut chosen, Some(name.clone()), name);
                    }
                });
        });
        if let Some(name) = chosen {
            if self.selected_display.as_deref() != Some(name.as_str()) {
                self.select_display(&name);
            }
        }
    }

    fn local_file_tab(&mut self, ui: &mut egui::Ui) {
        if ui.add(egui::Button::new("Browse…")).clicked() {
            self.start_file_picker();
        }
        if self.file_picker.is_some() {
            ui.label("Choose a media file…");
        }
    }

    fn web_url_tab(&mut self, ui: &mut egui::Ui) {
        ui.label("Remote media URL (absolute http:// or https:// with a host)");
        let valid = Self::validate_url(&self.url_input);
        ui.horizontal(|ui| {
            ui.add(
                egui::TextEdit::singleline(&mut self.url_input)
                    .hint_text("https://example.com/media/video.mp4")
                    .desired_width(480.0),
            );
            if ui.add_enabled(valid, egui::Button::new("Apply")).clicked() {
                let url = self.url_input.trim().to_string();
                self.select_url(&url);
            }
        });
        if !valid {
            ui.colored_label(
                egui::Color32::from_rgb(255, 176, 0),
                "Enter an absolute http:// or https:// URL with a host.",
            );
        }
    }

    fn controls_bar(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            let can_transport = self.selected_receiver.is_some();
            let can_play_pause = self.can_play();
            if ui
                .add_enabled(can_play_pause, egui::Button::new("Play"))
                .clicked()
            {
                self.play();
            }
            if ui
                .add_enabled(can_play_pause, egui::Button::new("Pause"))
                .clicked()
            {
                self.pause();
            }
            if ui
                .add_enabled(can_transport, egui::Button::new("Stop"))
                .clicked()
            {
                self.stop();
            }
            ui.separator();
            let slider = ui.add(egui::Slider::new(&mut self.volume, 0..=100).text("Volume"));
            if slider.dragged() {
                self.set_volume_dragging(true);
                self.set_volume(self.volume);
            } else if slider.changed() {
                // Track click (no drag): queue the change too.
                self.set_volume(self.volume);
            }
            if slider.drag_stopped() {
                self.set_volume_dragging(false);
            }
            if ui.checkbox(&mut self.muted, "Mute").changed() {
                self.toggle_mute(self.muted);
            }
        });
        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            self.status_strip(ui);
        });
    }

    fn status_strip(&mut self, ui: &mut egui::Ui) {
        let (color, text) = match &self.connection {
            ConnectionState::Scanning => {
                (egui::Color32::from_rgb(255, 176, 0), "Scanning".to_string())
            }
            ConnectionState::Connected(device) => (
                egui::Color32::from_rgb(0, 180, 0),
                format!("Connected {}", device.name),
            ),
            ConnectionState::Disconnected => (egui::Color32::RED, "Disconnected".to_string()),
        };
        ui.colored_label(color, "●");
        ui.label(text);
        ui.separator();
        let playback = match self.playback {
            PlaybackState::Idle => "Idle",
            PlaybackState::Playing => "Playing",
            PlaybackState::Paused => "Paused",
            PlaybackState::Buffering => "Buffering",
        };
        ui.label(playback);
    }

    fn error_banner_bar(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.colored_label(egui::Color32::RED, "Error:");
            if let Some(message) = self.error_banner.as_deref() {
                ui.label(message);
            }
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui.button("Dismiss").clicked() {
                    self.dismiss_error();
                }
            });
        });
    }

    fn settings_window(&mut self, ui: &mut egui::Ui) {
        ui.set_min_width(360.0);
        ui.heading("Settings");
        ui.add_space(8.0);
        ui.label("Media server (proxy) port: the backend rebinds the HTTP listener and the advertised URL changes.");
        ui.add(
            egui::DragValue::new(&mut self.port_draft)
                .range(1024..=65535)
                .speed(1.0),
        );
        ui.label(format!("Current port: {}", self.proxy_port));
        ui.add_space(8.0);
        ui.horizontal(|ui| {
            let valid = (1024..=65535).contains(&self.port_draft);
            if ui.add_enabled(valid, egui::Button::new("Save")).clicked() {
                self.save_proxy_port(self.port_draft);
                self.settings_open = false;
            }
            if ui.button("Cancel").clicked() {
                self.settings_open = false;
            }
        });
    }
}

impl eframe::App for CastDashboard {
    /// Per frame: drain events, poll the volume throttle and the file picker,
    /// then render. No blocking calls on this thread (`02-gui.md` §5).
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let handled = self.handle_events();
        if handled > 0 {
            ui.ctx().request_repaint();
        }

        if self.poll_and_dispatch_volume(Instant::now()) {
            ui.ctx().request_repaint();
        }

        if self.error_banner.is_some() {
            egui::Panel::bottom("error_banner")
                .exact_size(28.0)
                .show(ui, |ui| self.error_banner_bar(ui));
        }
        egui::Panel::bottom("controls")
            .exact_size(CONTROLS_BAR_HEIGHT)
            .show(ui, |ui| self.controls_bar(ui));
        egui::Panel::top("title_bar")
            .exact_size(28.0)
            .show(ui, |ui| self.top_bar(ui));
        egui::Panel::left("receivers")
            .exact_size(RECEIVER_PANEL_WIDTH)
            .show(ui, |ui| self.receivers_panel(ui));
        egui::CentralPanel::default().show(ui, |ui| self.source_panel(ui));

        if self.settings_open {
            egui::Modal::new(egui::Id::new("settings"))
                .show(ui.ctx(), |ui| self.settings_window(ui));
        }

        self.poll_file_picker(ui.ctx());

        let interval = if self.discovery == DiscoveryState::Scanning {
            Self::REPAINT_SCANNING_INTERVAL
        } else {
            Self::REPAINT_INTERVAL
        };
        ui.ctx().request_repaint_after(interval);
    }
}
