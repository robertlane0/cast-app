// SPDX-License-Identifier: MIT OR Apache-2.0
//! GUI state tests (`02-gui.md` §3–§4): command dispatch for every
//! `AppCommand` variant, `try_recv` event application, URL validation,
//! volume-throttle timing, and status-indicator updates from synthetic
//! `BackendEvent`s.
//! Gate: `cargo test --test gui_state_tests`.

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use cast_app::app::{
    CastDashboard, ConnectionState, DiscoveryState, PlaybackState, VolumeThrottle,
};
use cast_app::state::{AppCommand, BackendEvent, CastDevice, SourceTab};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

fn device(name: &str) -> CastDevice {
    CastDevice {
        id: format!("{name}:8009"),
        name: name.to_string(),
        addr: SocketAddr::from(([192, 168, 1, 50], 8009)),
    }
}

struct Harness {
    dashboard: CastDashboard,
    commands: UnboundedReceiver<AppCommand>,
    events: UnboundedSender<BackendEvent>,
}

impl Harness {
    fn new() -> Self {
        let (command_tx, commands) = tokio::sync::mpsc::unbounded_channel();
        let (events, event_rx) = tokio::sync::mpsc::unbounded_channel();
        let dashboard = CastDashboard::new(command_tx, event_rx);
        Self {
            dashboard,
            commands,
            events,
        }
    }

    /// Push one event and drain it through `handle_events`.
    fn push(&mut self, event: BackendEvent) {
        self.events.send(event).unwrap();
        assert_eq!(self.dashboard.handle_events(), 1);
    }

    fn next_command(&mut self) -> AppCommand {
        self.commands
            .try_recv()
            .expect("expected a dispatched command")
    }

    fn assert_no_command(&mut self) {
        assert!(
            self.commands.try_recv().is_err(),
            "no command should have been dispatched"
        );
    }
}

// ---------------------------------------------------------------------------
// Initial state
// ---------------------------------------------------------------------------

#[test]
fn initial_state_is_scanning_and_idle() {
    let mut h = Harness::new();
    assert_eq!(h.dashboard.discovery(), &DiscoveryState::Scanning);
    assert_eq!(h.dashboard.connection(), &ConnectionState::Scanning);
    assert_eq!(h.dashboard.playback(), PlaybackState::Idle);
    assert!(h.dashboard.receivers().is_empty());
    assert!(h.dashboard.displays().is_empty());
    assert!(h.dashboard.selected_receiver().is_none());
    assert_eq!(h.dashboard.volume(), 0);
    assert!(!h.dashboard.muted());
    assert_eq!(h.dashboard.proxy_port(), 8080);
    assert!(!h.dashboard.has_active_source());
    assert!(!h.dashboard.file_picker_open());
    assert!(h.dashboard.error_banner().is_none());
    assert_eq!(
        h.dashboard.ffmpeg_available(),
        cast_app::screen::ffmpeg_discover::ffmpeg_available()
    );
    h.assert_no_command();
}

// ---------------------------------------------------------------------------
// Discovery state (02-gui.md §3.1)
// ---------------------------------------------------------------------------

#[test]
fn receivers_updated_populates_list_and_ready_state() {
    let mut h = Harness::new();
    let first = device("Living Room");
    let second = device("Bedroom");
    h.push(BackendEvent::ReceiversUpdated(vec![first, second]));
    assert_eq!(h.dashboard.discovery(), &DiscoveryState::Ready);
    assert_eq!(
        h.dashboard.receivers(),
        &[device("Living Room"), device("Bedroom")]
    );
}

#[test]
fn empty_snapshot_marks_no_receivers() {
    let mut h = Harness::new();
    h.push(BackendEvent::ReceiversUpdated(vec![]));
    assert_eq!(h.dashboard.discovery(), &DiscoveryState::NoReceivers);
}

#[test]
fn list_survives_an_empty_refresh_only_when_empty() {
    let mut h = Harness::new();
    h.push(BackendEvent::ReceiversUpdated(vec![device("A")]));
    h.push(BackendEvent::ReceiversUpdated(vec![]));
    assert_eq!(h.dashboard.discovery(), &DiscoveryState::NoReceivers);
    assert!(h.dashboard.receivers().is_empty());
    h.push(BackendEvent::ReceiversUpdated(vec![device("A")]));
    assert_eq!(h.dashboard.discovery(), &DiscoveryState::Ready);
}

#[test]
fn connection_error_without_receivers_shows_error_and_retry_rescans() {
    let mut h = Harness::new();
    h.push(BackendEvent::ConnectionError(
        "multicast socket setup failed".into(),
    ));
    assert_eq!(
        h.dashboard.discovery(),
        &DiscoveryState::Error("multicast socket setup failed".into())
    );
    assert_eq!(h.dashboard.connection(), &ConnectionState::Disconnected);

    h.dashboard.select_receiver(&device("A"));
    h.next_command();
    h.dashboard.retry_discovery();
    assert_eq!(h.dashboard.discovery(), &DiscoveryState::Scanning);
    assert_eq!(h.next_command(), AppCommand::Rescan);
}

#[test]
fn connection_error_with_receivers_keeps_the_list() {
    let mut h = Harness::new();
    h.push(BackendEvent::ReceiversUpdated(vec![device("A")]));
    h.push(BackendEvent::ConnectionError("connection refused".into()));
    assert_eq!(h.dashboard.discovery(), &DiscoveryState::Ready);
    assert_eq!(h.dashboard.receivers(), &[device("A")]);
    assert_eq!(h.dashboard.connection(), &ConnectionState::Disconnected);
}

// ---------------------------------------------------------------------------
// Connection and playback status (02-gui.md §3.4)
// ---------------------------------------------------------------------------

#[test]
fn receiver_connected_sets_connection_and_selection() {
    let mut h = Harness::new();
    h.push(BackendEvent::ReceiverConnected(device("Living Room")));
    assert_eq!(
        h.dashboard.connection(),
        &ConnectionState::Connected(device("Living Room"))
    );
    assert_eq!(
        h.dashboard.selected_receiver(),
        Some(&device("Living Room"))
    );
}

#[test]
fn receiver_disconnected_clears_selection_and_playback() {
    let mut h = Harness::new();
    h.push(BackendEvent::ReceiverConnected(device("A")));
    h.push(BackendEvent::MediaStatus {
        playing: true,
        buffering: false,
    });
    h.push(BackendEvent::ReceiverDisconnected(device("A")));
    assert_eq!(h.dashboard.connection(), &ConnectionState::Disconnected);
    assert!(h.dashboard.selected_receiver().is_none());
    assert_eq!(h.dashboard.playback(), PlaybackState::Idle);
}

#[test]
fn media_status_maps_to_playback_states() {
    let mut h = Harness::new();
    h.push(BackendEvent::MediaStatus {
        playing: true,
        buffering: false,
    });
    assert_eq!(h.dashboard.playback(), PlaybackState::Playing);
    h.push(BackendEvent::MediaStatus {
        playing: false,
        buffering: true,
    });
    assert_eq!(h.dashboard.playback(), PlaybackState::Buffering);
    h.push(BackendEvent::MediaStatus {
        playing: false,
        buffering: false,
    });
    assert_eq!(h.dashboard.playback(), PlaybackState::Paused);
}

// ---------------------------------------------------------------------------
// Error banner (02-gui.md §3.4)
// ---------------------------------------------------------------------------

#[test]
fn stream_error_shows_banner_and_success_event_dismisses_it() {
    let mut h = Harness::new();
    h.push(BackendEvent::StreamError("capture failed".into()));
    assert_eq!(h.dashboard.error_banner(), Some("capture failed"));
    h.push(BackendEvent::DisplaysUpdated(vec!["DP-1".into()]));
    assert!(h.dashboard.error_banner().is_none());
}

#[test]
fn error_banner_is_manually_dismissable() {
    let mut h = Harness::new();
    h.push(BackendEvent::ConnectionError("boom".into()));
    assert!(h.dashboard.error_banner().is_some());
    h.dashboard.dismiss_error();
    assert!(h.dashboard.error_banner().is_none());
}

#[test]
fn error_banner_is_transient_across_success_events() {
    let mut h = Harness::new();
    h.push(BackendEvent::StreamError("one".into()));
    h.push(BackendEvent::StreamError("two".into()));
    assert_eq!(h.dashboard.error_banner(), Some("two"));
    h.push(BackendEvent::MediaStatus {
        playing: true,
        buffering: false,
    });
    assert!(h.dashboard.error_banner().is_none());
}

#[test]
fn multiple_events_are_applied_in_order_in_one_drain() {
    let mut h = Harness::new();
    h.events
        .send(BackendEvent::ReceiversUpdated(vec![device("A")]))
        .unwrap();
    h.events
        .send(BackendEvent::ConnectionError("late failure".into()))
        .unwrap();
    assert_eq!(h.dashboard.handle_events(), 2);
    assert_eq!(h.dashboard.receivers(), &[device("A")]);
    assert_eq!(h.dashboard.error_banner(), Some("late failure"));
}

// ---------------------------------------------------------------------------
// Command dispatch (02-gui.md §4.1)
// ---------------------------------------------------------------------------

#[test]
fn selecting_receiver_dispatches_command() {
    let mut h = Harness::new();
    h.dashboard.select_receiver(&device("Living Room"));
    assert_eq!(
        h.dashboard.selected_receiver(),
        Some(&device("Living Room"))
    );
    assert_eq!(
        h.next_command(),
        AppCommand::SelectReceiver(device("Living Room"))
    );
}

#[test]
fn switching_tab_dispatches_select_source() {
    let mut h = Harness::new();
    h.dashboard.select_tab(SourceTab::WebUrl);
    assert_eq!(h.dashboard.source_tab(), SourceTab::WebUrl);
    assert_eq!(
        h.next_command(),
        AppCommand::SelectSource(SourceTab::WebUrl)
    );
}

#[test]
fn selecting_display_dispatches_command_and_activates_source() {
    let mut h = Harness::new();
    h.dashboard.select_display("HDMI-1");
    assert_eq!(h.dashboard.selected_display(), Some("HDMI-1"));
    assert!(h.dashboard.has_active_source());
    assert_eq!(h.next_command(), AppCommand::SelectDisplay("HDMI-1".into()));
}

#[test]
fn selecting_file_dispatches_command_and_activates_source() {
    let mut h = Harness::new();
    let path = PathBuf::from("/tmp/movie.mp4");
    h.dashboard.select_file(path.clone());
    assert!(h.dashboard.has_active_source());
    assert_eq!(h.next_command(), AppCommand::SelectFile(path));
}

#[test]
fn selecting_url_trims_and_dispatches() {
    let mut h = Harness::new();
    h.dashboard.select_url("  https://example.com/a.mp4  ");
    assert!(h.dashboard.has_active_source());
    assert_eq!(
        h.next_command(),
        AppCommand::SelectUrl("https://example.com/a.mp4".into())
    );
}

#[test]
fn transport_commands_dispatch() {
    let mut h = Harness::new();
    h.dashboard.play();
    h.dashboard.pause();
    h.dashboard.stop();
    assert_eq!(h.next_command(), AppCommand::Play);
    assert_eq!(h.next_command(), AppCommand::Pause);
    assert_eq!(h.next_command(), AppCommand::Stop);
}

#[test]
fn mute_dispatches() {
    let mut h = Harness::new();
    h.dashboard.toggle_mute(true);
    assert!(h.dashboard.muted());
    assert_eq!(h.next_command(), AppCommand::Mute(true));
}

#[test]
fn saving_proxy_port_dispatches() {
    let mut h = Harness::new();
    h.dashboard.save_proxy_port(9090);
    assert_eq!(h.dashboard.proxy_port(), 9090);
    assert_eq!(h.next_command(), AppCommand::SetProxyPort(9090));
}

// ---------------------------------------------------------------------------
// Enablement rules (02-gui.md §3.3)
// ---------------------------------------------------------------------------

#[test]
fn play_pause_stop_enablement_rules() {
    let mut h = Harness::new();
    assert!(!h.dashboard.can_play());
    assert!(!h.dashboard.can_pause());
    assert!(!h.dashboard.can_stop());

    h.dashboard.select_receiver(&device("A"));
    h.next_command();
    assert!(h.dashboard.can_stop());
    assert!(!h.dashboard.can_play(), "no source active yet");
    assert!(!h.dashboard.can_pause());

    h.dashboard.select_file(PathBuf::from("/tmp/a.mp4"));
    h.next_command();
    assert!(h.dashboard.can_play());
    assert!(h.dashboard.can_pause());
    assert!(h.dashboard.can_stop());
}

#[test]
fn display_dropdown_disabled_without_monitors() {
    let mut h = Harness::new();
    assert!(!h.dashboard.display_dropdown_enabled());
    h.push(BackendEvent::DisplaysUpdated(vec![]));
    assert!(!h.dashboard.display_dropdown_enabled());
}

#[test]
fn displays_updated_populates_display_list() {
    let mut h = Harness::new();
    h.push(BackendEvent::DisplaysUpdated(vec![
        "HDMI-1".into(),
        "DP-1".into(),
    ]));
    assert_eq!(h.dashboard.displays(), &["HDMI-1", "DP-1"]);
}

// ---------------------------------------------------------------------------
// URL validation (02-gui.md §3.2)
// ---------------------------------------------------------------------------

#[test]
fn valid_urls_parse_as_absolute_http_with_host() {
    for valid in [
        "http://example.com",
        "https://example.com/video.mp4?q=1",
        "https://example.com:8443/path/media.mkv",
        "HTTP://EXAMPLE.COM:8080/a",
        "  https://example.com/a  ",
        // Anonymous network shares (`04-media-proxy.md` §4.4): host + share +
        // file path, no credentials.
        "smb://nas/share/video.mp4",
        "smb://192.168.1.50/media/dir/movie.mkv",
        "smb://nas:1445/share/My%20Movie.mp4",
    ] {
        assert!(
            CastDashboard::validate_url(valid),
            "expected valid: {valid:?}"
        );
    }
}

#[test]
fn invalid_urls_are_rejected() {
    for invalid in [
        "",
        "   ",
        "example.com/video.mp4",
        "/path/video.mp4",
        "ftp://example.com/a.mp4",
        "file:///tmp/a.mp4",
        "javascript:alert(1)",
        "http://",
        "https://",
        "not a url",
        // SMB URLs without a share or file path are not streamable.
        "smb://host",
        "smb://host/share",
        "smb://host/share/",
        // Credentials are never accepted on share URLs (anonymous-only).
        "smb://user@host/share/video.mp4",
        "smb://user:pass@host/share/video.mp4",
    ] {
        assert!(
            !CastDashboard::validate_url(invalid),
            "expected invalid: {invalid:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Volume throttle (02-gui.md §3.3)
// ---------------------------------------------------------------------------

#[test]
fn volume_is_throttled_to_one_message_per_100ms_with_trailing_edge() {
    let mut throttle = VolumeThrottle::new();
    let t0 = Instant::now();
    assert_eq!(throttle.poll(t0), None, "idle throttle sends nothing");

    throttle.note_change(0.5);
    assert_eq!(
        throttle.poll(t0),
        Some(0.5),
        "leading edge sends immediately"
    );
    assert!(!throttle.is_pending());

    throttle.note_change(0.6);
    assert_eq!(throttle.poll(t0 + Duration::from_millis(50)), None);
    assert!(throttle.is_pending(), "queued for the trailing edge");
    assert_eq!(throttle.poll(t0 + Duration::from_millis(99)), None);
    assert_eq!(
        throttle.poll(t0 + Duration::from_millis(100)),
        Some(0.6),
        "trailing edge lands at exactly 100 ms"
    );
    assert!(!throttle.is_pending());
    assert_eq!(throttle.poll(t0 + Duration::from_millis(250)), None);
}

#[test]
fn volume_bursts_coalesce_to_the_latest_value() {
    let mut throttle = VolumeThrottle::new();
    let t0 = Instant::now();
    throttle.note_change(0.2);
    throttle.note_change(0.3);
    throttle.note_change(0.4);
    assert_eq!(throttle.poll(t0 + Duration::from_millis(500)), Some(0.4));
}

#[test]
fn volume_slider_maps_0_100_to_0_1() {
    let mut throttle = VolumeThrottle::new();
    let t0 = Instant::now();
    throttle.note_change(50.0 / 100.0);
    assert_eq!(throttle.poll(t0), Some(0.5));
}

#[test]
fn volume_event_corrects_local_value_and_mute() {
    let mut h = Harness::new();
    h.push(BackendEvent::Volume {
        level: 0.42,
        muted: true,
    });
    assert_eq!(h.dashboard.volume(), 42);
    assert!(h.dashboard.muted());
}

#[test]
fn volume_event_is_ignored_while_slider_is_dragged() {
    let mut h = Harness::new();
    h.push(BackendEvent::Volume {
        level: 0.42,
        muted: true,
    });
    h.dashboard.set_volume_dragging(true);
    h.push(BackendEvent::Volume {
        level: 0.90,
        muted: false,
    });
    assert_eq!(h.dashboard.volume(), 42, "no jump during a drag");
    assert!(
        !h.dashboard.muted(),
        "mute is corrected even while the volume slider is dragged"
    );
    h.dashboard.set_volume_dragging(false);
}

// ---------------------------------------------------------------------------
// Settings (02-gui.md §3.5)
// ---------------------------------------------------------------------------

#[test]
fn proxy_port_defaults_to_8080() {
    let h = Harness::new();
    assert_eq!(h.dashboard.proxy_port(), 8080);
}

#[test]
fn set_volume_command_flow() {
    let mut h = Harness::new();
    h.dashboard.set_volume(75);
    assert!(
        h.dashboard.poll_and_dispatch_volume(Instant::now()),
        "leading edge dispatches immediately"
    );
    assert_eq!(h.next_command(), AppCommand::SetVolume(0.75));
    assert!(!h.dashboard.volume_pending());
}
