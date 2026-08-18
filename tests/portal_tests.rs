// SPDX-License-Identifier: MIT OR Apache-2.0
//! End-to-end tests of the portal client (`screen/portal.rs`) against a fake
//! xdg-desktop-portal over a zbus p2p socket pair (`05-screen-capture.md`
//! §3.4): the real `ZbusScreenCast` speaks the real wire protocol
//! (CreateSession/SelectSources/Start/OpenPipeWireRemote/Close plus
//! `Request.Response` signals), the fake server answers it, and the client's
//! response parsing, error mapping, stale-response skipping and abort race
//! are exercised without a real portal.

#![cfg(target_os = "linux")]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use cast_app::screen::portal::{AbortSignal, PortalError, ScreenCast, ZbusScreenCast};
use cast_app::util::shutdown::Shutdown;
use zbus::Connection;
use zbus::interface;
use zbus::zvariant::{OwnedFd, OwnedObjectPath, OwnedValue, Value};

const REQUEST_IFACE: &str = "org.freedesktop.portal.Request";
const MAIN_PATH: &str = "/org/freedesktop/portal/desktop";
const REQUEST_PREFIX: &str = "/org/freedesktop/portal/desktop/request/cast_app_test/";
const SESSION_PREFIX: &str = "/org/freedesktop/portal/desktop/session/cast_app_test/";
const SERVER_GUID: &str = "0123456789abcdef0123456789abcdef";

/// How the fake portal's `Start` answers (or never answers).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum StartOutcome {
    /// Emit a valid stream response.
    #[default]
    Stream,
    /// User dismissed the dialog: response code 1.
    Canceled,
    /// Portal error: the given response code.
    Rejected(u32),
    /// Never emit a response (the client must abort).
    Never,
}

#[derive(Default)]
struct FakePortalInner {
    next_request: AtomicU32,
    next_session: AtomicU32,
    /// Session paths Close() was called on, in order.
    closed_sessions: Mutex<Vec<String>>,
    /// Session paths handed out by CreateSession.
    created_sessions: Mutex<Vec<String>>,
    start_outcome: Mutex<StartOutcome>,
    /// Emit one unrelated Response signal before the real one (KDE reuses
    /// request paths across calls, so the client must skip stale responses).
    stale_response_first: Mutex<bool>,
}

struct FakePortal {
    inner: Arc<FakePortalInner>,
}

impl FakePortal {
    fn next_request_path(&self) -> OwnedObjectPath {
        let n = self.inner.next_request.fetch_add(1, Ordering::Relaxed);
        format!("{REQUEST_PREFIX}{n}").try_into().unwrap()
    }

    fn next_session_path(&self) -> String {
        let n = self.inner.next_session.fetch_add(1, Ordering::Relaxed);
        format!("{SESSION_PREFIX}{n}")
    }

    async fn emit_response(
        &self,
        conn: &Connection,
        request: &OwnedObjectPath,
        code: u32,
        results: HashMap<String, Value<'static>>,
    ) -> zbus::fdo::Result<()> {
        conn.emit_signal(
            None::<&str>,
            request,
            REQUEST_IFACE,
            "Response",
            &(code, results),
        )
        .await
        .map_err(|error| zbus::fdo::Error::Failed(error.to_string()))
    }
}

/// The `org.freedesktop.portal.ScreenCast` server (fake portal).
#[interface(name = "org.freedesktop.portal.ScreenCast")]
impl FakePortal {
    async fn create_session(
        &self,
        _options: HashMap<String, OwnedValue>,
        #[zbus(connection)] conn: &Connection,
        #[zbus(object_server)] os: &zbus::ObjectServer,
    ) -> zbus::fdo::Result<OwnedObjectPath> {
        let request = self.next_request_path();
        let session = self.next_session_path();
        self.inner
            .created_sessions
            .lock()
            .unwrap()
            .push(session.clone());
        if *self.inner.stale_response_first.lock().unwrap() {
            self.emit_response(conn, &request, 0, HashMap::new())
                .await?;
        }
        os.at::<_, FakeSession>(
            session.as_str(),
            FakeSession {
                path: session.clone(),
                inner: Arc::clone(&self.inner),
            },
        )
        .await
        .map_err(|error| zbus::fdo::Error::Failed(error.to_string()))?;
        let results = HashMap::from([("session_handle".to_string(), Value::from(session))]);
        self.emit_response(conn, &request, 0, results).await?;
        Ok(request)
    }

    async fn select_sources(
        &self,
        session: OwnedObjectPath,
        _options: HashMap<String, OwnedValue>,
    ) -> zbus::fdo::Result<()> {
        // A real portal rejects unknown session handles.
        if !self
            .inner
            .created_sessions
            .lock()
            .unwrap()
            .contains(&session.to_string())
        {
            return Err(zbus::fdo::Error::UnknownObject(
                "unknown session handle".into(),
            ));
        }
        Ok(())
    }

    async fn start(
        &self,
        _session: OwnedObjectPath,
        _parent_window: &str,
        _options: HashMap<String, OwnedValue>,
        #[zbus(connection)] conn: &Connection,
    ) -> zbus::fdo::Result<OwnedObjectPath> {
        let request = self.next_request_path();
        let outcome = *self.inner.start_outcome.lock().unwrap();
        match outcome {
            StartOutcome::Never => {}
            StartOutcome::Canceled => {
                self.emit_response(conn, &request, 1, HashMap::new())
                    .await?;
            }
            StartOutcome::Rejected(code) => {
                self.emit_response(conn, &request, code, HashMap::new())
                    .await?;
            }
            StartOutcome::Stream => {
                let props = HashMap::from([("size".to_string(), Value::from((640i32, 480i32)))]);
                let streams = Value::from(vec![(1u32, props)]);
                let results = HashMap::from([("streams".to_string(), streams)]);
                self.emit_response(conn, &request, 0, results).await?;
            }
        }
        Ok(request)
    }

    /// The real D-Bus member is `OpenPipeWireRemote` (capital W); the macro
    /// would derive `OpenPipewireRemote` from the Rust name.
    #[zbus(name = "OpenPipeWireRemote")]
    async fn open_pipewire_remote(
        &self,
        _session: OwnedObjectPath,
        _options: HashMap<String, OwnedValue>,
    ) -> zbus::fdo::Result<OwnedFd> {
        // The fd is passed over the p2p socket like a real portal's.
        let file = std::fs::File::open("/dev/null")
            .map_err(|error| zbus::fdo::Error::Failed(error.to_string()))?;
        Ok(OwnedFd::from(std::os::fd::OwnedFd::from(file)))
    }
}

/// The `org.freedesktop.portal.Session` server on a fake session path.
struct FakeSession {
    path: String,
    inner: Arc<FakePortalInner>,
}

#[interface(name = "org.freedesktop.portal.Session")]
impl FakeSession {
    async fn close(&self) -> zbus::fdo::Result<()> {
        self.inner
            .closed_sessions
            .lock()
            .unwrap()
            .push(self.path.clone());
        Ok(())
    }
}

/// A portal client speaking to the fake server over a p2p socket pair. The
/// server connection is returned so the test scope keeps it alive (its
/// dispatcher runs on the async-io reactor).
fn portal_pair(inner: Arc<FakePortalInner>) -> (ZbusScreenCast, Connection) {
    let (server_stream, client_stream) =
        std::os::unix::net::UnixStream::pair().expect("socketpair");
    // The SASL handshake needs both sides live, so the two connections are
    // built concurrently (same pattern as zbus's own p2p tests).
    let (server, client) = async_io::block_on(async {
        let server = zbus::connection::Builder::unix_stream(server_stream)
            .server(SERVER_GUID)
            .unwrap()
            .p2p()
            .serve_at(
                MAIN_PATH,
                FakePortal {
                    inner: Arc::clone(&inner),
                },
            )
            .unwrap();
        let client = zbus::connection::Builder::unix_stream(client_stream).p2p();
        futures_util::try_join!(server.build(), client.build()).expect("p2p connections")
    });
    (ZbusScreenCast::with_connection(client), server)
}

fn inner() -> Arc<FakePortalInner> {
    Arc::new(FakePortalInner::default())
}

#[test]
fn full_dance_negotiates_a_stream_and_closes_the_session() {
    let inner = inner();
    let (client, _server) = portal_pair(Arc::clone(&inner));
    let session = client
        .create_session("cast_app_capture")
        .expect("session handle");
    assert!(session.starts_with(SESSION_PREFIX));
    client.select_sources(&session).expect("sources selected");
    let stream = client
        .start(
            &session,
            &AbortSignal::new(Arc::new(AtomicBool::new(false)), Shutdown::new()),
        )
        .expect("stream");
    assert_eq!(stream.id, 1);
    assert_eq!(stream.size, (640, 480));
    let fd = client.open_pipewire_remote(&session).expect("stream fd");
    // The fd really crossed the socket pair: reading /dev/null returns EOF.
    use std::io::Read;
    let mut file = std::fs::File::from(fd);
    let mut buffer = [0u8; 16];
    assert_eq!(file.read(&mut buffer).expect("read"), 0);
    client.close(&session).expect("session close");
    assert_eq!(
        *inner.closed_sessions.lock().unwrap(),
        vec![session],
        "Close must reach the session object path"
    );
}

#[test]
fn stale_responses_are_skipped() {
    // KDE reuses one request path per session; the response for an earlier
    // request can arrive first and must not be mistaken for the current one.
    let inner = inner();
    *inner.stale_response_first.lock().unwrap() = true;
    let (client, _server) = portal_pair(inner);
    let session = client
        .create_session("cast_app_capture")
        .expect("session handle");
    assert!(session.starts_with(SESSION_PREFIX));
}

#[test]
fn canceled_start_maps_to_canceled() {
    let inner = inner();
    *inner.start_outcome.lock().unwrap() = StartOutcome::Canceled;
    let (client, _server) = portal_pair(inner);
    let session = client
        .create_session("cast_app_capture")
        .expect("session handle");
    client.select_sources(&session).expect("sources selected");
    let result = client.start(
        &session,
        &AbortSignal::new(Arc::new(AtomicBool::new(false)), Shutdown::new()),
    );
    assert!(matches!(result, Err(PortalError::Canceled)));
}

#[test]
fn rejected_start_maps_to_rejected() {
    let inner = inner();
    *inner.start_outcome.lock().unwrap() = StartOutcome::Rejected(42);
    let (client, _server) = portal_pair(inner);
    let session = client
        .create_session("cast_app_capture")
        .expect("session handle");
    client.select_sources(&session).expect("sources selected");
    let result = client.start(
        &session,
        &AbortSignal::new(Arc::new(AtomicBool::new(false)), Shutdown::new()),
    );
    match result {
        Err(PortalError::Rejected(message)) => assert!(message.contains("42"), "{message}"),
        other => panic!("expected Rejected, got {other:?}"),
    }
}

#[test]
fn abort_interrupts_a_never_responding_start() {
    let inner = inner();
    *inner.start_outcome.lock().unwrap() = StartOutcome::Never;
    let (client, _server) = portal_pair(inner);
    let session = client
        .create_session("cast_app_capture")
        .expect("session handle");
    client.select_sources(&session).expect("sources selected");
    // The bridge sets the stop flag on teardown while the dialog is open;
    // the client must stop waiting and return Aborted.
    let abort = AbortSignal::new(Arc::new(AtomicBool::new(true)), Shutdown::new());
    let result = client.start(&session, &abort);
    assert!(matches!(result, Err(PortalError::Aborted)));
}

#[test]
fn select_sources_rejects_invalid_sessions() {
    // A real portal answers with a D-Bus error for unknown session paths;
    // the fake's ObjectServer does the same for a path it never served.
    let (client, _server) = portal_pair(inner());
    let result = client.select_sources("/org/freedesktop/portal/desktop/session/nope");
    assert!(result.is_err(), "unknown session must fail: {result:?}");
}
