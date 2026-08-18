// SPDX-License-Identifier: MIT OR Apache-2.0
//! xdg-desktop-portal ScreenCast client (`05-screen-capture.md` §3.4):
//! the D-Bus session dance that yields a PipeWire stream fd for Wayland
//! screen capture (CreateSession → SelectSources → Start →
//! OpenPipeWireRemote → Close).
//!
//! The portal lives on the session bus (`org.freedesktop.portal.Desktop`).
//! `CreateSession`/`SelectSources`/`Start`/`OpenPipeWireRemote` are called on
//! the main object `/org/freedesktop/portal/desktop`, interface
//! `org.freedesktop.portal.ScreenCast`, with the session handle passed as the
//! first argument. Request results are delivered asynchronously: the portal
//! emits `org.freedesktop.portal.Request.Response` `(u response, a{sv}
//! results)` on the request object path, so the client must subscribe to the
//! signal *before* the call that triggers it. A response code of 0 is
//! success, 1 is user cancellation, anything else is an error.
//!
//! Session handles are opaque object paths returned in the `CreateSession`
//! response (`results["session_handle"]`); `Start`'s response carries the
//! stream list (`results["streams"]`, an `a(ua{sv})` whose entries describe
//! size/position). `OpenPipeWireRemote` returns the PipeWire socket fd for
//! the session's stream; the portal closes the session when the client
//! disconnects or calls `org.freedesktop.portal.Session.Close`.
//!
//! Everything is driven on the caller's `std::thread` via `async_io::block_on`
//! (the bridge controller), and the response wait races the shutdown signal
//! so teardown can abort a pending share dialog (KDE's `Start` dialog waits
//! for a user click and can stay open indefinitely).

use std::collections::HashMap;
use std::os::fd::OwnedFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures_util::StreamExt;
use thiserror::Error;
use zbus::message::Type;
use zbus::zvariant::{ObjectPath, OwnedValue, Value};
use zbus::{Connection, MatchRule, MessageStream};

use crate::util::shutdown::Shutdown;

/// Portal D-Bus name and object paths (`05-screen-capture.md` §3.4).
const PORTAL_NAME: &str = "org.freedesktop.portal.Desktop";
const MAIN_PATH: &str = "/org/freedesktop/portal/desktop";
const SCREENCAST_IFACE: &str = "org.freedesktop.portal.ScreenCast";
const REQUEST_IFACE: &str = "org.freedesktop.portal.Request";
const SESSION_IFACE: &str = "org.freedesktop.portal.Session";
const REQUEST_PATH_PREFIX: &str = "/org/freedesktop/portal/desktop/request";

/// Response code carried by `Request.Response` signals.
const RESPONSE_SUCCESS: u32 = 0;
const RESPONSE_CANCELED: u32 = 1;

/// `SelectSources` source-type mask: 1 = monitor, 2 = window, 4 = virtual.
const SOURCE_TYPE_MONITOR: u32 = 1;

/// Handle tokens are restricted to `[A-Za-z0-9_]` (a dash is rejected with
/// `InvalidArgument` by the portal; verified against xdg-desktop-portal-kde).
const HANDLE_TOKEN: &str = "cast_app_capture";

/// Errors from the portal dance (`05-screen-capture.md` §3.4).
#[derive(Debug, Error)]
pub enum PortalError {
    /// D-Bus transport or method-call failure (portal missing, bus gone, …).
    #[error("dbus error: {0}")]
    Dbus(#[from] zbus::Error),
    /// The user dismissed the share dialog (`Response` code 1).
    #[error("screen share was canceled by the user")]
    Canceled,
    /// The portal rejected the request (`Response` code ≥ 2).
    #[error("the portal rejected the request: {0}")]
    Rejected(String),
    /// A response arrived without the expected payload.
    #[error("the portal response was malformed: {0}")]
    Malformed(String),
    /// The capture was aborted (shutdown or display switch while the share
    /// dialog was open).
    #[error("screen capture aborted")]
    Aborted,
}

/// One captured screen stream from `Start`'s response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PortalStream {
    /// PipeWire stream id.
    pub id: u32,
    /// Stream size in pixels as reported by the portal.
    pub size: (u32, u32),
}

/// Abort signal for long portal waits: completes when the pipeline is
/// stopping or the application is shutting down.
#[derive(Debug, Clone)]
pub struct AbortSignal {
    stop: Arc<AtomicBool>,
    shutdown: Shutdown,
}

impl AbortSignal {
    /// Build an abort signal from the bridge's stop flag and the global
    /// shutdown token.
    pub fn new(stop: Arc<AtomicBool>, shutdown: Shutdown) -> Self {
        Self { stop, shutdown }
    }

    /// Whether an abort has been requested already.
    pub fn is_aborted(&self) -> bool {
        self.stop.load(Ordering::Relaxed) || self.shutdown.is_shutting_down()
    }

    /// A future that completes when the capture is aborted.
    async fn wait(&self) {
        let mut shutdown_rx = self.shutdown.subscribe();
        let stop = self.stop.clone();
        let shutdown_wait = async move {
            let _ = shutdown_rx.changed().await;
        };
        let stop_wait = async move {
            loop {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                async_io::Timer::after(std::time::Duration::from_millis(50)).await;
            }
        };
        futures_util::future::select(Box::pin(shutdown_wait), Box::pin(stop_wait)).await;
    }
}

/// The portal client surface, extracted as a trait so bridge tests can
/// inject a fake portal (no D-Bus). All methods block the calling thread.
pub trait ScreenCast: Send + Sync {
    /// Open a capture session. `session_handle_token` must match
    /// `[A-Za-z0-9_]+`; the returned value is the session handle (object
    /// path) reported by the portal.
    fn create_session(&self, session_handle_token: &str) -> Result<String, PortalError>;
    /// Select monitor sources for the session (no dialog for monitors).
    fn select_sources(&self, session: &str) -> Result<(), PortalError>;
    /// Start the capture: blocks until the user accepts the share dialog or
    /// `abort` fires, then returns the first stream.
    fn start(&self, session: &str, abort: &AbortSignal) -> Result<PortalStream, PortalError>;
    /// The PipeWire socket fd for the session's stream.
    fn open_pipewire_remote(&self, session: &str) -> Result<OwnedFd, PortalError>;
    /// Close the session (also happens automatically on disconnect).
    fn close(&self, session: &str) -> Result<(), PortalError>;
}

/// A real portal client over zbus.
#[derive(Debug)]
pub struct ZbusScreenCast {
    conn: Connection,
}

impl ZbusScreenCast {
    /// Connect to the session bus and verify the portal is present.
    pub fn connect_blocking() -> Result<Self, PortalError> {
        async_io::block_on(Self::connect())
    }

    /// Wrap an existing connection (tests drive the client over a p2p
    /// socket pair against a fake portal; the session bus is the only
    /// production path).
    pub fn with_connection(conn: Connection) -> Self {
        Self { conn }
    }

    async fn connect() -> Result<Self, PortalError> {
        let conn = Connection::session().await?;
        Ok(Self { conn })
    }

    /// Subscribe to `org.freedesktop.portal.Request.Response` signals for our
    /// own request objects, *before* the triggering call, so a fast response
    /// can never be missed (the portal responds on the request path, and
    /// KDE reuses one request path per session). No `sender` constraint: on
    /// a real bus only the portal emits this interface, and on p2p test
    /// connections there is no well-known name to match anyway.
    async fn response_stream(&self) -> Result<MessageStream, zbus::Error> {
        let rule = MatchRule::builder()
            .msg_type(Type::Signal)
            .interface(REQUEST_IFACE)?
            .member("Response")?
            .path_namespace(REQUEST_PATH_PREFIX)?
            .build();
        MessageStream::for_match_rule(rule, &self.conn, None).await
    }

    /// Call a portal request method, then wait for the first matching
    /// `Response` signal. Signals that do not satisfy `expect` (e.g. a
    /// response to an earlier request on a reused path) are skipped.
    async fn call_and_wait(
        &self,
        method: &str,
        args: &(impl serde::Serialize + zbus::zvariant::Type + Send),
        expect: impl Fn(&HashMap<String, OwnedValue>) -> bool,
        abort: &AbortSignal,
    ) -> Result<(u32, HashMap<String, OwnedValue>), PortalError> {
        let mut stream = self.response_stream().await?;
        let _request_path = self
            .conn
            .call_method(
                Some(PORTAL_NAME),
                MAIN_PATH,
                Some(SCREENCAST_IFACE),
                method,
                args,
            )
            .await?;
        loop {
            let next = stream.next();
            futures_util::pin_mut!(next);
            let msg = match futures_util::future::select(next, Box::pin(abort.wait())).await {
                futures_util::future::Either::Left((Some(msg), _)) => msg,
                futures_util::future::Either::Left((None, _)) => {
                    return Err(PortalError::Dbus(zbus::Error::Failure(
                        "the portal closed the connection while waiting for a response".into(),
                    )));
                }
                futures_util::future::Either::Right(((), _)) => {
                    return Err(PortalError::Aborted);
                }
            };
            let body: Result<(u32, HashMap<String, OwnedValue>), zbus::Error> =
                msg?.body().deserialize();
            let Ok((code, results)) = body else {
                // Tolerate unrelated messages on the namespace and keep
                // waiting (zbus match rules on non-bus connections are
                // filtered locally).
                continue;
            };
            // A non-zero code is a definitive answer (canceled/rejected)
            // even when the payload lacks the expected key; the portal sends
            // canceled responses with an empty `results` dict.
            if code != RESPONSE_SUCCESS || expect(&results) {
                return Ok((code, results));
            }
        }
    }
}

impl ScreenCast for ZbusScreenCast {
    fn create_session(&self, session_handle_token: &str) -> Result<String, PortalError> {
        let abort = AbortSignal::new(Arc::new(AtomicBool::new(false)), Shutdown::new());
        // The portal's options parameter is `a{sv}`; zbus only emits dict
        // entries for `HashMap` (a tuple array would serialize as `a(sv)`).
        let options = HashMap::from([
            ("handle_token".to_string(), Value::from(HANDLE_TOKEN)),
            (
                "session_handle_token".to_string(),
                Value::from(session_handle_token),
            ),
        ]);
        async_io::block_on(async {
            let (code, mut results) = self
                .call_and_wait(
                    "CreateSession",
                    &(options,),
                    |results| results.contains_key("session_handle"),
                    &abort,
                )
                .await?;
            check_code(code, "CreateSession")?;
            let handle = results
                .remove("session_handle")
                .ok_or_else(|| PortalError::Malformed("missing session_handle".into()))?;
            String::try_from(handle)
                .map_err(|error| PortalError::Malformed(format!("session_handle: {error}")))
        })
    }

    fn select_sources(&self, session: &str) -> Result<(), PortalError> {
        // The portal takes the handle as an `o` (object path), not a string.
        let session = session_object_path(session)?;
        let options = HashMap::from([
            ("types".to_string(), Value::from(SOURCE_TYPE_MONITOR)),
            ("multiple".to_string(), Value::from(false)),
        ]);
        async_io::block_on(async {
            let _reply = self
                .conn
                .call_method(
                    Some(PORTAL_NAME),
                    MAIN_PATH,
                    Some(SCREENCAST_IFACE),
                    "SelectSources",
                    &(session, options),
                )
                .await?;
            Ok(())
        })
    }

    fn start(&self, session: &str, abort: &AbortSignal) -> Result<PortalStream, PortalError> {
        let session = session_object_path(session)?;
        let options = HashMap::<String, Value>::new();
        async_io::block_on(async {
            let (code, mut results) = self
                .call_and_wait(
                    "Start",
                    &(session, "", options),
                    |results| results.contains_key("streams"),
                    abort,
                )
                .await?;
            check_code(code, "Start")?;
            let streams: Vec<(u32, HashMap<String, OwnedValue>)> = results
                .remove("streams")
                .ok_or_else(|| PortalError::Malformed("missing streams".into()))?
                .try_into()
                .map_err(|error: zbus::zvariant::Error| {
                    PortalError::Malformed(format!("streams: {error}"))
                })?;
            let (id, props) = streams
                .into_iter()
                .next()
                .ok_or_else(|| PortalError::Malformed("no streams in the response".into()))?;
            let size: (i32, i32) = props
                .get("size")
                .cloned()
                .ok_or_else(|| PortalError::Malformed("stream without size".into()))?
                .try_into()
                .map_err(|error: zbus::zvariant::Error| {
                    PortalError::Malformed(format!("size: {error}"))
                })?;
            if size.0 <= 0 || size.1 <= 0 {
                return Err(PortalError::Malformed(format!(
                    "invalid stream size {size:?}"
                )));
            }
            Ok(PortalStream {
                id,
                size: (size.0 as u32, size.1 as u32),
            })
        })
    }

    fn open_pipewire_remote(&self, session: &str) -> Result<OwnedFd, PortalError> {
        let session = session_object_path(session)?;
        let options = HashMap::<String, Value>::new();
        async_io::block_on(async {
            let reply = self
                .conn
                .call_method(
                    Some(PORTAL_NAME),
                    MAIN_PATH,
                    Some(SCREENCAST_IFACE),
                    "OpenPipeWireRemote",
                    &(session, options),
                )
                .await?;
            let fd: zbus::zvariant::OwnedFd = reply.body().deserialize()?;
            Ok(fd.into())
        })
    }

    fn close(&self, session: &str) -> Result<(), PortalError> {
        async_io::block_on(async {
            let _reply = self
                .conn
                .call_method(
                    Some(PORTAL_NAME),
                    session,
                    Some(SESSION_IFACE),
                    "Close",
                    &(),
                )
                .await?;
            Ok(())
        })
    }
}

/// Map a `Response` code: 0 is success, 1 is user cancellation, anything
/// else is a rejection.
fn check_code(code: u32, request: &str) -> Result<(), PortalError> {
    match code {
        RESPONSE_SUCCESS => Ok(()),
        RESPONSE_CANCELED => Err(PortalError::Canceled),
        other => Err(PortalError::Rejected(format!("{request}: code {other}"))),
    }
}

/// Parse a session handle (as returned by `CreateSession`) into the object
/// path the ScreenCast methods expect.
fn session_object_path(session: &str) -> Result<ObjectPath<'static>, PortalError> {
    ObjectPath::try_from(String::from(session))
        .map_err(|error| PortalError::Malformed(format!("session_handle: {error}")))
}

/// Whether the xdg-desktop-portal service is reachable on the session bus
/// (availability probe for the Wayland Display source). Cheap: one
/// `name_has_owner` round-trip.
pub fn portal_available() -> bool {
    let Ok(conn) = zbus::blocking::Connection::session() else {
        return false;
    };
    let Ok(proxy) = zbus::blocking::fdo::DBusProxy::new(&conn) else {
        return false;
    };
    let Ok(name) = zbus::names::BusName::try_from(PORTAL_NAME) else {
        return false;
    };
    proxy.name_has_owner(name).unwrap_or(false)
}
