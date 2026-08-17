// SPDX-License-Identifier: MIT OR Apache-2.0
//! Graceful session teardown (`03-cast-engine.md` §7): media `STOP` →
//! receiver `STOP_APP` → `close_notify` → close socket, all best effort.

use tokio::sync::mpsc;

use crate::cast::connection::state_machine::{ConnectionEvent, Phase, RunState};
use crate::cast::connection::transport::lock_transport;
use crate::cast::connection::writer::send_payload;
use crate::cast::namespaces::{MEDIA_NS, RECEIVER_ID, RECEIVER_NS, SOURCE_ID, stop, stop_app};
use crate::cast::proto::encode_cast_message;

/// Graceful session teardown (`03-cast-engine.md` §7): media `STOP` →
/// receiver `STOP_APP` → `close_notify` → close socket. Best effort — a dead
/// peer logs and proceeds.
pub(super) async fn teardown_session(
    state: &mut RunState,
    events: &mpsc::UnboundedSender<ConnectionEvent>,
) {
    let Some(mut session) = state.session.take() else {
        return;
    };

    if let Some(session_id) = session.session_id.clone() {
        if session.phase == Phase::Streaming {
            let id = session.next_request();
            let destination = session.media_destination();
            let payload = encode_cast_message(SOURCE_ID, &destination, MEDIA_NS, &stop(id));
            if let Err(error) = send_payload(&session.transport, payload).await {
                tracing::debug!(%error, "best-effort media STOP failed during teardown");
            }
        }
        let id = session.next_request();
        let payload = encode_cast_message(
            SOURCE_ID,
            RECEIVER_ID,
            RECEIVER_NS,
            &stop_app(id, &session_id),
        );
        if let Err(error) = send_payload(&session.transport, payload).await {
            tracing::debug!(%error, "best-effort STOP_APP failed during teardown");
        }
    }

    let transport = session.transport.clone();
    let closed = tokio::task::spawn_blocking(move || {
        let mut guard = lock_transport(&transport);
        guard.close();
    })
    .await;
    if closed.is_err() {
        tracing::warn!("teardown worker panicked");
    }

    // The reader thread exits once the socket is closed; dropping the
    // channel means its `None` signal is not needed.
    state.inbound = None;
    state.heartbeat = None;
    state.watchdog = None;

    let _ = events.send(ConnectionEvent::Disconnected(session.device));
}
