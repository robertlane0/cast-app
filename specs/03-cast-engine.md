# 03 — Cast Engine Specification

## 1. Scope

Implement Google Cast V2 communication without `rust-cast`, `mdns`, or `prost`.

The protocol flow is:

```text
mDNS discovery
    -> TLS connection
    -> CastV2 Protobuf framing
    -> JSON namespace messages
```

## 2. mDNS discovery

### 2.1 Socket setup

The engine SHALL:

1. Bind a `std::net::UdpSocket` to `0.0.0.0:0`.
2. Join IPv4 multicast group `224.0.0.251`.
3. Send a DNS query for the PTR record `_googlecast._tcp.local` to the mDNS multicast address and port:

```text
224.0.0.251:5353
```

Port `5353` is the standard mDNS port. Responders detect a non-`5353` source port and unicast their reply to the socket's ephemeral source port, so the ephemeral bind at `0.0.0.0:0` is sufficient and no `5353` listener is required.

### 2.2 Query

The engine SHALL send one DNS question per cycle:

- query type `PTR`;
- QNAME `_googlecast._tcp.local`;
- query ID `0`;
- recursion-desired flag cleared (multicast DNS).

The engine SHALL resend the query every 10 seconds to refresh the device list.

### 2.3 Response parsing

The engine SHALL safely parse binary DNS response packets and extract:

- receiver IP address from an `A` record;
- receiver port from an `SRV` record;
- friendly receiver name from a `TXT` record.

The overview states that the receiver port is usually `8009`.

The DNS packet grammar:

- 12-byte header (ID, flags, QDCOUNT, ANCOUNT, NSCOUNT, ARCOUNT);
- question section (`QDCOUNT` entries);
- answer section (`ANCOUNT` entries);
- authority section (`NSCOUNT` entries);
- additional section (`ARCOUNT` entries).

The parser SHALL:

- support DNS compression pointers (`0b11000000` high bits with a 14-bit offset), with a maximum pointer depth of 4 and a cycle guard;
- decode string labels using the length-prefix scheme;
- skip records of unsupported types;
- discard malformed packets by logging and skipping them; parsing SHALL never panic.

### 2.4 Record correlation

The PTR answer yields the service instance name:

```text
<device-name>._googlecast._tcp.local
```

The engine SHALL correlate SRV, TXT and A records in the same response to that instance name before extracting values:

- `SRV` — receiver port (the target hostname is not used for connection);
- `A` — receiver IPv4 address;
- `TXT` — friendly name from the `fn=` key-value pair, falling back to the instance label.

TXT records SHALL be parsed as RFC 6763 length-prefixed `key=value` strings.

### 2.5 Discovery lifecycle

- Devices SHALL be de-duplicated by IP and port.
- A device SHALL expire after 3 consecutive missed query cycles (~30 seconds).
- Discovery is explicitly IPv4-only: the socket binds `0.0.0.0`, joins the IPv4 group, and only IPv4 `A` records are consumed.
- Socket send/receive errors SHALL be logged and the discovery task SHALL continue; fatal setup errors (e.g., multicast join failure) SHALL be surfaced to the GUI.

## 3. TLS transport

The Cast receiver connection SHALL:

- use TCP;
- connect on the discovered receiver port, normally `8009`;
- use `rustls` (0.23.x with the `ring` provider; the exact version SHALL be pinned in `Cargo.lock` at implementation time);
- accept the receiver's self-signed certificate through a custom certificate verifier.

### 3.1 Certificate verifier

The verifier SHALL:

- complete the full TLS handshake including server signature verification;
- skip certificate chain and identity (hostname) trust evaluation, because Cast devices use self-signed certificates and no trust anchor or pin-distribution channel exists.

Hostname/certificate pinning is a documented future hardening option and is out of scope for this release.

### 3.2 Handshake parameters

- No ALPN negotiation is required by Cast receivers; the client SHALL offer none.
- No SNI is required.
- The TLS handshake SHALL complete within 5 seconds; otherwise the connection attempt SHALL fail with a timeout.
- Graceful shutdown SHALL send a `close_notify` alert before the socket is closed.

## 4. CastV2 framing

Every CastV2 message SHALL be framed as:

```text
4-byte big-endian u32 payload length
+
CastMessage Protobuf payload
```

The engine SHALL hand-roll the required Protobuf serialization.

### 4.1 Encoder

The encoder SHALL write a 4-byte big-endian `u32` length prefix followed by the encoded CastMessage in a single write.

### 4.2 Decoder

The decoder SHALL read 4 bytes to obtain the big-endian `u32` length `N`, then read exactly `N` bytes.

The maximum acceptable frame size is 16 MiB. A length prefix exceeding this limit SHALL be treated as a protocol error and SHALL close the connection.

## 5. CastMessage encoding

The encoder SHALL represent the CastMessage fields used by the application:

| Field | Number | Wire type | Value |
|---|---|---|---|
| `protocol_version` | 1 | varint | `0` = `CASTV2_1_0` |
| `source_id` | 2 | length-delimited (string) | ASCII source ID |
| `destination_id` | 3 | length-delimited (string) | ASCII destination ID |
| `namespace` | 4 | length-delimited (string) | namespace URN |
| `payload_type` | 5 | varint | `0` = `STRING`, `1` = `BINARY` |
| `payload_utf8` | 6 | length-delimited (string) | UTF-8 JSON payload |
| `payload_binary` | 7 | length-delimited (bytes) | unused by this application |

A representative encoder shape is:

```rust
fn encode_cast_message(
    source: &str,
    dest: &str,
    namespace: &str,
    payload: &str,
) -> Vec<u8>
```

The integer encoding helper SHALL write unsigned varints (base-128 LEB128). String and bytes fields SHALL be encoded as tag, varint length, then the raw bytes.

The decoder SHALL parse the inverse layout, tolerate unknown fields by skipping them according to their wire type, and reject malformed or over-length frames without panicking.

## 6. Namespaces

### 6.0 Message addressing

Source and destination IDs:

| Direction | Source ID | Destination ID | Namespaces |
|---|---|---|---|
| Transport (client to device) | `source-0` | `transport-0` | connection, heartbeat |
| Receiver (client to device) | `source-0` | `receiver-0` | receiver |
| Media (client to device) | `source-0` | `transport-<transportId>` | media |

The media destination ID is derived from the `transportId` in the `RECEIVER_STATUS` response to `LAUNCH`.

The engine SHALL maintain a per-connection monotonic `u32` request ID. Every request SHALL carry a `requestId`; incoming responses SHALL be correlated to outstanding requests by `requestId`, with a 5-second response timeout after which the request is considered failed and logged.

### 6.1 Connection

Namespace:

`urn:x-cast:com.google.cast.tp.connection`

On connection initialization, send:

```json
{"type":"CONNECT"}
```

### 6.2 Heartbeat

Namespace:

`urn:x-cast:com.google.cast.tp.heartbeat`

A background Tokio task SHALL send:

```json
{"type":"PING"}
```

every 5 seconds.

The engine SHALL process the corresponding receiver heartbeat traffic. A `PONG` SHALL reset the heartbeat timer. If no `PONG` is received within 10 seconds, the engine SHALL close the connection and enter the reconnection policy described in the receiver lifecycle.

### 6.3 Receiver

Namespace:

`urn:x-cast:com.google.cast.receiver`

The engine SHALL send a `LAUNCH` command for the Default Media Receiver App ID:

```text
CC1AD845
```

```json
{"type":"LAUNCH","requestId":1,"appId":"CC1AD845"}
```

Other receiver commands:

- `GET_STATUS` — `{"type":"GET_STATUS","requestId":N}`;
- `SET_VOLUME` — `{"type":"SET_VOLUME","requestId":N,"volume":{"level":0.5,"muted":false}}`, where `level` is in `0.0`–`1.0`;
- `STOP_APP` — `{"type":"STOP_APP","requestId":N,"sessionId":"<sessionId>"}`.

The engine SHALL parse `RECEIVER_STATUS` responses:

```json
{
  "type": "RECEIVER_STATUS",
  "requestId": 1,
  "status": {
    "applications": [
      {"appId":"CC1AD845","transportId":"<transportId>","sessionId":"<sessionId>"}
    ],
    "volume": {}
  }
}
```

and extract `transportId` and `sessionId` from the matching application entry.

### 6.4 Media

Namespace:

`urn:x-cast:com.google.cast.media`

The engine SHALL send media commands to `transport-<transportId>` with the per-connection `requestId` sequence.

`LOAD` starts playback of a media URL:

```json
{
  "type": "LOAD",
  "requestId": 2,
  "media": {
    "contentId": "http://<PC-IP>:8080/stream",
    "contentType": "video/mp4",
    "streamType": "BUFFERED"
  },
  "autoplay": true,
  "currentTime": 0
}
```

`streamType` SHALL be `BUFFERED` for local-file and proxied-URL sources and `LIVE` for the screen-capture source.

Transport controls:

- `PLAY` — `{"type":"PLAY","requestId":N}`;
- `PAUSE` — `{"type":"PAUSE","requestId":N}`;
- `STOP` — `{"type":"STOP","requestId":N}`.

The engine SHALL parse `MEDIA_STATUS` responses and extract at minimum the `playerState` (`IDLE`, `PLAYING`, `PAUSED`, `BUFFERING`) and `idleReason` fields.

## 7. Receiver lifecycle

At minimum:

```text
Discover
  -> Select
  -> Connect TCP
  -> Wrap TLS
  -> CONNECT
  -> Start heartbeat
  -> LAUNCH Default Media Receiver
  -> Await RECEIVER_STATUS; extract transportId/sessionId
  -> LOAD media URL
  -> Await MEDIA_STATUS
  -> Send subsequent media/control commands
  -> Teardown: STOP -> STOP_APP -> close_notify -> close socket
```

### 7.1 Disconnect and reconnection

- Network loss or a missed heartbeat SHALL close the TLS connection and attempt reconnection with exponential backoff: 1 s, 2 s, 4 s, ..., capped at 30 s, with a maximum of 5 attempts.
- After the cap is exhausted, the engine SHALL surface an error to the GUI and wait for the user to re-select the receiver.
- Reconnection SHALL restart the transport lifecycle from `Connect TCP`: TLS handshake, `CONNECT`, and heartbeat/watchdog timers resume, and the pending-request map is reset. No fresh `LAUNCH` is issued — a reconnected session returns to the `Connected` phase and the caller re-issues its command (the queued `LOAD` is not replayed after a mid-playback blip).

## 8. Engine acceptance criteria

- [x] Receiver discovery uses the specified multicast address and service query.
- [x] Receiver IP/port/name can be extracted from DNS records.
- [x] DNS responses with compression pointers parse correctly.
- [x] Malformed DNS packets are discarded without panicking.
- [x] TCP connection can be wrapped with `rustls`.
- [x] Self-signed receiver certificates are accepted as specified.
- [x] CastV2 frames contain a big-endian 4-byte length prefix.
- [x] The decoder reads the length prefix and the exact payload length, rejecting frames over 16 MiB.
- [x] CastMessage serialization does not depend on `prost`.
- [x] CastMessage deserialization tolerates unknown fields.
- [x] Connection namespace sends `CONNECT`.
- [x] Heartbeat sends `PING` every 5 seconds and a `PONG` resets the timer.
- [x] Receiver namespace can issue `LAUNCH` for `CC1AD845`.
- [x] `RECEIVER_STATUS` `transportId` is used as the media destination ID.
- [x] Media namespace can send `LOAD` with the proxy URL and `PLAY`/`PAUSE`/`STOP`.
- [x] Every request carries a `requestId` and responses are correlated to it.
