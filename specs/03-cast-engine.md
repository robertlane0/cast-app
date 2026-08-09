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
3. Send a DNS query for the PTR record `_googlecast._tcp.local`.

### 2.2 Response parsing

The engine SHALL safely parse binary DNS response packets and extract:

- receiver IP address from an `A` record;
- receiver port from an `SRV` record;
- friendly receiver name from a `TXT` record.

The overview states that the receiver port is usually `8009`.

The complete DNS packet grammar, compression-pointer handling, malformed-packet policy, TTL handling and refresh strategy are **TBD**.

## 3. TLS transport

The Cast receiver connection SHALL:

- use TCP;
- connect on the discovered receiver port, normally `8009`;
- use `rustls`;
- accept the receiver's self-signed certificate through a custom certificate verifier.

The overview explicitly describes certificate validation as non-strict because Cast devices use self-signed certificates.

Exact `rustls` API/version and certificate-verifier implementation details are **TBD**.

## 4. CastV2 framing

Every CastV2 message SHALL be framed as:

```text
4-byte big-endian u32 payload length
+
CastMessage Protobuf payload
```

The engine SHALL hand-roll the required Protobuf serialization.

## 5. CastMessage encoding

The encoder SHALL represent the CastMessage fields used by the application, including:

- protocol version
- source ID
- destination ID
- namespace
- payload type
- UTF-8 payload

The overview identifies:

- protocol version `0` as `CASTV2_1_0`;
- payload type `0` as `STRING`.

A representative encoder shape is:

```rust
fn encode_cast_message(
    source: &str,
    dest: &str,
    namespace: &str,
    payload: &str,
) -> Vec<u8>
```

The exact complete field schema, integer encoding helpers, maximum frame size and decoder requirements are **TBD**.

## 6. Namespaces

The engine SHALL support these namespaces.

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

The engine SHALL process the corresponding receiver heartbeat traffic. Exact PONG validation and timeout/reconnect policy are **TBD**.

### 6.3 Receiver

Namespace:

`urn:x-cast:com.google.cast.receiver`

The engine SHALL send a `LAUNCH` command for the Default Media Receiver App ID:

```text
CC1AD845
```

The complete receiver command JSON schema and request IDs are **TBD**.

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
  -> Send subsequent media/control commands
```

Exact disconnect, reconnect, receiver-status synchronization and shutdown behavior are **TBD**.

## 8. Engine acceptance criteria

- [ ] Receiver discovery uses the specified multicast address and service query.
- [ ] Receiver IP/port/name can be extracted from DNS records.
- [ ] TCP connection can be wrapped with `rustls`.
- [ ] Self-signed receiver certificates are accepted as specified.
- [ ] CastV2 frames contain a big-endian 4-byte length prefix.
- [ ] CastMessage serialization does not depend on `prost`.
- [ ] Connection namespace sends `CONNECT`.
- [ ] Heartbeat sends `PING` every 5 seconds.
- [ ] Receiver namespace can issue `LAUNCH` for `CC1AD845`.
