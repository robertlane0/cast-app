#![forbid(unsafe_code)]

//! UDP multicast receiver discovery over mDNS (`_googlecast._tcp.local`) with
//! a hand-rolled DNS packet parser. Owned by `03-cast-engine.md` §2.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::time::Duration;

use thiserror::Error;
use tokio::sync::{mpsc::UnboundedSender, watch};

use crate::state::{BackendEvent, CastDevice};
use crate::util::shutdown::Shutdown;

/// IPv4 mDNS multicast address (`03-cast-engine.md` §2.1).
pub const MDNS_ADDR: Ipv4Addr = Ipv4Addr::new(224, 0, 0, 251);
/// mDNS UDP port (`03-cast-engine.md` §2.1).
pub const MDNS_PORT: u16 = 5353;
/// Service query target for Google Cast devices.
pub const GOOGLECAST_SERVICE: &str = "_googlecast._tcp.local";
/// Interval between query cycles (`03-cast-engine.md` §2.2).
pub const REQUERY_INTERVAL: Duration = Duration::from_secs(10);
/// Missed query cycles before a device expires (`03-cast-engine.md` §2.5).
pub const MISSED_CYCLES_TO_EXPIRE: u8 = 3;
/// Maximum DNS compression-pointer hops while decoding a name
/// (`03-cast-engine.md` §2.3).
pub const MAX_POINTER_DEPTH: usize = 4;

/// UDP receive buffer size in bytes. mDNS responses with several receivers
/// stay far below this.
const RECEIVE_BUFFER_SIZE: usize = 4096;

const TYPE_A: u16 = 1;
const TYPE_PTR: u16 = 12;
const TYPE_TXT: u16 = 16;
const TYPE_SRV: u16 = 33;

/// Errors produced while parsing a DNS packet. Parsing never panics;
/// malformed packets surface as an error and are discarded by the caller.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DnsError {
    #[error("packet too short: only {0} bytes available")]
    Truncated(usize),
    #[error("compression pointer targets offset {0}, past the end of the packet")]
    PointerOutOfBounds(usize),
    #[error("compression pointer cycle detected")]
    PointerCycle,
    #[error("compression pointer depth limit ({MAX_POINTER_DEPTH}) exceeded")]
    PointerDepthExceeded,
    #[error("invalid label length byte 0x{0:02x}")]
    BadLabelLength(u8),
}

/// A parsed DNS record; the rdata is decoded for the record types the Cast
/// engine consumes and ignored for everything else (`03-cast-engine.md` §2.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsRecord {
    /// Owner name of the record.
    pub owner: String,
    /// DNS record type.
    pub record_type: u16,
    /// Time-to-live in seconds.
    pub ttl: u32,
    /// Decoded record data.
    pub rdata: RecordData,
}

/// Decoded rdata for the record types consumed by discovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordData {
    /// PTR target name (the service instance name).
    Ptr(String),
    /// SRV service data.
    Srv {
        priority: u16,
        weight: u16,
        port: u16,
        target: String,
    },
    /// TXT strings, still length-prefixed.
    Txt(Vec<Vec<u8>>),
    /// IPv4 A record.
    A(Ipv4Addr),
    /// Unsupported record type; ignored.
    Other,
}

/// A device fully described by one correlated response
/// (`03-cast-engine.md` §2.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredDevice {
    pub ip: Ipv4Addr,
    pub port: u16,
    pub name: String,
}

/// Build the mDNS PTR query for `_googlecast._tcp.local`
/// (`03-cast-engine.md` §2.2): query ID 0, recursion-desired cleared,
/// one question, type PTR, class IN.
pub fn build_ptr_query() -> Vec<u8> {
    let mut out = Vec::with_capacity(41);
    out.extend_from_slice(&[0x00, 0x00]); // ID = 0
    out.extend_from_slice(&[0x00, 0x00]); // flags: RD cleared
    out.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT = 1
    out.extend_from_slice(&[0, 0, 0, 0, 0, 0]); // AN/NS/AR = 0
    out.extend_from_slice(&name_bytes(GOOGLECAST_SERVICE));
    out.extend_from_slice(&TYPE_PTR.to_be_bytes());
    out.extend_from_slice(&1u16.to_be_bytes()); // class IN
    out
}

/// Bind the discovery socket to `0.0.0.0:0`, join the mDNS multicast group
/// and switch to non-blocking mode (`03-cast-engine.md` §2.1).
///
/// Setup failures (e.g. multicast join) are returned to the caller, which
/// surfaces them to the GUI as a `ConnectionError` (`03-cast-engine.md` §2.5).
pub fn bind_socket() -> std::io::Result<UdpSocket> {
    let socket = UdpSocket::bind(("0.0.0.0", 0))?;
    socket.join_multicast_v4(&MDNS_ADDR, &Ipv4Addr::UNSPECIFIED)?;
    socket.set_nonblocking(true)?;
    Ok(socket)
}

/// Discovery loop: re-query every [`REQUERY_INTERVAL`], parse incoming
/// responses, maintain the device table with dedup by IP:port and expiry
/// after [`MISSED_CYCLES_TO_EXPIRE`] missed cycles, and push snapshots to the
/// GUI via `BackendEvent::ReceiversUpdated` (`03-cast-engine.md` §2.5).
///
/// `rescan` triggers an immediate re-query outside the interval cadence
/// (the GUI Error-state retry action, `02-gui.md` §3.1).
///
/// Send/receive errors are logged and the loop continues.
pub async fn run(
    socket: std::net::UdpSocket,
    shutdown: Shutdown,
    event_tx: UnboundedSender<BackendEvent>,
    rescan: watch::Receiver<u8>,
) {
    run_with_dest(
        socket,
        shutdown,
        event_tx,
        rescan,
        SocketAddr::new(IpAddr::V4(MDNS_ADDR), MDNS_PORT),
    )
    .await
}

/// Same as [`run`] with an explicit query destination. Production always
/// targets the mDNS multicast group; unit tests point the query at a local
/// sniffer socket so the rescan path is observable without multicast.
pub(crate) async fn run_with_dest(
    socket: std::net::UdpSocket,
    shutdown: Shutdown,
    event_tx: UnboundedSender<BackendEvent>,
    mut rescan: watch::Receiver<u8>,
    query_dest: SocketAddr,
) {
    let socket = match tokio::net::UdpSocket::from_std(socket) {
        Ok(socket) => socket,
        Err(err) => {
            tracing::error!("failed to convert mDNS socket to tokio: {err}");
            return;
        }
    };

    let mut devices: HashMap<(Ipv4Addr, u16), DeviceEntry> = HashMap::new();
    let mut shutdown_rx = shutdown.subscribe();
    let mut interval = tokio::time::interval(REQUERY_INTERVAL);
    let mut buf = vec![0u8; RECEIVE_BUFFER_SIZE];

    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
            _ = interval.tick() => {
                if let Err(err) = socket.send_to(&build_ptr_query(), query_dest).await {
                    tracing::warn!("mDNS query send failed: {err}");
                }
                expire_cycle(&mut devices, &event_tx);
            }
            changed = rescan.changed() => {
                if changed.is_err() {
                    // The rescan sender is gone (supervisor exited): stop.
                    break;
                }
                if let Err(err) = socket.send_to(&build_ptr_query(), query_dest).await {
                    tracing::warn!("mDNS rescan query send failed: {err}");
                }
            }
            result = socket.recv_from(&mut buf) => {
                match result {
                    Ok((len, _src)) => match parse_packet(&buf[..len]) {
                        Ok(found) => {
                            let mut changed = false;
                            for device in found {
                                changed |= upsert_device(&mut devices, device);
                            }
                            if changed {
                                push_snapshot(&devices, &event_tx);
                            }
                        }
                        Err(err) => tracing::warn!("discarding malformed mDNS packet: {err}"),
                    },
                    Err(err) => tracing::warn!("mDNS receive error: {err}"),
                }
            }
        }
    }
}

/// Parse a DNS response packet and return the fully correlated devices it
/// describes (`03-cast-engine.md` §2.3–2.4).
///
/// All sections (answer, authority, additional) are parsed; records of
/// unsupported types are skipped. Malformed packets return an error and never
/// panic.
pub fn parse_packet(data: &[u8]) -> Result<Vec<DiscoveredDevice>, DnsError> {
    let mut parser = Parser::new(data);

    // 12-byte header (ID, flags, QDCOUNT, ANCOUNT, NSCOUNT, ARCOUNT).
    let _id = parser.read_u16()?;
    let _flags = parser.read_u16()?;
    let qdcount = parser.read_u16()?;
    let ancount = parser.read_u16()?;
    let nscount = parser.read_u16()?;
    let arcount = parser.read_u16()?;

    // Question section.
    for _ in 0..qdcount {
        let _name = parser.read_name()?;
        let _qtype = parser.read_u16()?;
        let _qclass = parser.read_u16()?;
    }

    // Answer, authority and additional sections.
    let mut records = Vec::new();
    for _ in 0..(ancount as u32 + nscount as u32 + arcount as u32) {
        records.push(parser.read_record()?);
    }

    Ok(correlate(&records))
}

/// Correlate PTR / SRV / TXT / A records of the same response into devices
/// (`03-cast-engine.md` §2.4).
///
/// - PTR answers for `_googlecast._tcp.local` name the service instances;
/// - SRV (owner = instance) supplies the port and target hostname;
/// - TXT (owner = instance) supplies the friendly name via `fn=`;
/// - A (owner = SRV target, or the instance name as fallback) supplies the IP.
///
/// Instances missing an SRV or A record are skipped; IPv6 `AAAA` records are
/// ignored (discovery is IPv4-only).
pub fn correlate(records: &[DnsRecord]) -> Vec<DiscoveredDevice> {
    let mut instances: Vec<String> = Vec::new();
    let mut srv_by_instance: HashMap<String, &DnsRecord> = HashMap::new();
    let mut txt_by_instance: HashMap<String, &DnsRecord> = HashMap::new();
    let mut a_by_host: HashMap<String, &DnsRecord> = HashMap::new();

    for record in records {
        match &record.rdata {
            RecordData::Ptr(instance) if record.owner.eq_ignore_ascii_case(GOOGLECAST_SERVICE) => {
                instances.push(instance.clone());
            }
            RecordData::Srv { .. } => {
                srv_by_instance.insert(record.owner.to_ascii_lowercase(), record);
            }
            RecordData::Txt(_) => {
                txt_by_instance.insert(record.owner.to_ascii_lowercase(), record);
            }
            RecordData::A(_) => {
                a_by_host.insert(record.owner.to_ascii_lowercase(), record);
            }
            _ => {}
        }
    }

    let mut devices = Vec::new();
    for instance in instances {
        let instance_key = instance.to_ascii_lowercase();
        let Some(srv) = srv_by_instance.get(&instance_key) else {
            continue;
        };
        let RecordData::Srv { port, target, .. } = &srv.rdata else {
            continue;
        };
        let a = a_by_host
            .get(&target.to_ascii_lowercase())
            .or_else(|| a_by_host.get(&instance_key));
        let Some(a) = a else {
            continue;
        };
        let RecordData::A(ip) = a.rdata else {
            continue;
        };

        let name = txt_by_instance
            .get(&instance_key)
            .and_then(|record| friendly_name_from_txt(record))
            .unwrap_or_else(|| instance_label(&instance));

        devices.push(DiscoveredDevice {
            ip,
            port: *port,
            name,
        });
    }

    devices
}

/// Extract the `fn=` value from a TXT record (`03-cast-engine.md` §2.4).
fn friendly_name_from_txt(record: &DnsRecord) -> Option<String> {
    let RecordData::Txt(strings) = &record.rdata else {
        return None;
    };
    for entry in strings {
        let Some(equals) = entry.iter().position(|&byte| byte == b'=') else {
            continue;
        };
        if entry[..equals].eq_ignore_ascii_case(b"fn") {
            return Some(String::from_utf8_lossy(&entry[equals + 1..]).into_owned());
        }
    }
    None
}

/// The first label of an instance name, used as the friendly-name fallback.
fn instance_label(instance: &str) -> String {
    instance.split('.').next().unwrap_or(instance).to_owned()
}

/// Encode a domain name as length-prefixed labels with a terminating zero.
fn name_bytes(name: &str) -> Vec<u8> {
    let mut out = Vec::new();
    for label in name.split('.') {
        // GOOGLECAST_SERVICE labels are compile-time constants below 64 bytes.
        assert!(label.len() <= 63, "DNS label longer than 63 bytes");
        out.push(label.len() as u8);
        out.extend_from_slice(label.as_bytes());
    }
    out.push(0);
    out
}

/// One device in the discovery table, tracking missed query cycles.
#[derive(Debug)]
struct DeviceEntry {
    name: String,
    missed_cycles: u8,
}

/// Insert or refresh a discovered device; returns whether the table changed.
/// De-duplication is by (IP, port) (`03-cast-engine.md` §2.5).
fn upsert_device(
    devices: &mut HashMap<(Ipv4Addr, u16), DeviceEntry>,
    device: DiscoveredDevice,
) -> bool {
    let key = (device.ip, device.port);
    match devices.get_mut(&key) {
        Some(entry) => {
            entry.missed_cycles = 0;
            if entry.name != device.name {
                entry.name = device.name;
                true
            } else {
                false
            }
        }
        None => {
            devices.insert(
                key,
                DeviceEntry {
                    name: device.name,
                    missed_cycles: 0,
                },
            );
            true
        }
    }
}

/// Advance one query cycle: devices not re-announced for
/// [`MISSED_CYCLES_TO_EXPIRE`] cycles are removed (`03-cast-engine.md` §2.5).
fn expire_cycle(
    devices: &mut HashMap<(Ipv4Addr, u16), DeviceEntry>,
    event_tx: &UnboundedSender<BackendEvent>,
) {
    if devices.is_empty() {
        return;
    }
    let mut changed = false;
    devices.retain(|_, entry| {
        entry.missed_cycles += 1;
        if entry.missed_cycles >= MISSED_CYCLES_TO_EXPIRE {
            changed = true;
            false
        } else {
            true
        }
    });
    if changed {
        push_snapshot(devices, event_tx);
    }
}

/// Push the authoritative receiver list to the GUI as a sorted snapshot.
fn push_snapshot(
    devices: &HashMap<(Ipv4Addr, u16), DeviceEntry>,
    event_tx: &UnboundedSender<BackendEvent>,
) {
    let mut receivers: Vec<CastDevice> = devices
        .iter()
        .map(|((ip, port), entry)| CastDevice {
            id: format!("{ip}:{port}"),
            name: entry.name.clone(),
            addr: SocketAddr::new(IpAddr::V4(*ip), *port),
        })
        .collect();
    receivers.sort_by(|a, b| a.id.cmp(&b.id));
    let _ = event_tx.send(BackendEvent::ReceiversUpdated(receivers));
}

/// Cursor over a DNS packet with bounds-checked reads.
struct Parser<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn read_u8(&mut self) -> Result<u8, DnsError> {
        if self.pos >= self.data.len() {
            return Err(DnsError::Truncated(self.data.len()));
        }
        let byte = self.data[self.pos];
        self.pos += 1;
        Ok(byte)
    }

    fn read_u16(&mut self) -> Result<u16, DnsError> {
        let hi = self.read_u8()? as u16;
        let lo = self.read_u8()? as u16;
        Ok((hi << 8) | lo)
    }

    fn read_u32(&mut self) -> Result<u32, DnsError> {
        let a = self.read_u8()? as u32;
        let b = self.read_u8()? as u32;
        let c = self.read_u8()? as u32;
        let d = self.read_u8()? as u32;
        Ok((a << 24) | (b << 16) | (c << 8) | d)
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], DnsError> {
        if len > self.data.len() - self.pos {
            return Err(DnsError::Truncated(self.data.len()));
        }
        let slice = &self.data[self.pos..self.pos + len];
        self.pos += len;
        Ok(slice)
    }

    /// Decode a (possibly compressed) domain name, following compression
    /// pointers with a depth limit and a cycle guard (`03-cast-engine.md` §2.3).
    fn read_name(&mut self) -> Result<String, DnsError> {
        let mut labels: Vec<String> = Vec::new();
        self.read_name_into(&mut labels, 0, &mut Vec::new())?;
        Ok(labels.join("."))
    }

    fn read_name_into(
        &mut self,
        labels: &mut Vec<String>,
        depth: usize,
        visited: &mut Vec<usize>,
    ) -> Result<(), DnsError> {
        loop {
            let byte = self.read_u8()?;
            match byte {
                0 => return Ok(()),
                b if b & 0xC0 == 0xC0 => {
                    if depth >= MAX_POINTER_DEPTH {
                        return Err(DnsError::PointerDepthExceeded);
                    }
                    let offset = (((b & 0x3F) as usize) << 8) | self.read_u8()? as usize;
                    if offset >= self.data.len() {
                        return Err(DnsError::PointerOutOfBounds(offset));
                    }
                    if visited.contains(&offset) {
                        return Err(DnsError::PointerCycle);
                    }
                    visited.push(offset);
                    let saved = self.pos;
                    self.pos = offset;
                    let result = self.read_name_into(labels, depth + 1, visited);
                    self.pos = saved;
                    return result;
                }
                b if b & 0xC0 != 0 => return Err(DnsError::BadLabelLength(b)),
                len => {
                    let label = String::from_utf8_lossy(self.take(len as usize)?).into_owned();
                    labels.push(label);
                }
            }
        }
    }

    /// Decode one resource record. The rdata is decoded with a sub-parser so
    /// compression pointers inside it resolve against the whole packet.
    fn read_record(&mut self) -> Result<DnsRecord, DnsError> {
        let owner = self.read_name()?;
        let record_type = self.read_u16()?;
        let _class = self.read_u16()?;
        let ttl = self.read_u32()?;
        let rdlength = self.read_u16()? as usize;
        let rdata_start = self.pos;
        self.take(rdlength)?;

        let rdata = match record_type {
            TYPE_PTR => {
                let mut parser = Parser::new(self.data);
                parser.pos = rdata_start;
                RecordData::Ptr(parser.read_name()?)
            }
            TYPE_SRV => {
                let mut parser = Parser::new(self.data);
                parser.pos = rdata_start;
                RecordData::Srv {
                    priority: parser.read_u16()?,
                    weight: parser.read_u16()?,
                    port: parser.read_u16()?,
                    target: parser.read_name()?,
                }
            }
            TYPE_TXT => {
                let mut parser = Parser::new(self.data);
                parser.pos = rdata_start;
                let mut strings = Vec::new();
                while parser.pos < rdata_start + rdlength {
                    let len = parser.read_u8()? as usize;
                    strings.push(parser.take(len)?.to_vec());
                }
                RecordData::Txt(strings)
            }
            TYPE_A => {
                let mut parser = Parser::new(self.data);
                parser.pos = rdata_start;
                let octets = parser.take(4)?;
                RecordData::A(Ipv4Addr::new(octets[0], octets[1], octets[2], octets[3]))
            }
            _ => RecordData::Other,
        };

        Ok(DnsRecord {
            owner,
            record_type,
            ttl,
            rdata,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// A rescan bump must trigger an immediate PTR query (the GUI Error-state
    /// retry action, `02-gui.md` §3.1), outside the 10 s interval cadence.
    /// The query destination is pointed at a local sniffer socket so the
    /// test observes the wire without multicast.
    #[tokio::test]
    async fn rescan_bump_triggers_an_immediate_query() {
        let control = UdpSocket::bind(("127.0.0.1", 0)).expect("bind control");
        control.set_nonblocking(true).expect("nonblocking");
        let sniffer = UdpSocket::bind(("127.0.0.1", 0)).expect("bind sniffer");
        sniffer.set_nonblocking(true).expect("nonblocking");
        let dest = sniffer.local_addr().expect("sniffer addr");

        let (rescan_tx, rescan_rx) = watch::channel(0u8);
        let (event_tx, _event_rx) = tokio::sync::mpsc::unbounded_channel();
        let shutdown = Shutdown::new();
        let task = tokio::spawn(run_with_dest(
            control,
            shutdown.clone(),
            event_tx,
            rescan_rx,
            dest,
        ));

        // Before any tick (10 s) or rescan, nothing has been sent.
        let mut buf = [0u8; 4096];
        assert!(
            sniffer.recv_from(&mut buf).is_err(),
            "no query before the first tick or rescan"
        );

        rescan_tx.send(1).expect("rescan send");
        let (len, _) = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match sniffer.recv_from(&mut buf) {
                    Ok(result) => return result,
                    Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                }
            }
        })
        .await
        .expect("rescan must trigger an immediate query");
        assert_eq!(
            &buf[..len],
            &build_ptr_query(),
            "rescan sends the PTR query"
        );

        shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("discovery loop exits on shutdown")
            .expect("discovery loop did not panic");
    }
}
