//! Minecraft wire-protocol parsing/encoding: everything here is pure format logic (packets, NBT,
//! text components, PROXY protocol headers) with no app state and no I/O of its own - callers in
//! `net`/`udp` do the actual reading and writing.

pub mod compression;
pub mod handshake;
pub mod minecraft_protocol;
pub mod nbt;
pub mod proxy_protocol_datagram;
pub mod proxy_protocol_tcp;
pub mod reconnect_protocol;
pub mod status_json;
pub mod text_format;
pub mod varint;
