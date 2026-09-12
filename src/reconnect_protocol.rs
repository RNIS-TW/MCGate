//! Port of `protocol/ReconnectProtocol.kt` — packet IDs and encoders for the auto-reconnect
//! holding state (section 3's not-yet-ported `ReconnectHandler`).
//!
//! Minecraft's Configuration/Play packet IDs are NOT stable across versions in general — unlike
//! the handshake/status packets in `minecraft_protocol.rs`, this table only covers verified
//! protocol brackets and must be extended (a new entry in `BRACKETS`) to support others.
//!
//! IDs below were confirmed against the current minecraft.wiki protocol packet reference at
//! implementation time (ported verbatim from the Kotlin table, not re-verified independently).
//! The chunk data encoding is best-effort from protocol knowledge and was NOT verified against a
//! live client — test against a real client before relying on this in production.

use uuid::Uuid;

use crate::compression::frame;
use crate::nbt::write_root_compound;
use crate::varint::{write_string, write_var_int};

#[derive(Debug, Clone, Copy)]
pub struct ReconnectPacketIds {
    pub login_success: i32,
    pub config_finish_configuration: i32,
    pub config_keep_alive: i32,
    #[allow(dead_code)]
    pub config_ack_finish_configuration: i32,
    pub play_login: i32,
    pub play_keep_alive_clientbound: i32,
    #[allow(dead_code)]
    pub play_keep_alive_serverbound: i32,
    pub play_set_action_bar_text: i32,
    pub play_set_title_text: i32,
    pub play_set_subtitle_text: i32,
    pub play_set_center_chunk: i32,
    pub play_chunk_data_and_update_light: i32,
    pub play_start_configuration: i32,
    #[allow(dead_code)]
    pub play_ack_configuration: i32,
    pub play_disconnect: i32,
    /// Play-state clientbound `transfer` — tells the client to close this connection and open a
    /// brand-new one directly to a given host:port. Used by the console `transfer` command;
    /// unrelated to the auto-reconnect-hold feature this table otherwise serves, same as
    /// `play_disconnect` (reused purely as framing infrastructure for the `kick` command).
    pub play_transfer: i32,
}

// Protocol 774 (1.21.11), 775 (26.1/26.1.1/26.1.2), and 776 (26.2) share this exact packet-ID
// table — cross-checked against minecraft.wiki's Protocol History changelog, which lists every
// packet ID change release to release: from protocol 767 (1.21) through 776 (26.2) there is no
// recorded ID change for any packet used here. Extend BRACKETS with additional protocol ->
// ReconnectPacketIds entries for other brackets once similarly verified — don't assume older
// versions match without checking, since packet IDs do shift release to release in general.
const CURRENT_BRACKET: ReconnectPacketIds = ReconnectPacketIds {
    login_success: 0x02,
    config_finish_configuration: 0x03,
    config_keep_alive: 0x04,
    config_ack_finish_configuration: 0x03,
    play_login: 0x31,
    play_keep_alive_clientbound: 0x2C,
    play_keep_alive_serverbound: 0x1C,
    play_set_action_bar_text: 0x57,
    play_set_title_text: 0x72,
    play_set_subtitle_text: 0x70,
    play_set_center_chunk: 0x5E,
    play_chunk_data_and_update_light: 0x2D,
    play_start_configuration: 0x76,
    play_ack_configuration: 0x10,
    play_disconnect: 0x20,
    play_transfer: 0x81,
};

fn bracket_for(protocol_version: i32) -> Option<ReconnectPacketIds> {
    match protocol_version {
        774 | 775 | 776 => Some(CURRENT_BRACKET),
        _ => None,
    }
}

/// Whether auto-reconnect can serve this protocol version — i.e. we have a verified packet-ID
/// bracket for it.
pub fn reconnect_supported(protocol_version: i32) -> bool {
    bracket_for(protocol_version).is_some()
}

pub fn reconnect_packet_ids(protocol_version: i32) -> ReconnectPacketIds {
    bracket_for(protocol_version).unwrap_or_else(|| panic!("No auto-reconnect packet table for protocol {protocol_version}"))
}

fn uuid_bits(uuid: Uuid) -> (i64, i64) {
    let bytes = uuid.into_bytes();
    (i64::from_be_bytes(bytes[0..8].try_into().unwrap()), i64::from_be_bytes(bytes[8..16].try_into().unwrap()))
}

/// Login state (packet 0x00), stable across every protocol version. Always sent before any
/// compression is negotiated (Login state), so no `compression_threshold` parameter needed.
/// Takes an already-rendered rich-text string (`to_json_component` applied by the caller) rather
/// than parsing it here — the caller decides whether that's a one-off ad-hoc message or a static
/// config value pre-rendered once and reused.
pub fn encode_login_disconnect(rendered_reason_json: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    write_var_int(&mut payload, 0x00);
    write_string(&mut payload, rendered_reason_json);
    frame(&payload, -1)
}

/// Login state clientbound `login_finished` (formerly "Login Success"). As of the currently
/// supported protocol bracket this has two UUID fields: the Game Profile's id, and a separate
/// trailing Session ID. Session ID's exact semantics aren't verified; reusing the player's UUID
/// is an opaque but harmless placeholder since MCGate's synthetic reconnect-wait login has no
/// real session to report.
pub fn encode_login_success(ids: &ReconnectPacketIds, uuid: Uuid, username: &str, compression_threshold: i32) -> Vec<u8> {
    let mut payload = Vec::new();
    write_var_int(&mut payload, ids.login_success);
    let (most, least) = uuid_bits(uuid);
    payload.extend_from_slice(&most.to_be_bytes()); // Game Profile: id
    payload.extend_from_slice(&least.to_be_bytes());
    write_string(&mut payload, username); // Game Profile: name
    write_var_int(&mut payload, 0); // Game Profile: properties (none)
    payload.extend_from_slice(&most.to_be_bytes()); // Session ID
    payload.extend_from_slice(&least.to_be_bytes());
    frame(&payload, compression_threshold)
}

pub fn encode_finish_configuration(ids: &ReconnectPacketIds, compression_threshold: i32) -> Vec<u8> {
    let mut payload = Vec::new();
    write_var_int(&mut payload, ids.config_finish_configuration);
    frame(&payload, compression_threshold)
}

pub fn encode_config_keep_alive(ids: &ReconnectPacketIds, id: i64, compression_threshold: i32) -> Vec<u8> {
    let mut payload = Vec::new();
    write_var_int(&mut payload, ids.config_keep_alive);
    payload.extend_from_slice(&id.to_be_bytes());
    frame(&payload, compression_threshold)
}

pub fn encode_play_keep_alive(ids: &ReconnectPacketIds, id: i64, compression_threshold: i32) -> Vec<u8> {
    let mut payload = Vec::new();
    write_var_int(&mut payload, ids.play_keep_alive_clientbound);
    payload.extend_from_slice(&id.to_be_bytes());
    frame(&payload, compression_threshold)
}

pub fn encode_start_configuration(ids: &ReconnectPacketIds, compression_threshold: i32) -> Vec<u8> {
    let mut payload = Vec::new();
    write_var_int(&mut payload, ids.play_start_configuration);
    frame(&payload, compression_threshold)
}

/// Play state (not Login) Disconnect — used to kick a player already waiting for reconnect, since
/// by that point the client is well past Login state and a Login-state Disconnect packet would be
/// rejected as unexpected. Takes already-rendered legacy text (`to_legacy_text` applied by the
/// caller).
pub fn encode_play_disconnect(ids: &ReconnectPacketIds, rendered_reason_text: &str, compression_threshold: i32) -> Vec<u8> {
    let mut payload = Vec::new();
    write_var_int(&mut payload, ids.play_disconnect);
    payload.push(crate::nbt::STRING);
    crate::nbt::write_nbt_string(&mut payload, rendered_reason_text);
    frame(&payload, compression_threshold)
}

/// Play state clientbound `transfer` (id 0x81 as of protocol 776/26.2). Tells the client to close
/// this connection and open a brand-new one directly to `host`:`port` — MCGate is no longer in
/// that new connection's path at all. Port is an unsigned short, not a VarInt, per the packet's
/// wire format.
pub fn encode_transfer(ids: &ReconnectPacketIds, host: &str, port: u16, compression_threshold: i32) -> Vec<u8> {
    let mut payload = Vec::new();
    write_var_int(&mut payload, ids.play_transfer);
    write_string(&mut payload, host);
    payload.extend_from_slice(&port.to_be_bytes());
    frame(&payload, compression_threshold)
}

/// Minimal Play-state "Login" (Join Game) packet spawning the client into a held, empty world
/// while waiting to reconnect. Reuses the client's own built-in `minecraft:overworld` dimension
/// type rather than registering a custom one. Field list is best-effort against the current
/// protocol's Login(play) packet — the riskiest unverified part of the join sequence (see file
/// header).
pub fn encode_join_game(ids: &ReconnectPacketIds, entity_id: i32, compression_threshold: i32) -> Vec<u8> {
    let mut payload = Vec::new();
    write_var_int(&mut payload, ids.play_login);
    payload.extend_from_slice(&entity_id.to_be_bytes());
    payload.push(0); // is hardcore
    write_var_int(&mut payload, 1); // dimension names count
    write_string(&mut payload, "minecraft:overworld");
    write_var_int(&mut payload, 20); // max players (unused by client, vanilla ignores)
    write_var_int(&mut payload, 2); // view distance
    write_var_int(&mut payload, 2); // simulation distance
    payload.push(0); // reduced debug info
    payload.push(0); // enable respawn screen
    payload.push(0); // do limited crafting
    write_string(&mut payload, "minecraft:overworld"); // dimension type (by identifier)
    write_string(&mut payload, "minecraft:overworld"); // dimension name
    payload.extend_from_slice(&0i64.to_be_bytes()); // hashed seed
    write_var_int(&mut payload, 3); // gamemode: spectator, so gravity/void death don't apply
    payload.push(0xFF); // previous gamemode: none (-1 as unsigned byte)
    payload.push(0); // is debug
    payload.push(1); // is flat
    payload.push(0); // has death location
    write_var_int(&mut payload, 0); // portal cooldown
    write_var_int(&mut payload, 0); // sea level
    payload.push(0); // enforces secure chat
    frame(&payload, compression_threshold)
}

pub fn encode_set_center_chunk(ids: &ReconnectPacketIds, compression_threshold: i32, chunk_x: i32, chunk_z: i32) -> Vec<u8> {
    let mut payload = Vec::new();
    write_var_int(&mut payload, ids.play_set_center_chunk);
    write_var_int(&mut payload, chunk_x);
    write_var_int(&mut payload, chunk_z);
    frame(&payload, compression_threshold)
}

/// A single empty (all-air) chunk column at (`chunk_x`, `chunk_z`) with one section, no light
/// data, so the client stops showing "Loading terrain..." at the held spawn point.
pub fn encode_empty_chunk(ids: &ReconnectPacketIds, compression_threshold: i32, chunk_x: i32, chunk_z: i32) -> Vec<u8> {
    let mut payload = Vec::new();
    write_var_int(&mut payload, ids.play_chunk_data_and_update_light);
    payload.extend_from_slice(&chunk_x.to_be_bytes());
    payload.extend_from_slice(&chunk_z.to_be_bytes());

    // Heightmaps: empty NBT compound (no heightmaps present).
    write_root_compound(&mut payload, |_| {});

    // Chunk section data: one section, all air.
    let mut section = Vec::new();
    section.extend_from_slice(&0i16.to_be_bytes()); // block count = 0 (all air)
    // Block states: single-value paletted container, bits-per-entry 0 -> palette-only, value = air (0).
    section.push(0);
    write_var_int(&mut section, 0); // palette entry: air block state id 0
    write_var_int(&mut section, 0); // data array length 0
    // Biomes: single-value paletted container, biome id 0 (plains, arbitrary).
    section.push(0);
    write_var_int(&mut section, 0);
    write_var_int(&mut section, 0);

    write_var_int(&mut payload, section.len() as i32);
    payload.extend_from_slice(&section);

    write_var_int(&mut payload, 0); // block entities: none

    // Light data: no sections lit, empty masks/arrays.
    write_var_int(&mut payload, 0); // sky light mask longs
    write_var_int(&mut payload, 0); // block light mask longs
    write_var_int(&mut payload, 0); // empty sky light mask longs
    write_var_int(&mut payload, 0); // empty block light mask longs
    write_var_int(&mut payload, 0); // sky light array count
    write_var_int(&mut payload, 0); // block light array count

    frame(&payload, compression_threshold)
}

/// Action bar text (Play state). 1.20.3+ text components are network NBT rather than JSON; a
/// bare NBT string tag is valid as a text component. Takes already-rendered legacy text — this is
/// on the animation timer's hot path (fires every `animationInterval`, 500ms by default, for
/// every player waiting to reconnect); callers should render once and reuse the result, not parse
/// per-send.
pub fn encode_action_bar(ids: &ReconnectPacketIds, rendered_text: &str, compression_threshold: i32) -> Vec<u8> {
    let mut payload = Vec::new();
    write_var_int(&mut payload, ids.play_set_action_bar_text);
    payload.push(crate::nbt::STRING);
    crate::nbt::write_nbt_string(&mut payload, rendered_text);
    frame(&payload, compression_threshold)
}

/// Title text (Play state) — the large centered text shown while waiting to reconnect. Takes
/// already-rendered legacy text.
pub fn encode_set_title_text(ids: &ReconnectPacketIds, rendered_text: &str, compression_threshold: i32) -> Vec<u8> {
    let mut payload = Vec::new();
    write_var_int(&mut payload, ids.play_set_title_text);
    payload.push(crate::nbt::STRING);
    crate::nbt::write_nbt_string(&mut payload, rendered_text);
    frame(&payload, compression_threshold)
}

/// Subtitle text (Play state) — the smaller text shown under the title. Takes already-rendered
/// legacy text.
pub fn encode_set_subtitle_text(ids: &ReconnectPacketIds, rendered_text: &str, compression_threshold: i32) -> Vec<u8> {
    let mut payload = Vec::new();
    write_var_int(&mut payload, ids.play_set_subtitle_text);
    payload.push(crate::nbt::STRING);
    crate::nbt::write_nbt_string(&mut payload, rendered_text);
    frame(&payload, compression_threshold)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_supported_only_for_known_brackets() {
        assert!(reconnect_supported(774));
        assert!(reconnect_supported(775));
        assert!(reconnect_supported(776));
        assert!(!reconnect_supported(767));
        assert!(!reconnect_supported(1));
    }

    #[test]
    fn every_encoder_produces_a_nonempty_framed_packet() {
        let ids = reconnect_packet_ids(776);
        let uuid = Uuid::new_v4();
        assert!(!encode_login_disconnect("{}").is_empty());
        assert!(!encode_login_success(&ids, uuid, "Steve", -1).is_empty());
        assert!(!encode_finish_configuration(&ids, -1).is_empty());
        assert!(!encode_config_keep_alive(&ids, 1, -1).is_empty());
        assert!(!encode_play_keep_alive(&ids, 1, -1).is_empty());
        assert!(!encode_start_configuration(&ids, -1).is_empty());
        assert!(!encode_play_disconnect(&ids, "bye", -1).is_empty());
        assert!(!encode_transfer(&ids, "example.com", 25565, -1).is_empty());
        assert!(!encode_join_game(&ids, 1, -1).is_empty());
        assert!(!encode_set_center_chunk(&ids, -1, 0, 0).is_empty());
        assert!(!encode_empty_chunk(&ids, -1, 0, 0).is_empty());
        assert!(!encode_action_bar(&ids, "hi", -1).is_empty());
        assert!(!encode_set_title_text(&ids, "hi", -1).is_empty());
        assert!(!encode_set_subtitle_text(&ids, "hi", -1).is_empty());
    }

    #[test]
    #[should_panic(expected = "No auto-reconnect packet table")]
    fn reconnect_packet_ids_panics_for_unsupported_version() {
        reconnect_packet_ids(1);
    }

    #[test]
    fn transfer_packet_contains_host_and_port() {
        let ids = reconnect_packet_ids(776);
        let wire = encode_transfer(&ids, "example.com", 25566, -1);
        // frame length varint, then packet-id varint, then string, then u16 port
        let (_, mut pos) = crate::varint::read_var_int(&wire).unwrap();
        let (id, c) = crate::varint::read_var_int(&wire[pos..]).unwrap();
        pos += c;
        assert_eq!(id, ids.play_transfer);
        let (host, c) = crate::varint::read_string(&wire[pos..], 255).unwrap();
        pos += c;
        assert_eq!(host, "example.com");
        let port = u16::from_be_bytes(wire[pos..pos + 2].try_into().unwrap());
        assert_eq!(port, 25566);
    }
}
