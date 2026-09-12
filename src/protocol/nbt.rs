//! Port of `protocol/Nbt.kt` — a minimal binary NBT writer covering just the tag types needed to
//! build the tiny registry/dimension blobs the reconnect-wait world sends during the
//! Configuration phase (see `reconnect_protocol.rs`). Not a general-purpose NBT library: no
//! reader, no long/int arrays, no lists-of-lists.

pub const END: u8 = 0;
pub const BYTE: u8 = 1;
#[allow(dead_code)]
pub const SHORT: u8 = 2;
pub const INT: u8 = 3;
#[allow(dead_code)]
pub const LONG: u8 = 4;
pub const FLOAT: u8 = 5;
pub const DOUBLE: u8 = 6;
pub const STRING: u8 = 8;
#[allow(dead_code)]
pub const LIST: u8 = 9;
pub const COMPOUND: u8 = 10;

/// Writes an NBT-format string: a big-endian u16 byte length, then UTF-8 bytes (no VarInt — NBT
/// predates and is unrelated to the packet-level VarInt string format in `varint.rs`).
pub fn write_nbt_string(out: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    out.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    out.extend_from_slice(bytes);
}

/// Writes a named tag header (type + name) — the name is empty for the network root compound.
pub fn write_named_tag(out: &mut Vec<u8>, tag_type: u8, name: &str) {
    out.push(tag_type);
    write_nbt_string(out, name);
}

/// Builder for a single NBT compound's contents (assumes the caller already wrote the COMPOUND
/// header).
pub struct CompoundBuilder<'a> {
    out: &'a mut Vec<u8>,
}

impl<'a> CompoundBuilder<'a> {
    pub fn byte(&mut self, name: &str, value: i8) {
        write_named_tag(self.out, BYTE, name);
        self.out.push(value as u8);
    }

    pub fn int(&mut self, name: &str, value: i32) {
        write_named_tag(self.out, INT, name);
        self.out.extend_from_slice(&value.to_be_bytes());
    }

    pub fn float(&mut self, name: &str, value: f32) {
        write_named_tag(self.out, FLOAT, name);
        self.out.extend_from_slice(&value.to_be_bytes());
    }

    pub fn double(&mut self, name: &str, value: f64) {
        write_named_tag(self.out, DOUBLE, name);
        self.out.extend_from_slice(&value.to_be_bytes());
    }

    pub fn string(&mut self, name: &str, value: &str) {
        write_named_tag(self.out, STRING, name);
        write_nbt_string(self.out, value);
    }

    pub fn compound(&mut self, name: &str, block: impl FnOnce(&mut CompoundBuilder)) {
        write_named_tag(self.out, COMPOUND, name);
        let mut nested = CompoundBuilder { out: self.out };
        block(&mut nested);
        self.out.push(END);
    }
}

/// Writes a root, unnamed NBT compound (network NBT format used since 1.20.2 — no root name
/// string).
pub fn write_root_compound(out: &mut Vec<u8>, block: impl FnOnce(&mut CompoundBuilder)) {
    out.push(COMPOUND);
    let mut builder = CompoundBuilder { out };
    block(&mut builder);
    out.push(END);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_root_compound_is_start_and_end_byte() {
        let mut out = Vec::new();
        write_root_compound(&mut out, |_| {});
        assert_eq!(out, vec![COMPOUND, END]);
    }

    #[test]
    fn nested_compound_and_fields_shape() {
        let mut out = Vec::new();
        write_root_compound(&mut out, |root| {
            root.string("name", "hi");
            root.compound("nested", |c| {
                c.int("x", 42);
            });
        });
        assert_eq!(out[0], COMPOUND);
        assert_eq!(*out.last().unwrap(), END);
        // string tag: type(8) + name-len(2) + "name"(4) + value-len(2) + "hi"(2) = 11 bytes
        assert_eq!(out[1], STRING);
    }
}
