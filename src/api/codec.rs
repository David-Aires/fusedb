// src/api/codec.rs
// ──────────────────────────────────────────────────────────────────────────────
// The value encoding shared with the Python package.
//
// The container format (header, CRC, index) has always been language-neutral.
// This module is what makes the *payloads* neutral too: both implementations
// store MessagePack, encoded the same way, so neither side needs to know which
// one wrote the file.

use serde::{de::DeserializeOwned, Serialize};

use crate::core::FuseResult;

/// Encode `value` as MessagePack, matching Python's
/// `msgpack.packb(value, use_bin_type=True)`.
///
/// `to_vec_named` is the load-bearing detail: it makes Rust structs land on
/// disk as msgpack *maps* keyed by field name — the shape Python produces for a
/// `dict`. Plain `to_vec` would emit positional arrays, which the Python side
/// could not interpret.
///
/// Use this when you want to hand pre-encoded bytes to
/// [`FuseWriter::add_object_raw`](super::FuseWriter::add_object_raw), for
/// example to encode once and store under several writers.
///
/// ```
/// # use std::collections::BTreeMap;
/// let mut v = BTreeMap::new();
/// v.insert("company", "Google");
/// let raw = fusedb::encode(&v)?;
/// assert_eq!(fusedb::decode::<BTreeMap<String, String>>(&raw)?["company"], "Google");
/// # Ok::<(), fusedb::FuseError>(())
/// ```
pub fn encode<T: Serialize + ?Sized>(value: &T) -> FuseResult<Vec<u8>> {
    Ok(rmp_serde::encode::to_vec_named(value)?)
}

/// Decode MessagePack bytes into `T`, matching Python's
/// `msgpack.unpackb(raw, raw=False)`.
pub fn decode<T: DeserializeOwned>(raw: &[u8]) -> FuseResult<T> {
    Ok(rmp_serde::decode::from_slice(raw)?)
}
