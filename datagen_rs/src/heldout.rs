//! Held-out AML seeds, known only as salted SHA-256 hashes.
//!
//! The evaluation and robustness seeds are never compiled in or passed in
//! plaintext to anything that logs. The generator reads the hash file named by
//! `LB_HELDOUT_HASHES` (`spark/data/aml/heldout_hashes.json`, mounted from a
//! ConfigMap) and hashes the seed it was given:
//! `sha256(bytes.fromhex(salt) + b":" + decimal(seed))`. Every registered
//! role hash, the retired ones included, is also compiled in as a floor under
//! its own salt, so a stripped or re-salted file still protects them. The
//! floor is appended to only: a redraw appends to the file, to
//! `_HELDOUT_FLOOR` in `src/lakebench/config/datagen_seed.py` (the Python side
//! of the same rule) and to the floor here, and needs a new image.
//! `tests/test_heldout.py` holds the three equal.

use ring::digest::{digest, SHA256};
use serde_json::Value;

/// Environment variable naming the hash file.
pub const ENV: &str = "LB_HELDOUT_HASHES";
/// The only hash-file format and algorithm this build reads.
pub const FORMAT: u64 = 1;
pub const ALGORITHM: &str = "sha256(bytes.fromhex(salt) + b':' + decimal(seed))";

/// A held-out role.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Evaluation,
    Robustness,
}

impl Role {
    pub const ALL: [Role; 2] = [Role::Evaluation, Role::Robustness];

    pub fn name(self) -> &'static str {
        match self {
            Role::Evaluation => "evaluation",
            Role::Robustness => "robustness",
        }
    }
}

// BEGIN HELDOUT FLOOR (the owner's registrations, appended to only)
pub const FLOOR_SALT: &str = "267910981b7370a7aaed163588284507373136289bfeec908df42ae15e1d75aa";
pub const FLOOR: &[(Role, &str)] = &[
    (
        Role::Evaluation,
        "206064919eb86d06940ba8f4a66510605707e6cfe99bb2564f47dc859c2cba06",
    ),
    (
        Role::Evaluation,
        "e67e2ade9d5ff63721d79f5bb2aed5613fcf936a2c3e9bc80bc1697ee26e1c37",
    ),
    (
        Role::Robustness,
        "9d730778dff8ae49bc2eb428a83016de00a9f227e6c0a9c43f84043fe2869562",
    ),
    (
        Role::Robustness,
        "26b6c7817d64c1e6dfc853157a1c501e166cd4e5edcecab6549f5f3d34d696fc",
    ),
];
// END HELDOUT FLOOR

/// `sha256(salt bytes + b":" + decimal seed)` as lowercase hex.
pub fn seed_hash(salt: &[u8], seed: i64) -> String {
    let mut buf = salt.to_vec();
    buf.push(b':');
    buf.extend_from_slice(seed.to_string().as_bytes());
    digest(&SHA256, &buf)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn is_hex64(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap_or(0))
        .collect()
}

/// The hash file plus the compiled floor. Messages never hold a seed.
#[derive(Clone, Debug)]
pub struct HeldOut {
    salt: Vec<u8>,
    roles: Vec<(Role, String)>,
    spent: Vec<i64>,
    floor_salt: Vec<u8>,
    floor: Vec<(Role, String)>,
}

impl HeldOut {
    /// Read and check the file at `path` (strict: an unknown format,
    /// algorithm, key or role, a bad salt or hash, or a spent entry that is
    /// not a non-negative integer is an error).
    pub fn load(path: &str) -> Result<HeldOut, String> {
        let text = std::fs::read_to_string(path).map_err(|e| e.kind().to_string())?;
        HeldOut::from_json(&text)
    }

    /// `load` from text, with the compiled floor.
    pub fn from_json(text: &str) -> Result<HeldOut, String> {
        let floor = FLOOR.iter().map(|(r, h)| (*r, h.to_string())).collect();
        HeldOut::from_json_with_floor(text, FLOOR_SALT, floor)
    }

    /// `from_json` with another floor (tests use a fixture floor).
    pub fn from_json_with_floor(
        text: &str,
        floor_salt: &str,
        floor: Vec<(Role, String)>,
    ) -> Result<HeldOut, String> {
        let doc: Value =
            serde_json::from_str(text).map_err(|e| format!("not JSON ({})", e.classify() as u8))?;
        let obj = doc.as_object().ok_or("the document is not an object")?;
        let known = [
            "format",
            "algorithm",
            "salt",
            "roles",
            "spent",
            "absence_check",
        ];
        if let Some(k) = obj
            .keys()
            .find(|k| !known.contains(&k.as_str()) && !k.starts_with('_'))
        {
            return Err(format!("unknown key {k:?}"));
        }
        if obj.get("format").and_then(Value::as_u64) != Some(FORMAT) {
            return Err(format!("format must be {FORMAT}"));
        }
        if obj.get("algorithm").and_then(Value::as_str) != Some(ALGORITHM) {
            return Err("algorithm is not the supported one".into());
        }
        let salt = obj
            .get("salt")
            .and_then(Value::as_str)
            .filter(|s| is_hex64(s))
            .ok_or("salt must be 64 lowercase hex characters")?;
        let roles_v = obj
            .get("roles")
            .and_then(Value::as_object)
            .ok_or("roles must be an object")?;
        if let Some(k) = roles_v
            .keys()
            .find(|k| !Role::ALL.iter().any(|r| r.name() == k.as_str()))
        {
            return Err(format!("role {k:?} is not evaluation or robustness"));
        }
        let mut roles = Vec::new();
        for r in Role::ALL {
            let list = roles_v
                .get(r.name())
                .and_then(Value::as_array)
                .ok_or(format!("roles.{} must be a list", r.name()))?;
            for (i, h) in list.iter().enumerate() {
                let h = h.as_str().filter(|h| is_hex64(h)).ok_or(format!(
                    "roles.{}[{i}] is not a 64-character lowercase hex hash",
                    r.name()
                ))?;
                if roles.iter().any(|(_, x): &(Role, String)| x == h) {
                    return Err(format!("roles.{}[{i}] repeats a hash", r.name()));
                }
                roles.push((r, h.to_string()));
            }
        }
        let spent_v = obj
            .get("spent")
            .and_then(Value::as_array)
            .ok_or("spent must be a list of integers")?;
        let mut spent = Vec::new();
        for (i, v) in spent_v.iter().enumerate() {
            match v.as_i64() {
                Some(s) if s >= 0 => spent.push(s),
                _ => return Err(format!("spent[{i}] is not a non-negative 64-bit integer")),
            }
        }
        match obj.get("absence_check").and_then(Value::as_str) {
            Some("report") | Some("enforce") => {}
            _ => return Err("absence_check must be report or enforce".into()),
        }
        if !is_hex64(floor_salt) || Role::ALL.iter().any(|r| !floor.iter().any(|(f, _)| f == r)) {
            return Err("the compiled held-out floor is not initialised".into());
        }
        if salt == floor_salt {
            for (r, h) in &roles {
                if floor.iter().any(|(f, fh)| f != r && fh == h) {
                    return Err(format!(
                        "roles.{} holds a hash the compiled floor registers under another role",
                        r.name()
                    ));
                }
            }
        }
        Ok(HeldOut {
            salt: unhex(salt),
            roles,
            spent,
            floor_salt: unhex(floor_salt),
            floor,
        })
    }

    /// The role `seed` is held out for: its hash under the file's salt is in
    /// the file, or its hash under the floor's salt is in the floor. A seed
    /// the file and the floor give different roles is an error, so a
    /// re-salted file cannot move a floor seed to another role.
    pub fn role_of(&self, seed: i64) -> Result<Option<Role>, String> {
        let fh = seed_hash(&self.salt, seed);
        let gh = seed_hash(&self.floor_salt, seed);
        let mut found: Vec<Role> = Vec::new();
        for (r, h) in &self.roles {
            if *h == fh && !found.contains(r) {
                found.push(*r);
            }
        }
        for (r, h) in &self.floor {
            if *h == gh && !found.contains(r) {
                found.push(*r);
            }
        }
        match found.as_slice() {
            [] => Ok(None),
            [r] => Ok(Some(*r)),
            _ => Err("the hash file and the compiled floor give the seed different roles".into()),
        }
    }

    /// How a marker names a financial corpus seed: its salted hash under the
    /// file's salt (`datagen_seed.seed_ref` on the Python side), so a marker
    /// never carries the seed.
    pub fn seed_ref(&self, seed: i64) -> String {
        seed_hash(&self.salt, seed)
    }

    /// Whether the file lists `seed` as spent.
    pub fn is_spent(&self, seed: i64) -> bool {
        self.spent.contains(&seed)
    }
}
