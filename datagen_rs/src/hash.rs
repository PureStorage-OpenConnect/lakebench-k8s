//! Deterministic hashing, matching datagen_v2/world.py exactly.
//!
//! splitmix64 is bit-identical to numpy's implementation in world.py, so every
//! per-entity derivation keyed on (seed, id) produces the same value as the
//! Python generator. That is what lets the existing gate -- which derives the
//! IBAN identity key with the Python routine -- validate Rust output unchanged.

pub const GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

#[inline(always)]
pub fn splitmix64(x: u64) -> u64 {
    let x = x.wrapping_add(GAMMA);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// hash_frac(id, salt) == splitmix64(id ^ (salt * 0x9E3779B9)) / 2^64.
/// Matches world.py:hash_frac. `salt` is the small integer (seed + offset).
#[inline(always)]
pub fn hash_frac(id: u64, salt: i64) -> f64 {
    let s = (salt as u64).wrapping_mul(0x9E37_79B9);
    (splitmix64(id ^ s) as f64) / 18446744073709551616.0
}

/// Counter-based RNG for the sampled (non-deterministic-per-entity) parts of a
/// batch. Seeded per file so a file's content depends only on (seed, file_id).
pub struct Rng {
    state: u64,
}

impl Rng {
    #[inline(always)]
    pub fn new(seed: u64) -> Self {
        // Hash the seed so that streams from adjacent seeds (e.g. consecutive
        // typology instance seeds) are independent, not one-step shifts of each
        // other. Without this, rng(s) and rng(s+1) share nearly every draw.
        Self {
            state: splitmix64(seed ^ 0x243F_6A88_85A3_08D3),
        }
    }
    #[inline(always)]
    pub fn next_u64(&mut self) -> u64 {
        let r = splitmix64(self.state);
        self.state = self.state.wrapping_add(GAMMA);
        r
    }
    #[inline(always)]
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next_u64() % n
        }
    }
    /// Uniform double in [0, 1).
    #[inline(always)]
    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
    /// Standard normal via Box-Muller (one of a pair; we discard the second).
    #[inline(always)]
    pub fn normal(&mut self) -> f64 {
        let u1 = (self.unit()).max(1e-300);
        let u2 = self.unit();
        (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
    }
}

/// The two 64-bit seeds a row's UETR and msg_id are derived from. The corpus
/// seed is hashed before it is mixed with the row uid: a plain `uid ^ seed`
/// made uid 100 under seed 42 collide with uid 101 under seed 43, so corpora
/// with nearby seeds shared UETRs. Shared by the bronze writer and the
/// manifest builder, which must agree exactly.
#[inline]
pub fn uetr_seeds(uid: u64, seed: i64) -> (u64, u64) {
    let s = splitmix64((seed as u64) ^ 0x7E7B_1D00_5EED_0001);
    (
        splitmix64(uid ^ s ^ 0x0E7A),
        splitmix64(uid ^ splitmix64(s ^ 0x5A1D)),
    )
}
