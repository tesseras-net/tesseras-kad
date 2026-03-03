//! Minimal SHA-1 (FIPS 180-4) implementation.

const H0: u32 = 0x6745_2301;
const H1: u32 = 0xEFCD_AB89;
const H2: u32 = 0x98BA_DCFE;
const H3: u32 = 0x1032_5476;
const H4: u32 = 0xC3D2_E1F0;

fn process_block(
    state: &mut [u32; 5],
    block: &[u8; 64],
) {
    let mut w = [0u32; 80];
    for (i, word) in w[..16].iter_mut().enumerate()
    {
        let off = i * 4;
        *word = u32::from_be_bytes([
            block[off],
            block[off + 1],
            block[off + 2],
            block[off + 3],
        ]);
    }
    for i in 16..80 {
        w[i] = (w[i - 3]
            ^ w[i - 8]
            ^ w[i - 14]
            ^ w[i - 16])
            .rotate_left(1);
    }

    let [mut a, mut b, mut c, mut d, mut e] =
        *state;

    for (i, &wi) in w.iter().enumerate() {
        let (f, k) = match i {
            0..20 => ((b & c) | ((!b) & d), 0x5A82_7999u32),
            20..40 => (b ^ c ^ d, 0x6ED9_EBA1u32),
            40..60 => {
                ((b & c) | (b & d) | (c & d), 0x8F1B_BCDCu32)
            }
            _ => (b ^ c ^ d, 0xCA62_C1D6u32),
        };
        let temp = a
            .rotate_left(5)
            .wrapping_add(f)
            .wrapping_add(e)
            .wrapping_add(k)
            .wrapping_add(wi);
        e = d;
        d = c;
        c = b.rotate_left(30);
        b = a;
        a = temp;
    }

    state[0] = state[0].wrapping_add(a);
    state[1] = state[1].wrapping_add(b);
    state[2] = state[2].wrapping_add(c);
    state[3] = state[3].wrapping_add(d);
    state[4] = state[4].wrapping_add(e);
}

/// Compute the SHA-1 digest of `data`.
///
/// Returns a 20-byte hash.
pub fn sha1(data: &[u8]) -> [u8; 20] {
    let mut state = [H0, H1, H2, H3, H4];
    let bit_len = (data.len() as u64) * 8;

    // Process complete 64-byte blocks.
    let mut offset = 0;
    while offset + 64 <= data.len() {
        let block: &[u8; 64] =
            data[offset..offset + 64].try_into().unwrap();
        process_block(&mut state, block);
        offset += 64;
    }

    // Pad: remaining bytes + 0x80 + zeros + 8-byte length.
    let remaining = &data[offset..];
    let mut pad = [0u8; 128];
    pad[..remaining.len()].copy_from_slice(remaining);
    pad[remaining.len()] = 0x80;

    let pad_blocks = if remaining.len() < 56 { 1 } else { 2 };
    let total_pad = pad_blocks * 64;
    pad[total_pad - 8..total_pad]
        .copy_from_slice(&bit_len.to_be_bytes());

    let mut p = 0;
    while p < total_pad {
        let block: &[u8; 64] =
            pad[p..p + 64].try_into().unwrap();
        process_block(&mut state, block);
        p += 64;
    }

    let mut out = [0u8; 20];
    for (i, &word) in state.iter().enumerate() {
        out[i * 4..i * 4 + 4]
            .copy_from_slice(&word.to_be_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        bytes
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    #[test]
    fn empty_string() {
        let digest = sha1(b"");
        assert_eq!(
            hex(&digest),
            "da39a3ee5e6b4b0d3255bfef95601890afd80709"
        );
    }

    #[test]
    fn abc() {
        let digest = sha1(b"abc");
        assert_eq!(
            hex(&digest),
            "a9993e364706816aba3e25717850c26c9cd0d89d"
        );
    }

    #[test]
    fn long_string() {
        let digest = sha1(
            b"abcdbcdecdefdefgefghfghighij\
              hijkijkljklmklmnlmnomnopnopq",
        );
        assert_eq!(
            hex(&digest),
            "84983e441c3bd26ebaae4aa1f95129e5e54670f1"
        );
    }
}
