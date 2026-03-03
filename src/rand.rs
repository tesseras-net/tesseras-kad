//! Cross-platform cryptographically secure random
//! number generation.
//!
//! Uses `/dev/urandom` on Unix and `BCryptGenRandom`
//! on Windows. No external dependencies.

/// Fill `buf` with cryptographically secure random
/// bytes.
#[cfg(unix)]
pub fn fill_bytes(buf: &mut [u8]) {
    use std::io::Read;
    let mut f = std::fs::File::open("/dev/urandom")
        .expect("failed to open /dev/urandom");
    f.read_exact(buf).expect("failed to read random bytes");
}

/// Fill `buf` with cryptographically secure random
/// bytes.
#[cfg(windows)]
pub fn fill_bytes(buf: &mut [u8]) {
    #[link(name = "bcrypt")]
    unsafe extern "system" {
        fn BCryptGenRandom(
            h_algorithm: *mut core::ffi::c_void,
            pb_buffer: *mut u8,
            cb_buffer: u32,
            dw_flags: u32,
        ) -> i32;
    }
    const BCRYPT_USE_SYSTEM_PREFERRED_RNG: u32 = 0x0000_0002;
    let status = unsafe {
        BCryptGenRandom(
            std::ptr::null_mut(),
            buf.as_mut_ptr(),
            buf.len() as u32,
            BCRYPT_USE_SYSTEM_PREFERRED_RNG,
        )
    };
    assert!(status >= 0, "BCryptGenRandom failed: {status}");
}

/// Return a random `u64`.
pub fn random_u64() -> u64 {
    let mut buf = [0u8; 8];
    fill_bytes(&mut buf);
    u64::from_le_bytes(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fill_bytes_not_all_zero() {
        let mut buf = [0u8; 32];
        fill_bytes(&mut buf);
        assert_ne!(buf, [0u8; 32]);
    }

    #[test]
    fn two_fills_differ() {
        let mut a = [0u8; 32];
        let mut b = [0u8; 32];
        fill_bytes(&mut a);
        fill_bytes(&mut b);
        assert_ne!(a, b);
    }

    #[test]
    fn random_u64_works() {
        let a = random_u64();
        let b = random_u64();
        assert_ne!(a, b);
    }
}
