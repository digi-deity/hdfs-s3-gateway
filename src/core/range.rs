//! Pure byte-range resolution logic, free of `s3s`/HTTP types.
//!
//! `s3s` parses the HTTP `Range` header into a structured [`s3s::dto::Range`] for us, but
//! the offset arithmetic is unit-tested in isolation (no `s3s`, no I/O).
//! This module mirrors the three HTTP range shapes and resolves them against a known file
//! length into an inclusive `(start, end_exclusive)` byte window. The `s3` module converts the
//! `s3s` range into [`ByteRange`] and calls [`resolve_range`].

/// A byte-range request, mirroring the three HTTP `Range` shapes (RFC 9110 §14.1.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteRange {
    /// `bytes=first-` — from `first` to the end of the file.
    From { first: u64 },
    /// `bytes=first-last` — inclusive on both ends.
    Inclusive { first: u64, last: u64 },
    /// `bytes=-length` — the last `length` bytes of the file.
    Suffix { length: u64 },
}

/// Resolve a [`ByteRange`] against a file of `file_len` bytes.
///
/// Returns `(start, end_exclusive)` — the half-open window to read. Returns `None` if the
/// range is not satisfiable (e.g. `first >= file_len`, or a zero-length suffix on an empty
/// file), which the caller maps to a `416 RangeNotSatisfiable` / `InvalidRange` error.
///
/// Per RFC 9110, an `Inclusive` range with `last >= file_len` is clamped to the file end
/// rather than rejected, and a `Suffix` longer than the file returns the whole file.
pub fn resolve_range(file_len: u64, range: ByteRange) -> Option<(u64, u64)> {
    match range {
        ByteRange::From { first } => {
            if first >= file_len {
                return None;
            }
            Some((first, file_len))
        }
        ByteRange::Inclusive { first, last } => {
            if first >= file_len {
                return None;
            }
            let last = last.min(file_len - 1);
            if first > last {
                return None;
            }
            Some((first, last + 1))
        }
        ByteRange::Suffix { length } => {
            if length == 0 {
                return None;
            }
            let length = length.min(file_len);
            Some((file_len - length, file_len))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_full_file() {
        // bytes=0- on a 100-byte file → whole file
        assert_eq!(
            resolve_range(100, ByteRange::From { first: 0 }),
            Some((0, 100))
        );
    }

    #[test]
    fn from_middle() {
        // bytes=10- on a 100-byte file → 10..100
        assert_eq!(
            resolve_range(100, ByteRange::From { first: 10 }),
            Some((10, 100))
        );
    }

    #[test]
    fn from_past_eof_unsatisfiable() {
        // bytes=100- on a 100-byte file → unsatisfiable
        assert_eq!(resolve_range(100, ByteRange::From { first: 100 }), None);
    }

    #[test]
    fn inclusive_exact() {
        // bytes=0-99 on a 100-byte file → 0..100
        assert_eq!(
            resolve_range(100, ByteRange::Inclusive { first: 0, last: 99 }),
            Some((0, 100))
        );
    }

    #[test]
    fn inclusive_clamped_to_eof() {
        // bytes=50-999 on a 100-byte file → clamped to 50..100
        assert_eq!(
            resolve_range(
                100,
                ByteRange::Inclusive {
                    first: 50,
                    last: 999
                }
            ),
            Some((50, 100))
        );
    }

    #[test]
    fn inclusive_first_past_eof_unsatisfiable() {
        assert_eq!(
            resolve_range(
                100,
                ByteRange::Inclusive {
                    first: 100,
                    last: 200
                }
            ),
            None
        );
    }

    #[test]
    fn inclusive_inverted_unsatisfiable() {
        // first > last after clamping
        assert_eq!(
            resolve_range(
                100,
                ByteRange::Inclusive {
                    first: 90,
                    last: 80
                }
            ),
            None
        );
    }

    #[test]
    fn suffix_exact() {
        // bytes=-50 on a 100-byte file → 50..100
        assert_eq!(
            resolve_range(100, ByteRange::Suffix { length: 50 }),
            Some((50, 100))
        );
    }

    #[test]
    fn suffix_longer_than_file() {
        // bytes=-500 on a 100-byte file → whole file
        assert_eq!(
            resolve_range(100, ByteRange::Suffix { length: 500 }),
            Some((0, 100))
        );
    }

    #[test]
    fn suffix_zero_unsatisfiable() {
        assert_eq!(resolve_range(100, ByteRange::Suffix { length: 0 }), None);
    }

    #[test]
    fn suffix_on_empty_file_is_empty_window() {
        // An empty file yields a zero-length window (0..0), not an error.
        assert_eq!(
            resolve_range(0, ByteRange::Suffix { length: 10 }),
            Some((0, 0))
        );
    }
}
