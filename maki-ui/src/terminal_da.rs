use std::sync::OnceLock;

static DETECTED_FEATURES: OnceLock<Vec<Da1Feature>> = OnceLock::new();

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Da1Feature {
    Sixel,
    Osc52,
}

impl Da1Feature {
    pub(crate) fn response_tail<'a>(&self, buffer: &'a [u8]) -> Option<&'a [u8]> {
        let feature: &[u8] = match self {
            Self::Sixel => b"4",
            Self::Osc52 => b"52",
        };
        for (offset, prefix) in buffer.windows(3).enumerate() {
            if prefix != b"\x1b[?" {
                continue;
            }
            let params = &buffer[offset + prefix.len()..];
            let Some(end) = params.iter().position(|byte| (0x40..=0x7e).contains(byte)) else {
                continue;
            };
            if params[end] != b'c' {
                continue;
            }
            let parts = params[..end].split(|&byte| byte == b';');
            if parts
                .clone()
                .all(|part| !part.is_empty() && part.iter().all(u8::is_ascii_digit))
                && parts.skip(1).any(|part| part == feature)
            {
                return Some(&params[end + 1..]);
            }
        }
        None
    }

    pub(crate) fn is_supported(&self) -> bool {
        DETECTED_FEATURES
            .get()
            .is_some_and(|features| features.contains(self))
    }
}

pub(crate) fn cache_features(buffer: &[u8]) {
    DETECTED_FEATURES.get_or_init(|| {
        [Da1Feature::Sixel, Da1Feature::Osc52]
            .into_iter()
            .filter(|feature| feature.response_tail(buffer).is_some())
            .collect()
    });
}

#[cfg(test)]
mod tests {
    use super::Da1Feature;
    use test_case::test_case;

    #[test_case(b"\x1b[?62;22;52c", true; "clipboard_without_graphics")]
    #[test_case(b"\x1b[?62;4;52c\x1b[0n", true; "clipboard_and_sixel")]
    #[test_case(b"\x1b[?62;52;22c", true; "feature_before_other_features")]
    #[test_case(b"\x1b[?62;22c", false; "no_advertisement")]
    #[test_case(b"\x1b[?52;22c", false; "model_is_not_a_feature")]
    #[test_case(b"\x1b[?62;152c", false; "exact_feature_only")]
    #[test_case(b"\x1b[?62;52", false; "incomplete_reply")]
    #[test_case(b"\x1b[?62;52n", false; "wrong_terminator")]
    #[test_case(b"\x1b[?62;;52c", false; "empty_parameter")]
    #[test_case(b"\x1b[?62;52;badc", false; "invalid_parameter")]
    #[test_case(b"\x1b[>62;52c", false; "secondary_attributes")]
    #[test_case(b"\x1b[?62;22c\x1b[?62;52c", true; "later_reply")]
    #[test_case(b"\x1b[?62;52\x1b[?62;52c", true; "malformed_then_valid_reply")]
    #[test_case(b"\x1b_Gi=31;OK\x1b\\\x1b[0n", false; "graphics_does_not_imply_clipboard")]
    fn osc52_advertisement(response: &[u8], expected: bool) {
        assert_eq!(
            Da1Feature::Osc52.response_tail(response).is_some(),
            expected
        );
    }

    #[test_case(Da1Feature::Sixel, b"\x1b[?62;4c", Some(b"".as_slice()); "sixel_without_tail")]
    #[test_case(Da1Feature::Sixel, b"\x1b[0n\x1b[?62;4c", Some(b"".as_slice()); "sentinel_before_reply")]
    #[test_case(Da1Feature::Sixel, b"\x1b[?62;4c\x1b[0n", Some(b"\x1b[0n".as_slice()); "sentinel_after_reply")]
    #[test_case(Da1Feature::Sixel, b"\x1b[?4;52c", None; "sixel_model_is_not_a_feature")]
    #[test_case(Da1Feature::Osc52, b"\x1b[?62;22c\x1b[?62;52c\x1b[0n", Some(b"\x1b[0n".as_slice()); "tail_of_matching_reply")]
    fn feature_response_tail(feature: Da1Feature, response: &[u8], expected: Option<&[u8]>) {
        assert_eq!(feature.response_tail(response), expected);
    }
}
