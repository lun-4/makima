//! Fuzzy matching backed by nucleo, the same matcher makima's built-in
//! pickers use. Shared by command-argument resolution and the `maki.match`
//! Lua API.

use nucleo_matcher::pattern::{AtomKind, CaseMatching, Normalization, Pattern};
use nucleo_matcher::{Config, Matcher, Utf32Str};

/// Resolve a free-text argument against a candidate list: a case-insensitive
/// exact match wins outright; otherwise every candidate that fuzzy-matches is
/// collected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolution {
    /// Exactly one candidate matched.
    Unique(usize),
    /// Nothing matched.
    NoMatch,
    /// Two or more candidates matched.
    Ambiguous,
}

/// nucleo fuzzy match of {query} against {text}.
///
/// Returns the nucleo score (higher is better) and the 1-based codepoint
/// indices of the matched characters, ascending and deduplicated. Every
/// whitespace-separated query word must match somewhere in the text (order
/// does not matter). An empty or whitespace-only query matches with score 0
/// and no indices.
pub fn fuzzy_match(query: &str, text: &str) -> Option<(u32, Vec<u32>)> {
    let query = query.trim();
    if query.is_empty() {
        return Some((0, Vec::new()));
    }
    let mut matcher = Matcher::new(Config::DEFAULT);
    let pattern = Pattern::new(
        query,
        CaseMatching::Smart,
        Normalization::Smart,
        AtomKind::Fuzzy,
    );
    let mut chars = Vec::new();
    // codepoint haystack, not Utf32Str::new's grapheme segmentation: the
    // returned indices are codepoint positions (consumers map them to bytes
    // via utf8.offset), and graphemes would shift them for emoji/markers
    let haystack = if text.is_ascii() {
        Utf32Str::Ascii(text.as_bytes())
    } else {
        chars.extend(text.chars());
        Utf32Str::Unicode(&chars)
    };
    let mut indices = Vec::new();
    let score = pattern.indices(haystack, &mut matcher, &mut indices)?;
    indices.sort_unstable();
    indices.dedup();
    let indices = indices.into_iter().map(|index| index + 1).collect();
    Some((score, indices))
}

/// Resolve {query} against {candidates}: a case-insensitive exact match wins
/// outright; otherwise every candidate that fuzzy-matches is collected
/// (0 -> `NoMatch`, 1 -> `Unique`, 2+ -> `Ambiguous`). An empty or
/// whitespace-only query resolves to `NoMatch`.
pub fn fuzzy_resolve(query: &str, candidates: &[impl AsRef<str>]) -> Resolution {
    let query = query.trim();
    if query.is_empty() {
        return Resolution::NoMatch;
    }
    if let Some(index) = candidates
        .iter()
        .position(|candidate| candidate.as_ref().eq_ignore_ascii_case(query))
    {
        return Resolution::Unique(index);
    }
    fuzzy_resolve_by(query, candidates, |query, candidate| {
        fuzzy_match(query, candidate.as_ref()).is_some()
    })
}

pub struct MatchCandidate<'a> {
    pub value: &'a str,
    pub fields: Vec<&'a str>,
}

pub fn fuzzy_resolve_candidates(query: &str, candidates: &[MatchCandidate<'_>]) -> Resolution {
    let query = query.trim();
    if query.is_empty() {
        return Resolution::NoMatch;
    }
    if let Some(index) = candidates
        .iter()
        .position(|candidate| candidate.value.eq_ignore_ascii_case(query))
    {
        return Resolution::Unique(index);
    }
    fuzzy_resolve_by(query, candidates, |query, candidate| {
        fuzzy_match_fields(query, candidate.fields.iter().copied())
    })
}

fn fuzzy_resolve_by<T>(
    query: &str,
    candidates: &[T],
    matches: impl Fn(&str, &T) -> bool,
) -> Resolution {
    let query = query.trim();
    if query.is_empty() {
        return Resolution::NoMatch;
    }
    let mut unique: Option<usize> = None;
    for (index, candidate) in candidates.iter().enumerate() {
        if matches(query, candidate) {
            match unique {
                None => unique = Some(index),
                Some(_) => return Resolution::Ambiguous,
            }
        }
    }
    match unique {
        Some(index) => Resolution::Unique(index),
        None => Resolution::NoMatch,
    }
}

fn fuzzy_match_fields<I>(query: &str, fields: I) -> bool
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let patterns: Vec<_> = query
        .split_whitespace()
        .map(|word| {
            Pattern::new(
                word,
                CaseMatching::Smart,
                Normalization::Smart,
                AtomKind::Fuzzy,
            )
        })
        .collect();
    let fields: Vec<_> = fields.into_iter().collect();
    let mut matcher = Matcher::new(Config::DEFAULT);
    patterns.iter().all(|pattern| {
        fields.iter().any(|field| {
            let text = field.as_ref();
            let mut chars = Vec::new();
            let haystack = if text.is_ascii() {
                Utf32Str::Ascii(text.as_bytes())
            } else {
                chars.extend(text.chars());
                Utf32Str::Unicode(&chars)
            };
            pattern.score(haystack, &mut matcher).is_some()
        })
    })
}

#[cfg(test)]
mod tests {
    use super::{Resolution, fuzzy_match, fuzzy_resolve};
    use test_case::test_case;

    #[test_case("", "hello world" ; "empty_query")]
    #[test_case("   ", "hello world" ; "whitespace_only_query")]
    fn empty_query_matches_with_zero_score(query: &str, text: &str) {
        assert_eq!(fuzzy_match(query, text), Some((0, Vec::new())));
    }

    #[test_case("xyz", "hello world" ; "missing_term")]
    #[test_case("hello xyz", "hello world" ; "one_missing_term_fails_and")]
    fn no_match_returns_none(query: &str, text: &str) {
        assert_eq!(fuzzy_match(query, text), None);
    }

    #[test]
    fn indices_are_1_based_codepoints() {
        let (_, indices) = fuzzy_match("ap", "apple").unwrap();
        assert_eq!(indices, vec![1, 2]);
    }

    #[test]
    fn multi_term_order_is_independent() {
        assert!(fuzzy_match("441 review", "review gh pr 441").is_some());
    }

    #[test]
    fn smart_case_uppercase_query_is_case_sensitive() {
        assert_eq!(fuzzy_match("APPLE", "apple pie"), None);
    }

    #[test]
    fn smart_case_lowercase_query_is_case_insensitive() {
        assert!(fuzzy_match("apple", "Apple pie").is_some());
    }

    #[test]
    fn cjk_query_yields_codepoint_indices() {
        let (_, indices) = fuzzy_match("好世", "你好世界").unwrap();
        assert_eq!(indices, vec![2, 3]);
    }

    #[test]
    fn indices_are_codepoints_not_graphemes() {
        // "👍🏽" is two codepoints, one grapheme: "a" is codepoint 3, grapheme 2.
        let (_, indices) = fuzzy_match("a", "👍🏽abc").unwrap();
        assert_eq!(indices, vec![3]);
        // "🇺🇸" is two regional-indicator codepoints, one grapheme.
        let (_, indices) = fuzzy_match("b", "🇺🇸ab").unwrap();
        assert_eq!(indices, vec![4]);
    }

    #[test]
    fn contiguous_match_scores_higher_than_scattered() {
        let (tight, _) = fuzzy_match("app", "apple").unwrap();
        let (loose, _) = fuzzy_match("app", "axpyp").unwrap();
        assert!(tight > loose);
    }

    #[test]
    fn repeated_terms_dedup_indices() {
        let (_, indices) = fuzzy_match("a a", "banana").unwrap();
        assert_eq!(indices, vec![2]);
    }

    fn resolve(query: &str, candidates: &[&str]) -> Resolution {
        fuzzy_resolve(query, candidates)
    }

    #[test]
    fn fuzzy_resolve_unique() {
        let c = ["tokyonight", "dracula", "gruvbox"];
        assert_eq!(resolve("toky", &c), Resolution::Unique(0));
    }

    #[test]
    fn fuzzy_resolve_no_match() {
        let c = ["tokyonight", "dracula"];
        assert_eq!(resolve("zzz", &c), Resolution::NoMatch);
    }

    #[test]
    fn fuzzy_resolve_ambiguous() {
        let c = [
            "catppuccin-frappe",
            "catppuccin-latte",
            "dracula",
            "catppuccin-mocha",
        ];
        assert_eq!(resolve("catppuccin", &c), Resolution::Ambiguous);
    }

    #[test]
    fn fuzzy_resolve_exact_wins() {
        // "mo" fuzzy-matches several, but an exact "mocha" still resolves.
        let c = ["mocha", "tokyonight-moon", "dracula"];
        assert_eq!(resolve("mocha", &c), Resolution::Unique(0));
        // The exact match wins even when it is not the only fuzzy hit.
        let c2 = ["moon", "mocha", "emerald"];
        assert_eq!(resolve("mocha", &c2), Resolution::Unique(1));
    }

    #[test]
    fn fuzzy_resolve_case_insensitive() {
        let c = ["Tokyonight", "Dracula"];
        assert_eq!(resolve("TOKYONIGHT", &c), Resolution::Unique(0));
    }

    #[test_case("" ; "empty")]
    #[test_case("   " ; "whitespace_only")]
    fn fuzzy_resolve_empty_query(query: &str) {
        assert_eq!(resolve(query, &["dracula"]), Resolution::NoMatch);
    }
}
