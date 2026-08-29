//! Fuzzy matching backed by nucleo, the same matcher makima's built-in
//! pickers use. Shared by command-argument resolution and the `maki.match`
//! Lua API.

use nucleo_matcher::pattern::{AtomKind, CaseMatching, Normalization, Pattern};
use nucleo_matcher::{Config, Matcher, Utf32Str};
use std::cmp::Ordering;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionMatch {
    pub indices: Vec<u32>,
    pub ranking: CompletionRanking,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletionRanking {
    pub quality_rank: u8,
    pub boundary_rank: u8,
    pub start_index: usize,
    pub gap_count: usize,
    pub span_length: usize,
    pub unmatched_suffix: usize,
    pub fuzzy_score: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletionMatchOptions {
    pub case_matching: CaseMatching,
    pub normalization: Normalization,
}

impl Default for CompletionMatchOptions {
    fn default() -> Self {
        Self {
            case_matching: CaseMatching::Smart,
            normalization: Normalization::Smart,
        }
    }
}

pub fn completion_match_default(query: &str, label: &str) -> Option<CompletionMatch> {
    completion_match(query, label, CompletionMatchOptions::default())
}

pub fn completion_match(
    query: &str,
    label: &str,
    options: CompletionMatchOptions,
) -> Option<CompletionMatch> {
    let pattern = Pattern::parse(query, options.case_matching, options.normalization);
    let mut matcher = Matcher::new(Config::DEFAULT);
    let chars: Vec<char> = label.chars().collect();
    let haystack = if label.is_ascii() {
        Utf32Str::Ascii(label.as_bytes())
    } else {
        Utf32Str::Unicode(&chars)
    };
    let mut indices = Vec::new();
    let mut score = 0u32;
    let mut positive_atoms = 0;
    for atom in &pattern.atoms {
        if atom.negative {
            atom.score(haystack, &mut matcher)?;
            continue;
        }
        let mut atom_indices = Vec::new();
        let atom_score = atom.indices(haystack, &mut matcher, &mut atom_indices)?;
        positive_atoms += 1;
        score += u32::from(atom_score);
        indices.extend(atom_indices);
    }
    indices.sort_unstable();
    indices.dedup();
    if indices.is_empty() {
        return Some(CompletionMatch {
            indices,
            ranking: CompletionRanking {
                quality_rank: 4,
                boundary_rank: 1,
                start_index: 0,
                gap_count: 0,
                span_length: 0,
                unmatched_suffix: 0,
                fuzzy_score: score,
            },
        });
    }
    let start = indices[0] as usize;
    let end = *indices.last().unwrap() as usize;
    let contiguous = indices.windows(2).all(|w| w[1] == w[0] + 1);
    let query_length = query.chars().count();
    let quality_rank = if positive_atoms > 1 {
        3
    } else if label.chars().count() == query_length && contiguous {
        0
    } else if start == 0 && contiguous {
        1
    } else if contiguous {
        2
    } else {
        3
    };
    let boundary_rank = if start == 0
        || matches!(
            chars.get(start.wrapping_sub(1)),
            Some('/' | '-' | '_' | '.' | ':')
        ) {
        0
    } else {
        1
    };
    let gap_count = indices.windows(2).map(|w| (w[1] - w[0] - 1) as usize).sum();
    Some(CompletionMatch {
        indices,
        ranking: CompletionRanking {
            quality_rank,
            boundary_rank,
            start_index: start,
            gap_count,
            span_length: end - start + 1,
            unmatched_suffix: label.chars().count().saturating_sub(end + 1),
            fuzzy_score: score,
        },
    })
}

#[allow(clippy::too_many_arguments)]
pub fn compare_completion_matches(
    left: &CompletionMatch,
    right: &CompletionMatch,
    left_source_rank: u8,
    right_source_rank: u8,
    left_source_order: usize,
    right_source_order: usize,
    left_label: &str,
    right_label: &str,
) -> Ordering {
    let a = left.ranking;
    let b = right.ranking;
    a.quality_rank
        .cmp(&b.quality_rank)
        .then(a.boundary_rank.cmp(&b.boundary_rank))
        .then(a.start_index.cmp(&b.start_index))
        .then(a.gap_count.cmp(&b.gap_count))
        .then(a.span_length.cmp(&b.span_length))
        .then(a.unmatched_suffix.cmp(&b.unmatched_suffix))
        .then(b.fuzzy_score.cmp(&a.fuzzy_score))
        .then(left_source_rank.cmp(&right_source_rank))
        .then(left_source_order.cmp(&right_source_order))
        .then(left_label.cmp(right_label))
}

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
    use super::{
        CompletionMatchOptions, Resolution, completion_match, completion_match_default,
        fuzzy_match, fuzzy_resolve,
    };
    use nucleo_matcher::pattern::{CaseMatching, Normalization};
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

    #[test]
    fn completion_match_returns_zero_based_indices() {
        let matched = completion_match_default("ap", "apple").unwrap();
        assert_eq!(matched.indices, vec![0, 1]);
        assert_eq!(matched.ranking.quality_rank, 1);
    }

    #[test]
    fn completion_match_requires_all_positive_atoms_and_excludes_negative_indices() {
        assert!(completion_match_default("!pie", "pie").is_none());
        let matched = completion_match_default("apple !xyz", "apple").unwrap();
        assert_eq!(matched.indices, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn completion_match_options_control_case_matching() {
        let options = CompletionMatchOptions {
            case_matching: CaseMatching::Ignore,
            normalization: Normalization::Smart,
        };
        assert!(completion_match("APPLE", "apple", options).is_some());
    }
}
