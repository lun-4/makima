use serde::{Deserialize, Serialize};
use strum::{EnumString, VariantNames};

pub const POST_LEVELS: &[&str] = PostLevel::VARIANTS;

#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, EnumString, VariantNames,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum PostLevel {
    #[default]
    Info,
    Warn,
    Error,
}

impl PostLevel {
    pub fn parse(name: &str) -> Option<Self> {
        name.parse().ok()
    }
}

/// A display-only row in an agent's chat. It never enters provider history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Post {
    pub text: String,
    #[serde(default, skip_serializing_if = "is_info")]
    pub level: PostLevel,
    /// Transcript id of the child agent the post is about.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
}

fn is_info(level: &PostLevel) -> bool {
    *level == PostLevel::Info
}

#[cfg(test)]
mod tests {
    use super::{POST_LEVELS, PostLevel};
    use test_case::test_case;

    #[test_case("info", Some(PostLevel::Info) ; "info")]
    #[test_case("warn", Some(PostLevel::Warn) ; "warn")]
    #[test_case("error", Some(PostLevel::Error) ; "error")]
    #[test_case("fatal", None ; "unknown")]
    fn parse_post_level(name: &str, expected: Option<PostLevel>) {
        assert_eq!(PostLevel::parse(name), expected);
    }

    #[test]
    fn every_listed_level_parses() {
        assert!(
            POST_LEVELS
                .iter()
                .all(|name| PostLevel::parse(name).is_some())
        );
    }
}
