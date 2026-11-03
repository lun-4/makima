use crate::model::{ModelEntry, ModelFamily, ModelTier};
use crate::providers::{anthropic, google, openai};

#[derive(Debug, Clone, Copy)]
pub struct ProviderManifest {
    pub slug: &'static str,
    pub display_name: &'static str,
    pub family: ModelFamily,
    pub supports_thinking: bool,
    pub accepts_arbitrary_models: bool,
    pub fallback_max_output: Option<u32>,
    pub fallback_context_window: u32,
    pub models: &'static [ModelEntry],
}

const ANTHROPIC: ProviderManifest = ProviderManifest {
    slug: "anthropic",
    display_name: "Anthropic",
    family: ModelFamily::Claude,
    supports_thinking: true,
    accepts_arbitrary_models: false,
    fallback_max_output: Some(128_000),
    fallback_context_window: 200_000,
    models: anthropic::models(),
};

const OPENAI: ProviderManifest = ProviderManifest {
    slug: "openai",
    display_name: "OpenAI",
    family: ModelFamily::Gpt,
    supports_thinking: true,
    accepts_arbitrary_models: false,
    fallback_max_output: Some(100_000),
    fallback_context_window: 200_000,
    models: openai::models(),
};

const GOOGLE: ProviderManifest = ProviderManifest {
    slug: "google",
    display_name: "Google",
    family: ModelFamily::Gemini,
    supports_thinking: true,
    accepts_arbitrary_models: true,
    fallback_max_output: Some(65_536),
    fallback_context_window: 1_000_000,
    models: google::models(),
};

const OLLAMA: ProviderManifest = ProviderManifest {
    slug: "ollama",
    display_name: "Ollama",
    family: ModelFamily::Generic,
    supports_thinking: false,
    accepts_arbitrary_models: true,
    fallback_max_output: Some(16_384),
    fallback_context_window: 128_000,
    models: &[],
};

const LLAMA_CPP: ProviderManifest = ProviderManifest {
    slug: "llama-cpp",
    display_name: "LlamaCpp",
    family: ModelFamily::Generic,
    supports_thinking: true,
    accepts_arbitrary_models: true,
    fallback_max_output: None,
    fallback_context_window: 128_000,
    models: &[],
};

/// Protocol bases left from the native providers, removed in commit 9. They
/// resolve by slug but are not providers themselves.
const PROTOCOL_BASES: &[ProviderManifest] = &[ANTHROPIC, OPENAI, GOOGLE, OLLAMA, LLAMA_CPP];

pub struct ManifestRegistry;

impl ManifestRegistry {
    pub fn get(slug: &str) -> Option<&'static ProviderManifest> {
        PROTOCOL_BASES.iter().find(|m| m.slug == slug)
    }

    /// Kept apart from `get` until the protocol bases go in commit 9.
    pub fn for_slug(slug: &str) -> Option<&'static ProviderManifest> {
        Self::get(slug)
    }

    pub fn find_default_for_tier(slug: &str, tier: ModelTier) -> Option<&'static ModelEntry> {
        Self::for_slug(slug)?
            .models
            .iter()
            .find(|e| e.default && e.tier == tier)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::ProviderKind;
    use strum::IntoEnumIterator;

    #[test]
    fn for_slug_returns_none_for_unknown_slug() {
        assert!(ManifestRegistry::for_slug("totally-unknown-slug").is_none());
    }

    #[test]
    fn for_slug_returns_builtin_directly() {
        let manifest = ManifestRegistry::for_slug("anthropic").unwrap();
        assert_eq!(manifest.slug, "anthropic");
        assert_eq!(manifest.display_name, "Anthropic");
    }

    #[test]
    fn every_provider_kind_has_a_manifest() {
        for kind in ProviderKind::iter() {
            let manifest = ManifestRegistry::get(&kind.to_string())
                .unwrap_or_else(|| panic!("ProviderKind variant {kind:?} has no manifest"));
            assert_eq!(manifest.display_name, kind.display_name());
            assert_eq!(manifest.family, kind.family());
            assert_eq!(manifest.fallback_max_output, kind.fallback_max_output());
            assert_eq!(
                manifest.fallback_context_window,
                kind.fallback_context_window()
            );
        }
    }
}
