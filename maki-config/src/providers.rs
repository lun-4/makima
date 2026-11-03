use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::PathBuf;
use std::process;

use serde::{Deserialize, Serialize};
use tracing::debug;

use maki_storage::paths;

const PROVIDERS_FILE: &str = "providers.toml";
const BAD_CONFIG_EXIT_CODE: i32 = 2;
const NOT_TEXT_FIELD_ERR: &str = "is not a text field of a provider";

/// Coarse capability classification used by maki-providers to dispatch tiered
/// requests. Mirrors `maki_providers::ModelTier` shape but lives here so the
/// config layer can validate inputs without depending on maki-providers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    Weak,
    #[default]
    Medium,
    Strong,
    Compaction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelDef {
    pub id: String,
    #[serde(default)]
    pub tier: Tier,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_tool_examples: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_thinking: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requires_thinking: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_vision: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pricing_input: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pricing_output: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pricing_cache_write: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pricing_cache_read: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pricing_fast_input: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pricing_fast_output: Option<f64>,
}

impl ModelDef {
    /// Any pricing field set means the user provided pricing (other fields default to 0).
    pub fn has_pricing(&self) -> bool {
        self.pricing_input.is_some()
            || self.pricing_output.is_some()
            || self.pricing_cache_write.is_some()
            || self.pricing_cache_read.is_some()
    }

    pub fn has_fast_pricing(&self) -> bool {
        self.pricing_fast_input.is_some() || self.pricing_fast_output.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Protocol {
    Openai,
    OpenaiResponses,
    Anthropic,
    Google,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OverrideFields {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_thinking: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_vision: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base: Option<String>,
    /// Path prefix sent to the gateway, replacing the default (`/v1`, or
    /// `/v1beta` for Gemini routes). Set it to `""` when the upstream's base
    /// url already carries its own path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_prefix: Option<String>,
}

/// Overrides for a single gateway provider (Aperture), keyed by its id (e.g.
/// `zai`, `ollama`, `ikora-openai`). Provider-level fields apply to every model
/// from that provider; `models` refine individual models.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProviderOverride {
    #[serde(flatten)]
    pub default: OverrideFields,
    #[serde(default)]
    pub models: HashMap<String, OverrideFields>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProviderDef {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<Protocol>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key_env: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub discover_models: bool,
    /// Extra HTTP headers sent with every request to this provider. Values
    /// expand `${VAR}` from the environment, so gateway credentials (e.g.
    /// Cloudflare Access service tokens in front of a private endpoint) never
    /// land in the config file:
    ///
    /// ```toml
    /// [anthropic]
    /// base_url = "https://gw.internal/anthropic"
    /// [anthropic.headers]
    /// CF-Access-Client-Id = "${CF_ACCESS_CLIENT_ID}"
    /// CF-Access-Client-Secret = "${CF_ACCESS_CLIENT_SECRET}"
    /// ```
    ///
    /// A same-name header (case-insensitive) replaces the built-in auth
    /// header instead of appending, and keeps winning across key rotations.
    /// An unset or empty variable fails the whole provider so callers see the
    /// missing name instead of a silent 401.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    /// Opencode Zen and Go: show free catalog models, reachable without a
    /// key through the public fallback. Defaults to `false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enable_free_models: Option<bool>,
    /// Aperture-only: per-gateway-provider overrides for the providers it
    /// routes to.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub overrides: HashMap<String, ProviderOverride>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<ModelDef>,
}

impl ProviderDef {
    /// {answers} applied as string fields. Fails when a key is not a string
    /// field of `ProviderDef`, so a misspelt key cannot vanish on save.
    pub fn with_answers(&self, answers: &[(String, String)]) -> Result<Self, String> {
        let mut table = toml::Table::try_from(self).map_err(|e| e.to_string())?;
        for (key, value) in answers {
            table.insert(key.clone(), toml::Value::String(value.clone()));
        }
        let def: Self = table
            .try_into()
            .map_err(|e: toml::de::Error| e.to_string())?;
        let stored = toml::Table::try_from(&def).map_err(|e| e.to_string())?;
        match answers
            .iter()
            .find(|(key, value)| stored.get(key).and_then(toml::Value::as_str) != Some(value))
        {
            Some((key, _)) => Err(format!("{key:?} {NOT_TEXT_FIELD_ERR}")),
            None => Ok(def),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProvidersConfig {
    #[serde(flatten)]
    pub providers: HashMap<String, ProviderDef>,
}

impl ProvidersConfig {
    /// Read and parse `providers.toml`. Hard-exits on parse errors so a typo
    /// in tier or pricing surfaces immediately instead of silently dropping
    /// every provider and starting maki with an empty registry.
    pub fn load() -> Self {
        Self::try_load().unwrap_or_else(|e| {
            eprintln!("error: {e}");
            process::exit(BAD_CONFIG_EXIT_CODE);
        })
    }

    /// Same read, but a typo only costs the answer. For callers past startup,
    /// where taking the process down mid-session is never the right trade.
    pub fn load_or_default() -> Self {
        Self::try_load().unwrap_or_else(|e| {
            tracing::warn!(error = %e, "ignoring providers.toml");
            Self::default()
        })
    }

    /// Fails on a file that does not parse, so a caller about to save cannot
    /// overwrite it.
    pub fn try_load() -> Result<Self, String> {
        let path = providers_file_path();
        if !path.exists() {
            return Ok(Self::default());
        }
        let content = match fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "cannot read providers.toml");
                return Ok(Self::default());
            }
        };
        match toml::from_str::<ProvidersConfig>(&content) {
            Ok(config) => {
                debug!(path = %path.display(), "loaded providers config");
                Ok(config)
            }
            Err(e) => Err(format!("invalid {}: {e}", path.display())),
        }
    }

    pub fn save(&self) -> Result<(), std::io::Error> {
        let path = providers_file_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let content = toml::to_string_pretty(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        fs::write(&path, content)?;
        debug!(path = %path.display(), "saved providers config");
        Ok(())
    }

    pub fn get(&self, slug: &str) -> Option<&ProviderDef> {
        self.providers.get(slug)
    }

    pub fn upsert(&mut self, slug: String, def: ProviderDef) {
        self.providers.insert(slug, def);
    }

    pub fn remove(&mut self, slug: &str) -> bool {
        self.providers.remove(slug).is_some()
    }
}

fn providers_file_path() -> PathBuf {
    paths::config_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join(PROVIDERS_FILE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    #[test]
    fn provider_def_parses_custom_headers() {
        let def: ProviderDef = toml::from_str(
            "base_url = \"https://gw.example.com/v1\"\n[headers]\n\"CF-Access-Client-Id\" = \"${CF_ID}\"\n\"CF-Access-Client-Secret\" = \"${CF_SECRET}\"\n",
        )
        .unwrap();
        assert_eq!(def.headers.len(), 2);
        assert_eq!(def.headers["CF-Access-Client-Id"], "${CF_ID}");
    }

    #[test]
    fn provider_def_without_headers_is_empty() {
        let def: ProviderDef = toml::from_str("base_url = \"https://x\"\n").unwrap();
        assert!(def.headers.is_empty());
    }

    #[test]
    fn provider_def_roundtrip() {
        let mut config = ProvidersConfig::default();
        config.upsert(
            "my-provider".into(),
            ProviderDef {
                protocol: Some(Protocol::Openai),
                base_url: Some("https://api.example.com/v1".into()),
                api_key_env: Some("MY_API_KEY".into()),
                discover_models: true,
                enable_free_models: Some(false),
                ..Default::default()
            },
        );
        let toml_str = toml::to_string_pretty(&config).unwrap();
        let parsed: ProvidersConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(
            parsed.get("my-provider").unwrap().protocol,
            Some(Protocol::Openai)
        );
        assert_eq!(
            parsed.get("my-provider").unwrap().base_url,
            Some("https://api.example.com/v1".into())
        );
        assert_eq!(
            parsed.get("my-provider").unwrap().enable_free_models,
            Some(false)
        );
    }

    const EMPTY_PROVIDER_DEF_TOML: &str = "";

    #[test]
    fn provider_def_enable_free_models_defaults_none() {
        let def: ProviderDef = toml::from_str(EMPTY_PROVIDER_DEF_TOML).unwrap();
        assert_eq!(def.enable_free_models, None);
    }

    const UNKNOWN_TIER_TOML: &str = r#"id = "x"
tier = "mediums"
"#;

    #[test]
    fn model_def_rejects_unknown_tier() {
        let err = toml::from_str::<ModelDef>(UNKNOWN_TIER_TOML).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("medium"), "expected enum hint, got: {msg}");
    }

    #[test]
    fn model_def_tier_defaults_to_medium() {
        let m: ModelDef = toml::from_str(r#"id = "x""#).unwrap();
        assert_eq!(m.tier, Tier::Medium);
    }

    #[test_case("weak", Tier::Weak ; "weak")]
    #[test_case("medium", Tier::Medium ; "medium")]
    #[test_case("strong", Tier::Strong ; "strong")]
    #[test_case("compaction", Tier::Compaction ; "compaction")]
    fn model_def_tier_roundtrip(input: &str, expected: Tier) {
        let toml = format!(
            r#"id = "x"
tier = "{input}"
"#
        );
        let m: ModelDef = toml::from_str(&toml).unwrap();
        assert_eq!(m.tier, expected);
    }

    #[test_case("base_url", "https://example.com" ; "string_field")]
    #[test_case("protocol", "anthropic" ; "enum_field")]
    fn with_answers_sets_fields(key: &str, value: &str) {
        let def = ProviderDef::default()
            .with_answers(&[(key.into(), value.into())])
            .unwrap();
        let stored = toml::Table::try_from(&def).unwrap();
        assert_eq!(stored[key].as_str(), Some(value));
    }

    #[test_case("base_urll", Some(NOT_TEXT_FIELD_ERR) ; "unknown_key")]
    #[test_case("discover_models", None ; "non_string_key")]
    fn with_answers_rejects_unknown_and_non_string_keys(key: &str, expected: Option<&str>) {
        let error = ProviderDef::default()
            .with_answers(&[(key.into(), String::new())])
            .unwrap_err();
        if let Some(expected) = expected {
            assert!(error.contains(expected), "{error}");
        }
    }
}
