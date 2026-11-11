use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::process;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use tracing::debug;

use makima_storage::paths;

const PROVIDERS_FILE: &str = "providers.toml";
const BAD_CONFIG_EXIT_CODE: i32 = 2;

/// One provider's `providers.toml` section. Its fields belong to the
/// provider that reads them, so the host keeps the table as written.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProviderDef(pub toml::Table);

impl ProviderDef {
    /// {answers} applied as text fields.
    pub fn with_answers(&self, answers: &[(String, String)]) -> Self {
        let mut table = self.0.clone();
        for (key, value) in answers {
            table.insert(key.clone(), toml::Value::String(value.clone()));
        }
        Self(table)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProvidersConfig {
    #[serde(flatten)]
    pub providers: HashMap<String, ProviderDef>,
}

impl ProvidersConfig {
    /// Read and parse `providers.toml`. Hard-exits on parse errors so a
    /// broken file surfaces immediately instead of silently dropping every
    /// provider and starting makima with an empty registry.
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

/// When `providers.toml` last changed and how long it is, `None` while it
/// does not exist, for a reader that keeps the parsed file until it changes.
pub fn file_stamp() -> Option<(SystemTime, u64)> {
    let metadata = fs::metadata(providers_file_path()).ok()?;
    Some((metadata.modified().ok()?, metadata.len()))
}

fn providers_file_path() -> PathBuf {
    paths::config_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join(PROVIDERS_FILE)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECTION: &str =
        "base_url = \"https://x\"\ndiscover_models = true\n[headers]\nX-Id = \"${ID}\"\n";
    const BASE_URL: &str = "base_url";
    const ANSWER: &str = "https://y";

    #[test]
    fn sections_round_trip_as_written() {
        let mut config = ProvidersConfig::default();
        config.upsert("custom".into(), toml::from_str(SECTION).unwrap());
        let parsed: ProvidersConfig =
            toml::from_str(&toml::to_string_pretty(&config).unwrap()).unwrap();
        assert_eq!(parsed.get("custom"), config.get("custom"));
    }

    #[test]
    fn with_answers_replaces_text_fields_and_keeps_the_rest() {
        let def: ProviderDef = toml::from_str(SECTION).unwrap();
        let answered = def.with_answers(&[(BASE_URL.into(), ANSWER.into())]);
        assert_eq!(answered.0[BASE_URL].as_str(), Some(ANSWER));
        assert_eq!(answered.0.len(), def.0.len());
    }
}
