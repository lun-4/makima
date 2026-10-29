use std::cell::Cell;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use maki_providers::model::{ModelInfo, ModelTier};
use mlua::{Error, Lua, Result, Table, Value, VmState};

use crate::provider_staging::model_descriptors;

const MEMORY_LIMIT: usize = 32 * 1024 * 1024;
const INTERRUPT_LIMIT: usize = 100_000;

pub struct OfflineProvider {
    pub id: String,
    pub display_name: String,
    pub models: Vec<OfflineModel>,
    pub docs_notes: Vec<String>,
    pub docs: Option<OfflineProviderDocs>,
}

pub struct OfflineProviderDocs {
    pub auth_line: String,
    pub urls: Vec<String>,
    pub features: Option<String>,
    pub no_catalog_note: Option<String>,
}

pub struct OfflineModel {
    pub info: ModelInfo,
    pub prefixes: Vec<String>,
    pub tier: Option<ModelTier>,
    pub default: bool,
}

fn module_source(name: &str) -> Option<&'static str> {
    match name {
        "maki.anthropic_direct" => {
            Some(include_str!("../../plugins/lib/maki/anthropic_direct.lua"))
        }
        "maki.anthropic" => Some(include_str!("../../plugins/lib/maki/anthropic.lua")),
        "maki.sse" => Some(include_str!("../../plugins/lib/maki/sse.lua")),
        "maki.chat_completions" => {
            Some(include_str!("../../plugins/lib/maki/chat_completions.lua"))
        }
        "maki.gemini" => Some(include_str!("../../plugins/lib/maki/gemini.lua")),
        "maki.mistral" => Some(include_str!("../../plugins/lib/maki/mistral.lua")),
        "maki.synthetic" => Some(include_str!("../../plugins/lib/maki/synthetic.lua")),
        "maki.provider_types" => Some(include_str!("../../plugins/lib/maki/provider_types.lua")),
        _ => None,
    }
}

fn optional_string(table: &Table, field: &str) -> Result<Option<String>> {
    match table.raw_get::<Value>(field)? {
        Value::Nil => Ok(None),
        Value::String(value) => Ok(Some(value.to_str()?.to_string())),
        _ => Err(Error::runtime(format!("{field}: expected a string"))),
    }
}

fn string_array(table: &Table, field: &str) -> Result<Option<Vec<String>>> {
    let value = match table.raw_get::<Value>(field)? {
        Value::Nil => return Ok(None),
        Value::Table(value) => value,
        _ => {
            return Err(Error::runtime(format!(
                "{field}: expected an array of strings"
            )));
        }
    };
    let mut entries = Vec::new();
    for pair in value.pairs::<Value, Value>() {
        let (key, item) = pair?;
        match (key, item) {
            (Value::Integer(index), Value::String(item)) if index > 0 => {
                entries.push((index, item.to_str()?.to_string()));
            }
            _ => {
                return Err(Error::runtime(format!(
                    "{field}: expected an array of strings"
                )));
            }
        }
    }
    entries.sort_unstable_by_key(|(index, _)| *index);
    if entries
        .iter()
        .enumerate()
        .any(|(index, (key, _))| *key as usize != index + 1)
    {
        return Err(Error::runtime(format!(
            "{field}: expected a contiguous array"
        )));
    }
    Ok(Some(entries.into_iter().map(|(_, value)| value).collect()))
}

fn load_sources(sources: &[(&str, &str)]) -> Result<Vec<OfflineProvider>> {
    let lua = Lua::new();
    lua.set_memory_limit(MEMORY_LIMIT)?;
    let ticks = Cell::new(0usize);
    lua.set_interrupt(move |_| {
        let count = ticks.get() + 1;
        ticks.set(count);
        if count > INTERRUPT_LIMIT {
            Err(Error::runtime("offline provider execution limit exceeded"))
        } else {
            Ok(VmState::Continue)
        }
    });
    let globals = lua.globals();
    for name in ["require", "io", "os", "package", "debug"] {
        globals.set(name, Value::Nil)?;
    }
    let loaded = lua.create_table()?;
    let loading = Arc::new(Mutex::new(BTreeSet::<String>::new()));
    let require = lua.create_function(move |lua, name: String| {
        let cached: Value = loaded.raw_get(name.as_str())?;
        if !matches!(cached, Value::Nil) {
            return Ok(cached);
        }
        let source = module_source(&name)
            .ok_or_else(|| Error::runtime(format!("offline module not allowed: {name}")))?;
        if !loading
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(name.clone())
        {
            return Err(Error::runtime(format!("offline module cycle: {name}")));
        }
        let result = lua.load(source).set_name(&name).eval::<Value>();
        loading
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&name);
        let value = result?;
        loaded.raw_set(name, value.clone())?;
        Ok(value)
    })?;
    globals.set("require", require)?;
    let providers = Arc::new(Mutex::new(Vec::<OfflineProvider>::new()));
    let registered = Arc::clone(&providers);
    let api = lua.create_table()?;
    api.set(
        "register_provider",
        lua.create_function(move |_, table: Table| {
            let id = optional_string(&table, "id")?
                .ok_or_else(|| Error::runtime("id: expected a string"))?;
            if id.trim().is_empty()
                || registered
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .iter()
                    .any(|p| p.id == id)
            {
                return Err(Error::runtime(format!(
                    "invalid or duplicate provider id: {id}"
                )));
            }
            let display_name = optional_string(&table, "display_name")?;
            let descriptors = model_descriptors(&table).map_err(Error::runtime)?;
            let model_table: Option<Table> = table.raw_get("models")?;
            let mut models = Vec::with_capacity(descriptors.len());
            for (index, info) in descriptors.into_iter().enumerate() {
                let table = model_table
                    .as_ref()
                    .ok_or_else(|| Error::runtime("models: expected an array"))?;
                let model: Table = table.raw_get(index + 1)?;
                let prefixes = string_array(&model, "prefixes")?;
                let tier = optional_string(&model, "tier")?;
                let tier = tier
                    .map(|tier| tier.parse::<ModelTier>().map_err(Error::external))
                    .transpose()?;
                let default = match model.raw_get::<Value>("default")? {
                    Value::Nil => false,
                    Value::Boolean(value) => value,
                    _ => return Err(Error::runtime("model default: expected a boolean")),
                };
                let prefixes = prefixes.unwrap_or_else(|| vec![info.id.clone()]);
                models.push(OfflineModel {
                    info,
                    prefixes,
                    tier,
                    default,
                });
            }
            let docs_notes = string_array(&table, "docs_notes")?;
            let docs = match table.raw_get::<Value>("docs")? {
                Value::Nil => None,
                Value::Table(docs) => Some(OfflineProviderDocs {
                    auth_line: optional_string(&docs, "auth_line")?
                        .ok_or_else(|| Error::runtime("auth_line: expected a string"))?,
                    urls: string_array(&docs, "urls")?
                        .ok_or_else(|| Error::runtime("urls: expected an array of strings"))?,
                    features: optional_string(&docs, "features")?,
                    no_catalog_note: optional_string(&docs, "no_catalog_note")?,
                }),
                _ => return Err(Error::runtime("docs: expected a table")),
            };
            registered
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(OfflineProvider {
                    display_name: display_name.unwrap_or_else(|| id.clone()),
                    id,
                    models,
                    docs_notes: docs_notes.unwrap_or_default(),
                    docs,
                });
            Ok(())
        })?,
    )?;
    let maki = lua.create_table()?;
    maki.set("api", api)?;
    globals.set("maki", maki)?;
    lua.sandbox(true)?;
    for (name, source) in sources {
        lua.load(*source).set_name(*name).exec()?;
    }
    let result = std::mem::take(&mut *providers.lock().unwrap_or_else(|e| e.into_inner()));
    Ok(result)
}

pub fn load_bundled_providers() -> Result<Vec<OfflineProvider>> {
    load_sources(&[
        (
            "anthropic",
            include_str!("../../plugins/anthropic/init.lua"),
        ),
        ("deepseek", include_str!("../../plugins/deepseek/init.lua")),
        ("google", include_str!("../../plugins/google/init.lua")),
        ("mistral", include_str!("../../plugins/mistral/init.lua")),
        (
            "synthetic",
            include_str!("../../plugins/synthetic/init.lua"),
        ),
    ])
}

#[cfg(test)]
mod tests {
    use maki_providers::model::ModelTier;

    use super::{load_bundled_providers, load_sources};

    #[test]
    fn preserves_documentation_metadata() {
        let source = r#"
            maki.api.register_provider({
                id = 'test', docs_notes = {'first', 'second'},
                models = {
                    {id = 'one', prefixes = {'one-', 'alias-'}, tier = 'strong', default = true},
                    {id = 'two'},
                    {id = 'three', prefixes = {}},
                },
            })
        "#;
        let providers = load_sources(&[("test", source)]).unwrap();
        let provider = &providers[0];
        assert_eq!(provider.docs_notes, ["first", "second"]);
        assert_eq!(provider.models[0].info.id, "one");
        assert_eq!(provider.models[0].prefixes, ["one-", "alias-"]);
        assert_eq!(provider.models[0].tier, Some(ModelTier::Strong));
        assert!(provider.models[0].default);
        assert_eq!(provider.models[1].info.id, "two");
        assert_eq!(provider.models[1].prefixes, ["two"]);
        assert_eq!(provider.models[1].tier, None);
        assert!(!provider.models[1].default);
        assert!(provider.models[2].prefixes.is_empty());
    }

    #[test]
    fn rejects_invalid_documentation_metadata() {
        for field in ["tier = 'unknown'", "default = 'yes'", "prefixes = {false}"] {
            let source = format!(
                "maki.api.register_provider({{id='test', models={{{{id='one', {field}}}}}}})"
            );
            assert!(load_sources(&[("test", &source)]).is_err(), "{field}");
        }
        assert!(
            load_sources(&[(
                "test",
                "maki.api.register_provider({id='test', docs_notes={false}})"
            )])
            .is_err()
        );
    }

    #[test]
    fn validates_docs_raw_without_metamethods() {
        let source = r#"
            local trap = {__index = function() error('index called') end,
                          __len = function() error('len called') end,
                          __iter = function() error('iter called') end}
            maki.api.register_provider({id='test', docs=setmetatable({
                auth_line='API key', urls=setmetatable({'https://example.com', 'https://example.org'}, trap),
                features='streaming', no_catalog_note='No catalog',
            }, trap)})
            maki.api.register_provider({id='minimal', docs={auth_line='', urls={}}})
            maki.api.register_provider({id='absent'})
        "#;
        let providers = load_sources(&[("test", source)]).unwrap();
        let docs = providers[0].docs.as_ref().unwrap();
        assert_eq!(docs.auth_line, "API key");
        assert_eq!(docs.urls, ["https://example.com", "https://example.org"]);
        assert_eq!(docs.features.as_deref(), Some("streaming"));
        assert_eq!(docs.no_catalog_note.as_deref(), Some("No catalog"));
        let docs = providers[1].docs.as_ref().unwrap();
        assert!(docs.urls.is_empty());
        assert!(docs.features.is_none());
        assert!(docs.no_catalog_note.is_none());
        assert!(providers[2].docs.is_none());
    }

    #[test]
    fn rejects_malformed_raw_metadata() {
        for field in [
            "docs=false",
            "docs={}",
            "docs={auth_line=1, urls={}}",
            "docs={auth_line='key', urls=false}",
            "docs={auth_line='key', urls={1}}",
            "docs={auth_line='key', urls={[2]='url'}}",
            "docs={auth_line='key', urls={'url', extra='url'}}",
            "docs={auth_line='key', urls={}, features=false}",
            "docs={auth_line='key', urls={}, no_catalog_note=1}",
            "docs=setmetatable({}, {__index={auth_line='key', urls={}}})",
            "docs_notes={[2]='note'}",
            "docs_notes={1}",
            "display_name=1",
            "models={{id='one', prefixes={[2]='prefix'}}}",
            "models={{id='one', tier=1}}",
        ] {
            let source = format!("maki.api.register_provider({{id='test', {field}}})");
            assert!(load_sources(&[("test", &source)]).is_err(), "{field}");
        }
    }

    #[test]
    fn bundled_catalogs_load() {
        let providers = load_bundled_providers().unwrap();
        assert!(
            providers
                .iter()
                .any(|p| p.id == "deepseek" && !p.models.is_empty())
        );
        assert!(
            providers
                .iter()
                .any(|p| p.id == "synthetic" && !p.models.is_empty())
        );
    }

    #[test]
    fn denies_side_effects_and_unknown_modules() {
        for source in [
            "return os.getenv('HOME')",
            "return require('outside')",
            "return maki.uv.os_getenv('HOME')",
            "while true do end",
        ] {
            assert!(load_sources(&[("test", source)]).is_err());
        }
    }

    #[test]
    fn validates_without_invoking_callbacks() {
        let source = "maki.api.register_provider({id='test', models={{id='model'}}, prepare=function() error('called') end, discover=function() error('called') end})";
        assert_eq!(
            load_sources(&[("test", source)]).unwrap()[0].models[0]
                .info
                .id,
            "model"
        );
        assert!(
            load_sources(&[(
                "test",
                "maki.api.register_provider({id='test', models={{id=''}}})"
            )])
            .is_err()
        );
    }
}
