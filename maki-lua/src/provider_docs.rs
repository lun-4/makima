use std::cell::Cell;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use maki_providers::model::ModelInfo;
use mlua::{Error, Lua, Result, Table, Value, VmState};

use crate::provider_staging::model_descriptors;

const MEMORY_LIMIT: usize = 32 * 1024 * 1024;
const INTERRUPT_LIMIT: usize = 100_000;

pub struct OfflineProvider {
    pub id: String,
    pub display_name: String,
    pub models: Vec<ModelInfo>,
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
            let id: String = table.raw_get("id")?;
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
            let display_name: Option<String> = table.raw_get("display_name")?;
            let models = model_descriptors(&table).map_err(Error::runtime)?;
            registered
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(OfflineProvider {
                    display_name: display_name.unwrap_or_else(|| id.clone()),
                    id,
                    models,
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
    use super::{load_bundled_providers, load_sources};

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
            load_sources(&[("test", source)]).unwrap()[0].models[0].id,
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
