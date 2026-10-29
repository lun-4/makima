use std::collections::{BTreeMap, BTreeSet};

use maki_providers::model::{FastPricing, ModelInfo, ModelPricing, validate_model_descriptors};
use mlua::{Table, Value};

use crate::loader::SourceIdentity;

#[derive(Default)]
pub(crate) struct ProviderStaging {
    entries: BTreeMap<String, (String, u64, SourceIdentity, Table)>,
}

impl ProviderStaging {
    pub(crate) fn validate(
        &self,
        owner: &str,
        candidate: &[(String, Table)],
    ) -> Result<(), String> {
        let mut replacement = BTreeSet::new();
        for (id, table) in candidate {
            if id.trim().is_empty() {
                return Err("provider id must not be empty".to_owned());
            }
            if !replacement.insert(id)
                || self
                    .entries
                    .get(id)
                    .is_some_and(|(existing, _, _, _)| existing != owner)
            {
                return Err(format!("duplicate provider id: {id}"));
            }
            validate_models(table)
                .map_err(|error| format!("plugin {owner}, provider {id}: {error}"))?;
        }
        Ok(())
    }

    pub(crate) fn replace(
        &mut self,
        owner: &str,
        generation: u64,
        identity: SourceIdentity,
        candidate: Vec<(String, Table)>,
    ) {
        self.unload(owner);
        self.entries.extend(
            candidate
                .into_iter()
                .map(|(id, table)| (id, (owner.to_owned(), generation, identity.clone(), table))),
        );
    }

    pub(crate) fn unload(&mut self, owner: &str) {
        self.entries
            .retain(|_, (existing, _, _, _)| existing != owner);
    }

    pub(crate) fn get(&self, id: &str) -> Option<(&str, u64, &SourceIdentity, &Table)> {
        self.entries
            .get(id)
            .map(|(owner, generation, identity, table)| {
                (owner.as_str(), *generation, identity, table)
            })
    }
}

pub(crate) fn auth_methods(
    provider: &Table,
) -> Result<Vec<maki_providers::provider_registry::ProviderAuthMethod>, String> {
    for hook in ["auth_status", "logout"] {
        match provider.raw_get::<Value>(hook).map_err(|e| e.to_string())? {
            Value::Nil | Value::Function(_) => {}
            _ => return Err(format!("{hook}: expected a function")),
        }
    }
    let methods = match provider
        .raw_get::<Value>("auth")
        .map_err(|e| e.to_string())?
    {
        Value::Nil => return Ok(Vec::new()),
        Value::Table(table) => table,
        _ => return Err("auth: expected an array".into()),
    };
    let mut entries = Vec::new();
    for entry in methods.pairs::<Value, Value>() {
        let (key, value) = entry.map_err(|e| e.to_string())?;
        let Value::Integer(index) = key else {
            return Err("auth: expected an array".into());
        };
        entries.push((index, value));
    }
    entries.sort_by_key(|(index, _)| *index);
    let mut ids = BTreeSet::new();
    let mut result = Vec::new();
    for (position, (index, value)) in entries.into_iter().enumerate() {
        if index != (position + 1) as i64 {
            return Err("auth: expected consecutive array keys".into());
        }
        let Value::Table(entry) = value else {
            return Err(format!("auth[{index}]: expected a table"));
        };
        let id: String = entry
            .raw_get("id")
            .map_err(|e| format!("auth[{index}].id: {e}"))?;
        let label: String = entry
            .raw_get("label")
            .map_err(|e| format!("auth[{index}].label: {e}"))?;
        let _: mlua::Function = entry
            .raw_get("login")
            .map_err(|e| format!("auth[{index}].login: {e}"))?;
        if id.trim().is_empty() || label.trim().is_empty() || !ids.insert(id.clone()) {
            return Err(format!("auth[{index}]: empty or duplicate method identity"));
        }
        result.push(maki_providers::provider_registry::ProviderAuthMethod { id, label });
    }
    Ok(result)
}

fn validate_models(provider: &Table) -> Result<(), String> {
    model_descriptors(provider).map(|_| ())
}

pub(crate) fn model_descriptors(provider: &Table) -> Result<Vec<ModelInfo>, String> {
    let models: Value = provider.raw_get("models").map_err(|e| e.to_string())?;
    let models = match models {
        Value::Nil => return Ok(Vec::new()),
        Value::Table(models) => models,
        _ => return Err("models: expected an array".into()),
    };
    parse_model_array(models)
}

pub(crate) fn parse_model_array(models: Table) -> Result<Vec<ModelInfo>, String> {
    let mut descriptors = Vec::new();
    let mut entries = Vec::new();
    for entry in models.pairs::<Value, Value>() {
        let (key, value) = entry.map_err(|e| format!("models: {e}"))?;
        let Value::Integer(index) = key else {
            return Err("models: expected an array with consecutive integer keys".into());
        };
        if index <= 0 {
            return Err("models: expected an array with consecutive integer keys".into());
        }
        entries.push((index, value));
    }
    entries.sort_by_key(|(index, _)| *index);
    for (position, (index, value)) in entries.into_iter().enumerate() {
        if index != (position + 1) as i64 {
            return Err("models: expected an array with consecutive integer keys".into());
        }
        let path = format!("models[{index}]");
        let Value::Table(model) = value else {
            return Err(format!("{path}: expected a table"));
        };
        let id = match model
            .raw_get::<Value>("id")
            .map_err(|e| format!("{path}.id: {e}"))?
        {
            Value::String(id) => id
                .to_str()
                .map_err(|e| format!("{path}.id: {e}"))?
                .to_string(),
            _ => return Err(format!("{path}.id: expected a string")),
        };
        let mut descriptor = ModelInfo::id_only(id);
        for (field, alias, target) in [
            ("vision", "supports_vision", &mut descriptor.supports_vision),
            (
                "supports_thinking",
                "supports_thinking",
                &mut descriptor.supports_thinking,
            ),
        ] {
            let read = |name: &str| -> Result<Option<bool>, String> {
                match model
                    .raw_get::<Value>(name)
                    .map_err(|e| format!("{path}.{name}: {e}"))?
                {
                    Value::Nil => Ok(None),
                    Value::Boolean(value) => Ok(Some(value)),
                    _ => Err(format!("{path}.{name}: expected a boolean")),
                }
            };
            let primary = read(field)?;
            let alternate = read(alias)?;
            if primary.is_some() && alternate.is_some() && primary != alternate {
                return Err(format!("{path}.{field}: conflicts with {alias}"));
            }
            *target = primary.or(alternate);
        }
        for (field, target) in [
            ("context_window", &mut descriptor.context_window),
            ("max_output_tokens", &mut descriptor.max_output_tokens),
        ] {
            let value: Value = model
                .raw_get(field)
                .map_err(|e| format!("{path}.{field}: {e}"))?;
            *target = match value {
                Value::Nil => None,
                Value::Integer(n) if n > 0 && n <= u32::MAX as i64 => Some(n as u32),
                Value::Number(n)
                    if n.is_finite() && n > 0.0 && n <= u32::MAX as f64 && n.fract() == 0.0 =>
                {
                    Some(n as u32)
                }
                _ => {
                    return Err(format!(
                        "{path}.{field}: expected a positive integer within u32 range"
                    ));
                }
            };
        }
        let pricing: Value = model
            .raw_get("pricing")
            .map_err(|e| format!("{path}.pricing: {e}"))?;
        descriptor.pricing = match pricing {
            Value::Nil => None,
            Value::Table(pricing) => {
                let rate = |table: &Table, field: &str, prefix: &str| -> Result<f64, String> {
                    match table
                        .raw_get::<Value>(field)
                        .map_err(|e| format!("{prefix}.{field}: {e}"))?
                    {
                        Value::Integer(n) => Ok(n as f64),
                        Value::Number(n) => Ok(n),
                        _ => Err(format!("{prefix}.{field}: expected a number")),
                    }
                };
                let prefix = format!("{path}.pricing");
                let fast = match pricing
                    .raw_get::<Value>("fast")
                    .map_err(|e| format!("{prefix}.fast: {e}"))?
                {
                    Value::Nil => None,
                    Value::Table(fast) => Some(FastPricing {
                        input: rate(&fast, "input", &format!("{prefix}.fast"))?,
                        output: rate(&fast, "output", &format!("{prefix}.fast"))?,
                    }),
                    _ => return Err(format!("{prefix}.fast: expected a table")),
                };
                Some(ModelPricing {
                    input: rate(&pricing, "input", &prefix)?,
                    output: rate(&pricing, "output", &prefix)?,
                    cache_write: rate(&pricing, "cache_write", &prefix)?,
                    cache_read: rate(&pricing, "cache_read", &prefix)?,
                    fast,
                })
            }
            _ => return Err(format!("{path}.pricing: expected a table")),
        };
        descriptors.push(descriptor);
    }
    validate_model_descriptors(&descriptors).map_err(|error| {
        format!(
            "models[{}] ({:?}).{}: {}",
            error.index + 1,
            error.id,
            error.field,
            error.reason
        )
    })?;
    Ok(descriptors)
}

#[cfg(test)]
mod tests {
    use mlua::Lua;
    use test_case::test_case;

    use super::{ProviderStaging, SourceIdentity, auth_methods, model_descriptors};

    #[test_case("{ models = { named = { id = 'model' } } }", "models: expected an array with consecutive integer keys"; "non_integer_key")]
    #[test_case("{ models = { [0] = { id = 'model' } } }", "models: expected an array with consecutive integer keys"; "zero_index")]
    #[test_case("{ models = { [1] = { id = 'model' }, [3] = { id = 'other' } } }", "models: expected an array with consecutive integer keys"; "sparse_array")]
    #[test_case("{ models = { false } }", "models[1]: expected a table"; "non_table_model")]
    #[test_case("{ models = {{ id = 'model', max_output_tokens = 4294967296 }} }", "models[1].max_output_tokens: expected a positive integer within u32 range"; "token_limit_overflow")]
    #[test_case("{ models = {{ id = 'model', pricing = { input = 0, output = 0, cache_write = 0, cache_read = 0, fast = { input = 1 } } }} }", "models[1].pricing.fast.output: expected a number"; "incomplete_fast_pricing")]
    fn malformed_descriptors_report_field(source: &str, expected: &str) -> mlua::Result<()> {
        let lua = Lua::new();
        let provider = lua.load(format!("return {source}")).eval()?;
        assert_eq!(model_descriptors(&provider).unwrap_err(), expected);
        Ok(())
    }

    #[test]
    fn normalized_descriptors_preserve_staged_metadata() -> mlua::Result<()> {
        let lua = Lua::new();
        let provider = lua.load("return { models = {{ id = 'model', context_window = 8192, pricing = { input = 1, output = 2, cache_write = 0, cache_read = 0 } }} }").eval()?;
        let models = model_descriptors(&provider).unwrap();
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].id, "model");
        assert_eq!(models[0].context_window, Some(8192));
        assert_eq!(models[0].pricing.as_ref().unwrap().output, 2.0);
        Ok(())
    }

    #[test]
    fn invalid_models_do_not_replace_staged_provider() -> mlua::Result<()> {
        let lua = Lua::new();
        let mut staging = ProviderStaging::default();
        let original = lua.create_table()?;
        original.set("marker", 1)?;
        staging.replace(
            "plugin",
            1,
            SourceIdentity::Other,
            vec![("provider".into(), original)],
        );
        for source in [
            "{ models = {{ id = 'same' }, { id = 'same' }} }",
            "{ models = {{ id = 'model', context_window = 1.5 }} }",
            "{ models = {{ id = 'model', pricing = { input = -1, output = 0, cache_write = 0, cache_read = 0 } }} }",
            "{ models = { [2] = { id = 'model' } } }",
        ] {
            let candidate = lua.load(format!("return {source}")).eval()?;
            let error = staging
                .validate("plugin", &[("provider".into(), candidate)])
                .unwrap_err();
            assert!(error.contains("plugin plugin, provider provider: models"));
            assert_eq!(staging.get("provider").unwrap().3.get::<i32>("marker")?, 1);
        }
        Ok(())
    }

    #[test]
    fn auth_metadata_rejects_invalid_or_duplicate_methods() -> mlua::Result<()> {
        let lua = Lua::new();
        for source in [
            "{ auth = {{ id = 'key', label = 'Key', login = function() end }, { id = 'key', label = 'Other', login = function() end }} }",
            "{ auth = {{ id = 'key', label = 'Key' }} }",
            "{ auth = { [2] = { id = 'key', label = 'Key', login = function() end } } }",
            "{ auth_status = true }",
        ] {
            let provider = lua.load(format!("return {source}")).eval()?;
            assert!(auth_methods(&provider).is_err(), "{source}");
        }
        Ok(())
    }

    #[test]
    fn replacement_is_atomic_and_owned() -> mlua::Result<()> {
        let lua = Lua::new();
        let mut staging = ProviderStaging::default();
        let first = lua.create_table()?;
        first.set("marker", 1)?;
        staging.replace(
            "a",
            1,
            SourceIdentity::Embedded("a"),
            vec![("one".into(), first)],
        );
        staging.replace(
            "b",
            1,
            SourceIdentity::Other,
            vec![("two".into(), lua.create_table()?)],
        );
        assert!(
            staging
                .validate("a", &[("two".into(), lua.create_table()?)])
                .is_err()
        );
        assert_eq!(staging.get("one").unwrap().3.get::<i32>("marker")?, 1);
        assert!(
            staging
                .validate(
                    "a",
                    &[
                        ("new".into(), lua.create_table()?),
                        ("new".into(), lua.create_table()?)
                    ]
                )
                .is_err()
        );
        assert!(staging.get("new").is_none());
        assert_eq!(staging.get("one").unwrap().1, 1);
        assert_eq!(
            staging.get("one").unwrap().2,
            &SourceIdentity::Embedded("a")
        );
        staging.replace(
            "a",
            2,
            SourceIdentity::Other,
            vec![("new".into(), lua.create_table()?)],
        );
        assert_eq!(staging.get("new").unwrap().1, 2);
        assert_eq!(staging.get("new").unwrap().2, &SourceIdentity::Other);
        assert!(staging.get("one").is_none());
        staging.unload("a");
        assert!(staging.get("new").is_none());
        assert_eq!(staging.get("two").unwrap().0, "b");
        Ok(())
    }
}
