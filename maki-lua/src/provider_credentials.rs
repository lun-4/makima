use maki_agent::cancel::CancelToken;
use maki_storage::auth::{
    CredentialError, CredentialGeneration, CredentialLifecycle, CredentialScope,
    LegacyCredentialKind, RefreshOutcome,
};
use mlua::{Function, Lua, MultiValue, Table, Value};

use crate::api::util::convert::{json_to_lua_with_nulls, lua_to_json};
use crate::loader::SourceIdentity;
use crate::runtime::{TaskHandle, same_invocation};

fn error(lua: &Lua, message: &str) -> mlua::Result<Table> {
    let error = lua.create_table()?;
    error.set("kind", "config")?;
    error.set("message", message)?;
    Ok(error)
}

fn authorized(lua: &Lua, task: &TaskHandle, generation: &CancelToken) -> bool {
    !generation.is_cancelled() && same_invocation(lua, task)
}

fn scope(
    lua: &Lua,
    source: &SourceIdentity,
    provider: &str,
    key: &str,
) -> Result<CredentialScope, String> {
    let trusted = matches!(source, SourceIdentity::CoreProviders)
        || matches!(source, SourceIdentity::Embedded(name) if *name == provider && matches!(provider, "ollama" | "llama-cpp" | "synthetic" | "zai" | "deepseek" | "regolo" | "requesty" | "mistral" | "tensorx") && key == "api_key");
    if !trusted
        || key.is_empty()
        || !key
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
    {
        return Err("credential access denied".into());
    }
    let dir = crate::api::env::host_state_dir(lua)?;
    if matches!(source, SourceIdentity::CoreProviders) && provider == "openai" {
        let kind = match key {
            "chatgpt" => Some(LegacyCredentialKind::OAuth),
            "api_key" => Some(LegacyCredentialKind::ApiKey),
            _ => None,
        };
        if let Some(kind) = kind {
            return CredentialScope::legacy(dir, "openai", kind).map_err(|e| e.to_string());
        }
    }
    if (matches!(
        provider,
        "ollama"
            | "llama-cpp"
            | "synthetic"
            | "zai"
            | "deepseek"
            | "regolo"
            | "requesty"
            | "mistral"
            | "tensorx"
    ) || matches!(source, SourceIdentity::CoreProviders)
        && matches!(provider, "anthropic" | "google"))
        && key == "api_key"
    {
        return CredentialScope::legacy(dir, provider, LegacyCredentialKind::ApiKey)
            .map_err(|e| e.to_string());
    }
    let name = format!("lua_{}_{}_{}", provider.len(), provider, key);
    CredentialScope::new(dir, &name).map_err(|e| e.to_string())
}

pub(crate) fn attach(
    lua: &Lua,
    context: &Table,
    task: TaskHandle,
    generation: CancelToken,
    source: SourceIdentity,
    provider: String,
) -> mlua::Result<()> {
    attach_inner(lua, context, task, generation, source, provider, None)
}

pub(crate) fn attach_fenced(
    lua: &Lua,
    context: &Table,
    task: TaskHandle,
    generation: CancelToken,
    source: SourceIdentity,
    provider: String,
    fence: (CredentialLifecycle, CredentialGeneration),
) -> mlua::Result<()> {
    attach_inner(
        lua,
        context,
        task,
        generation,
        source,
        provider,
        Some(fence),
    )
}

fn attach_inner(
    lua: &Lua,
    context: &Table,
    task: TaskHandle,
    generation: CancelToken,
    source: SourceIdentity,
    provider: String,
    fence: Option<(CredentialLifecycle, CredentialGeneration)>,
) -> mlua::Result<()> {
    let store = lua.create_table()?;
    for operation in ["read", "write", "delete", "refresh"] {
        let task = task.clone();
        let generation = generation.clone();
        let source = source.clone();
        let provider = provider.clone();
        let fence = fence.clone();
        store.set(
            operation,
            lua.create_async_function(
                move |lua, (_store, key, argument): (Table, String, Value)| {
                    let task = task.clone();
                    let generation = generation.clone();
                    let source = source.clone();
                    let provider = provider.clone();
                    let fence = fence.clone();
                    async move {
                        if !authorized(&lua, &task, &generation) {
                            return Ok((
                                Value::Nil,
                                Some(error(&lua, "credential context expired")?),
                            ));
                        }
                        let scope = match scope(&lua, &source, &provider, &key) {
                            Ok(scope) => scope,
                            Err(message) => return Ok((Value::Nil, Some(error(&lua, &message)?))),
                        };
                        let result: Result<Value, String> = match operation {
                            "read" => {
                                scope
                                    .read()
                                    .map_err(|e| e.to_string())
                                    .and_then(|v| match v {
                                        Some(value) => json_to_lua_with_nulls(&lua, &value, true)
                                            .map_err(|e| e.to_string()),
                                        None => Ok(Value::Nil),
                                    })
                            }
                            "write" => match lua_to_json(&lua, &argument) {
                                Ok(value) => fenced_refresh(&scope, fence.as_ref(), |_| async {
                                    if !authorized(&lua, &task, &generation) {
                                        return Err(CredentialError::Refresh(
                                            "credential context expired".into(),
                                        ));
                                    }
                                    Ok(RefreshOutcome::Updated(value))
                                })
                                .await
                                .map(|_| Value::Boolean(true))
                                .map_err(|e| e.to_string()),
                                Err(e) => Err(e.to_string()),
                            },
                            "delete" => fenced_refresh(&scope, fence.as_ref(), |current| {
                                let lua = &lua;
                                let task = &task;
                                let generation = &generation;
                                async move {
                                    if !authorized(lua, task, generation) {
                                        return Err(CredentialError::Refresh(
                                            "credential context expired".into(),
                                        ));
                                    }
                                    Ok(if current.is_some() {
                                        RefreshOutcome::Deleted
                                    } else {
                                        RefreshOutcome::Unchanged
                                    })
                                }
                            })
                            .await
                            .map(|_| Value::Boolean(true))
                            .map_err(|e| e.to_string()),
                            "refresh" => match argument {
                                Value::Function(callback) => {
                                    refresh(
                                        &lua,
                                        &scope,
                                        callback,
                                        &task,
                                        &generation,
                                        fence.as_ref(),
                                    )
                                    .await
                                }
                                _ => {
                                    return Err(mlua::Error::runtime(
                                        "credential refresh requires a callback",
                                    ));
                                }
                            },
                            _ => unreachable!(),
                        };
                        match result {
                            Ok(value) => Ok((value, None)),
                            Err(message) => Ok((Value::Nil, Some(error(&lua, &message)?))),
                        }
                    }
                },
            )?,
        )?;
    }
    context.set("credentials", store)
}

async fn fenced_refresh<F, Fut>(
    scope: &CredentialScope,
    fence: Option<&(CredentialLifecycle, CredentialGeneration)>,
    callback: F,
) -> Result<Option<serde_json::Value>, CredentialError>
where
    F: FnOnce(Option<serde_json::Value>) -> Fut,
    Fut: std::future::Future<Output = Result<RefreshOutcome, CredentialError>>,
{
    match fence {
        Some((lifecycle, generation)) => lifecycle.refresh(generation, scope, callback).await,
        None => scope.refresh(callback).await,
    }
}

async fn refresh(
    lua: &Lua,
    scope: &CredentialScope,
    callback: Function,
    task: &TaskHandle,
    generation: &CancelToken,
    fence: Option<&(CredentialLifecycle, CredentialGeneration)>,
) -> Result<Value, String> {
    let value = fenced_refresh(scope, fence, |current| async {
        if !authorized(lua, task, generation) {
            return Err(CredentialError::Refresh(
                "credential context expired".into(),
            ));
        }
        let input = match current {
            Some(value) => json_to_lua_with_nulls(lua, &value, true)
                .map_err(|_| CredentialError::Refresh("invalid credential value".into()))?,
            None => Value::Nil,
        };
        let values = callback
            .call_async::<MultiValue>(input)
            .await
            .map_err(|_| CredentialError::Refresh("callback failed".into()))?;
        if !authorized(lua, task, generation) {
            return Err(CredentialError::Refresh(
                "credential context expired".into(),
            ));
        }
        let mut values = values.into_iter();
        let value = values.next().unwrap_or(Value::Nil);
        let err = values.next().unwrap_or(Value::Nil);
        if !matches!(err, Value::Nil) {
            return Err(CredentialError::Refresh("callback failed".into()));
        }
        if matches!(value, Value::Nil) {
            return Ok(RefreshOutcome::Unchanged);
        }
        lua_to_json(lua, &value)
            .map(RefreshOutcome::Updated)
            .map_err(|_| CredentialError::Refresh("invalid callback value".into()))
    })
    .await
    .map_err(|e| e.to_string())?;
    match value {
        Some(value) => json_to_lua_with_nulls(lua, &value, true).map_err(|e| e.to_string()),
        None => Ok(Value::Nil),
    }
}

#[cfg(test)]
mod tests {
    use maki_agent::cancel::CancelToken;
    use mlua::{Function, Lua};
    use tempfile::TempDir;

    use super::attach;
    use crate::api::env::StateDirOverride;
    use crate::loader::SourceIdentity;
    use crate::runtime::{TaskCell, TaskScope};

    #[test]
    fn openai_legacy_scope_is_limited_to_trusted_provider() {
        use super::scope;
        use maki_storage::StateDir;
        use maki_storage::auth::{OAuthTokens, load_tokens, save_tokens};

        let tmp = TempDir::new().unwrap();
        let lua = Lua::new();
        lua.set_app_data(StateDirOverride(tmp.path().to_owned()));
        let dir = StateDir::from_path(tmp.path().to_owned());
        save_tokens(
            &dir,
            "openai",
            &OAuthTokens {
                access: "old".into(),
                refresh: "refresh".into(),
                expires: 100,
                account_id: None,
            },
        )
        .unwrap();
        let trusted = SourceIdentity::CoreProviders;
        assert!(
            scope(
                &lua,
                &SourceIdentity::Embedded("providers"),
                "openai",
                "chatgpt"
            )
            .is_err()
        );
        let oauth = scope(&lua, &trusted, "openai", "chatgpt").unwrap();
        assert_eq!(oauth.read().unwrap().unwrap()["access"], "old");
        smol::block_on(
            oauth.write(&serde_json::json!({"access":"new", "refresh":"refresh", "expires":200})),
        )
        .unwrap();
        assert_eq!(load_tokens(&dir, "openai").unwrap().access, "new");
        assert!(
            scope(&lua, &trusted, "other", "chatgpt")
                .unwrap()
                .read()
                .unwrap()
                .is_none()
        );
        assert!(
            scope(
                &lua,
                &SourceIdentity::Embedded("synthetic"),
                "openai",
                "chatgpt"
            )
            .is_err()
        );
        assert!(scope(&lua, &trusted, "openai", "../openai").is_err());
    }

    #[test]
    fn anthropic_legacy_key_requires_core_provider() {
        use super::scope;
        use maki_storage::StateDir;
        use maki_storage::auth::{ProviderCredentials, save_provider_credentials};

        let tmp = TempDir::new().unwrap();
        let lua = Lua::new();
        lua.set_app_data(StateDirOverride(tmp.path().to_owned()));
        let dir = StateDir::from_path(tmp.path().to_owned());
        save_provider_credentials(
            &dir,
            "anthropic",
            &ProviderCredentials {
                api_key: "legacy-key".into(),
                host: None,
            },
        )
        .unwrap();
        let key = scope(&lua, &SourceIdentity::CoreProviders, "anthropic", "api_key").unwrap();
        assert_eq!(key.read().unwrap().unwrap()["api_key"], "legacy-key");
        assert!(scope(&lua, &SourceIdentity::Other, "anthropic", "api_key").is_err());
        assert!(
            scope(
                &lua,
                &SourceIdentity::Embedded("synthetic"),
                "anthropic",
                "api_key"
            )
            .is_err()
        );
    }

    #[test]
    fn google_legacy_key_requires_core_provider() {
        use super::scope;
        use maki_storage::StateDir;
        use maki_storage::auth::{ProviderCredentials, save_provider_credentials};

        let tmp = TempDir::new().unwrap();
        let lua = Lua::new();
        lua.set_app_data(StateDirOverride(tmp.path().to_owned()));
        let dir = StateDir::from_path(tmp.path().to_owned());
        save_provider_credentials(
            &dir,
            "google",
            &ProviderCredentials {
                api_key: "legacy-key".into(),
                host: None,
            },
        )
        .unwrap();
        let key = scope(&lua, &SourceIdentity::CoreProviders, "google", "api_key").unwrap();
        assert_eq!(key.read().unwrap().unwrap()["api_key"], "legacy-key");
        assert!(scope(&lua, &SourceIdentity::Other, "google", "api_key").is_err());
        assert!(
            scope(
                &lua,
                &SourceIdentity::Embedded("synthetic"),
                "google",
                "api_key"
            )
            .is_err()
        );
    }

    #[test]
    fn zai_legacy_key_scope_is_restricted() {
        use super::scope;
        use maki_storage::StateDir;
        use maki_storage::auth::{ProviderCredentials, save_provider_credentials};

        let tmp = TempDir::new().unwrap();
        let lua = Lua::new();
        lua.set_app_data(StateDirOverride(tmp.path().to_owned()));
        let dir = StateDir::from_path(tmp.path().to_owned());
        save_provider_credentials(
            &dir,
            "zai",
            &ProviderCredentials {
                api_key: "saved-key".into(),
                host: None,
            },
        )
        .unwrap();
        let trusted = SourceIdentity::Embedded("zai");
        assert_eq!(
            scope(&lua, &trusted, "zai", "api_key")
                .unwrap()
                .read()
                .unwrap()
                .unwrap()["api_key"],
            "saved-key"
        );
        assert!(scope(&lua, &trusted, "zai", "other").is_err());
        assert!(scope(&lua, &SourceIdentity::Other, "zai", "api_key").is_err());
    }

    #[test]
    fn requesty_legacy_key_scope_is_restricted() {
        use super::scope;
        use maki_storage::StateDir;
        use maki_storage::auth::{
            ProviderCredentials, load_provider_credentials, save_provider_credentials,
        };

        let tmp = TempDir::new().unwrap();
        let lua = Lua::new();
        lua.set_app_data(StateDirOverride(tmp.path().to_owned()));
        let dir = StateDir::from_path(tmp.path().to_owned());
        save_provider_credentials(
            &dir,
            "requesty",
            &ProviderCredentials {
                api_key: "saved-key".into(),
                host: None,
            },
        )
        .unwrap();
        let trusted = SourceIdentity::Embedded("requesty");
        let key = scope(&lua, &trusted, "requesty", "api_key").unwrap();
        assert_eq!(key.read().unwrap().unwrap()["api_key"], "saved-key");
        smol::block_on(key.write(&serde_json::json!({"api_key": "updated-key"}))).unwrap();
        assert_eq!(
            load_provider_credentials(&dir, "requesty").unwrap().api_key,
            "updated-key"
        );
        assert!(scope(&lua, &trusted, "requesty", "other").is_err());
        assert!(scope(&lua, &trusted, "synthetic", "api_key").is_err());
        assert!(
            scope(
                &lua,
                &SourceIdentity::Embedded("synthetic"),
                "requesty",
                "api_key"
            )
            .is_err()
        );
        assert!(scope(&lua, &SourceIdentity::Other, "requesty", "api_key").is_err());
        smol::block_on(key.delete()).unwrap();
        assert!(load_provider_credentials(&dir, "requesty").is_none());
    }

    #[test]
    fn tensorx_legacy_key_scope_is_restricted() {
        use super::scope;
        use maki_storage::StateDir;
        use maki_storage::auth::{
            ProviderCredentials, load_provider_credentials, save_provider_credentials,
        };

        let tmp = TempDir::new().unwrap();
        let lua = Lua::new();
        lua.set_app_data(StateDirOverride(tmp.path().to_owned()));
        let dir = StateDir::from_path(tmp.path().to_owned());
        save_provider_credentials(
            &dir,
            "tensorx",
            &ProviderCredentials {
                api_key: "saved-key".into(),
                host: None,
            },
        )
        .unwrap();
        let trusted = SourceIdentity::Embedded("tensorx");
        let key = scope(&lua, &trusted, "tensorx", "api_key").unwrap();
        assert_eq!(key.read().unwrap().unwrap()["api_key"], "saved-key");
        assert!(scope(&lua, &trusted, "tensorx", "other").is_err());
        assert!(scope(&lua, &SourceIdentity::Other, "tensorx", "api_key").is_err());
        assert!(
            scope(
                &lua,
                &SourceIdentity::Embedded("mistral"),
                "tensorx",
                "api_key"
            )
            .is_err()
        );
        smol::block_on(key.delete()).unwrap();
        assert!(load_provider_credentials(&dir, "tensorx").is_none());
    }

    #[test]
    fn mistral_legacy_key_scope_is_restricted() {
        use super::scope;
        use maki_storage::StateDir;
        use maki_storage::auth::{
            ProviderCredentials, load_provider_credentials, save_provider_credentials,
        };

        let tmp = TempDir::new().unwrap();
        let lua = Lua::new();
        lua.set_app_data(StateDirOverride(tmp.path().to_owned()));
        let dir = StateDir::from_path(tmp.path().to_owned());
        save_provider_credentials(
            &dir,
            "mistral",
            &ProviderCredentials {
                api_key: "saved-key".into(),
                host: None,
            },
        )
        .unwrap();
        let trusted = SourceIdentity::Embedded("mistral");
        let key = scope(&lua, &trusted, "mistral", "api_key").unwrap();
        assert_eq!(key.read().unwrap().unwrap()["api_key"], "saved-key");
        smol::block_on(key.write(&serde_json::json!({"api_key":"updated"}))).unwrap();
        assert_eq!(
            load_provider_credentials(&dir, "mistral").unwrap().api_key,
            "updated"
        );
        assert!(scope(&lua, &trusted, "mistral", "other").is_err());
        assert!(scope(&lua, &trusted, "zai", "api_key").is_err());
        assert!(scope(&lua, &SourceIdentity::Other, "mistral", "api_key").is_err());
        smol::block_on(key.delete()).unwrap();
        assert!(load_provider_credentials(&dir, "mistral").is_none());
    }

    #[test]
    fn synthetic_legacy_key_scope_is_restricted() {
        use super::scope;
        use maki_storage::StateDir;
        use maki_storage::auth::{ProviderCredentials, save_provider_credentials};

        let tmp = TempDir::new().unwrap();
        let lua = Lua::new();
        lua.set_app_data(StateDirOverride(tmp.path().to_owned()));
        let dir = StateDir::from_path(tmp.path().to_owned());
        save_provider_credentials(
            &dir,
            "synthetic",
            &ProviderCredentials {
                api_key: "saved-key".into(),
                host: None,
            },
        )
        .unwrap();
        let trusted = SourceIdentity::Embedded("synthetic");
        let key = scope(&lua, &trusted, "synthetic", "api_key").unwrap();
        assert_eq!(key.read().unwrap().unwrap()["api_key"], "saved-key");
        assert!(scope(&lua, &trusted, "synthetic", "other").is_err());
        assert!(scope(&lua, &trusted, "openai", "api_key").is_err());
        assert!(scope(&lua, &SourceIdentity::Other, "synthetic", "api_key").is_err());
    }

    #[test]
    fn deepseek_legacy_key_is_restricted_to_bundled_provider() {
        use super::scope;
        use maki_storage::StateDir;
        use maki_storage::auth::{ProviderCredentials, save_provider_credentials};

        let tmp = TempDir::new().unwrap();
        let lua = Lua::new();
        lua.set_app_data(StateDirOverride(tmp.path().to_owned()));
        let dir = StateDir::from_path(tmp.path().to_owned());
        save_provider_credentials(
            &dir,
            "deepseek",
            &ProviderCredentials {
                api_key: "saved-key".into(),
                host: None,
            },
        )
        .unwrap();
        let source = SourceIdentity::Embedded("deepseek");
        assert_eq!(
            scope(&lua, &source, "deepseek", "api_key")
                .unwrap()
                .read()
                .unwrap()
                .unwrap()["api_key"],
            "saved-key"
        );
        assert!(scope(&lua, &source, "deepseek", "other").is_err());
        assert!(scope(&lua, &source, "zai", "api_key").is_err());
        assert!(scope(&lua, &SourceIdentity::Other, "deepseek", "api_key").is_err());
    }

    #[test]
    fn regolo_legacy_key_is_restricted_to_bundled_provider() {
        use super::scope;
        use maki_storage::StateDir;
        use maki_storage::auth::{ProviderCredentials, save_provider_credentials};

        let tmp = TempDir::new().unwrap();
        let lua = Lua::new();
        lua.set_app_data(StateDirOverride(tmp.path().to_owned()));
        let dir = StateDir::from_path(tmp.path().to_owned());
        save_provider_credentials(
            &dir,
            "regolo",
            &ProviderCredentials {
                api_key: "saved-key".into(),
                host: None,
            },
        )
        .unwrap();
        let source = SourceIdentity::Embedded("regolo");
        assert_eq!(
            scope(&lua, &source, "regolo", "api_key")
                .unwrap()
                .read()
                .unwrap()
                .unwrap()["api_key"],
            "saved-key"
        );
        assert!(scope(&lua, &source, "regolo", "other").is_err());
        assert!(scope(&lua, &source, "zai", "api_key").is_err());
        assert!(scope(&lua, &SourceIdentity::Other, "regolo", "api_key").is_err());
    }

    #[test]
    fn trusted_provider_can_delete_credentials() {
        let tmp = TempDir::new().unwrap();
        let lua = Lua::new();
        lua.set_app_data(StateDirOverride(tmp.path().to_owned()));
        let task = TaskScope::new(&lua, TaskCell::new(CancelToken::none(), None, None));
        let context = lua.create_table().unwrap();
        attach(
            &lua,
            &context,
            task.handle().clone(),
            CancelToken::none(),
            SourceIdentity::CoreProviders,
            "example".into(),
        )
        .unwrap();
        let run: Function = lua
            .load(
                r#"return function(ctx)
            assert(ctx.credentials:write('token', { secret = 'fixture-secret' }))
            assert(ctx.credentials:read('token').secret == 'fixture-secret')
            assert(ctx.credentials:delete('token'))
            local value, err = ctx.credentials:read('token')
            assert(value == nil and err == nil)
        end"#,
            )
            .eval()
            .unwrap();
        smol::block_on(task.scope_future(run.call_async::<()>(context))).unwrap();
        assert!(!tmp.path().join("auth/lua_7_example_token.json").exists());
    }
}
