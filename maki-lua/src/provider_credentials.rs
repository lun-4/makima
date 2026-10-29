use maki_agent::cancel::CancelToken;
use maki_storage::auth::{CredentialError, CredentialScope, RefreshOutcome};
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
    let trusted = matches!(source, SourceIdentity::Embedded("providers"))
        || matches!(source, SourceIdentity::Embedded("synthetic")) && provider == "synthetic";
    if !trusted
        || key.is_empty()
        || !key
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
    {
        return Err("credential access denied".into());
    }
    let name = format!("lua_{}_{}_{}", provider.len(), provider, key);
    CredentialScope::new(crate::api::env::host_state_dir(lua)?, &name).map_err(|e| e.to_string())
}

pub(crate) fn attach(
    lua: &Lua,
    context: &Table,
    task: TaskHandle,
    generation: CancelToken,
    source: SourceIdentity,
    provider: String,
) -> mlua::Result<()> {
    let store = lua.create_table()?;
    for operation in ["read", "write", "delete", "refresh"] {
        let task = task.clone();
        let generation = generation.clone();
        let source = source.clone();
        let provider = provider.clone();
        store.set(
            operation,
            lua.create_async_function(
                move |lua, (_store, key, argument): (Table, String, Value)| {
                    let task = task.clone();
                    let generation = generation.clone();
                    let source = source.clone();
                    let provider = provider.clone();
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
                                Ok(value) => scope
                                    .refresh(|_| async {
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
                            "delete" => scope
                                .refresh(|current| {
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
                                    refresh(&lua, &scope, callback, &task, &generation).await
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

async fn refresh(
    lua: &Lua,
    scope: &CredentialScope,
    callback: Function,
    task: &TaskHandle,
    generation: &CancelToken,
) -> Result<Value, String> {
    let value = scope
        .refresh(|current| async {
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
            SourceIdentity::Embedded("providers"),
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
