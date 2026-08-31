use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use maki_agent::session_coordinator::{SessionCoordinatorError, SessionOptionCatalog};
use maki_agent::session_options::{
    SessionOptionCategory, SessionOptionDefinition, SessionOptionError, SessionOptionOwner,
    SessionOptionState, SessionOptionValue,
};
use maki_lua_macro::{lua_fn, lua_table};
use mlua::{
    Function, Lua, MultiValue, Result as LuaResult, Table, UserData, UserDataMethods, Value,
};

use crate::api::session::resolve_coordinator;
use crate::api::util::command::UiAction;
use crate::api::util::pair::err_pair;

const SPEC_KEYS: &[&str] = &[
    "id",
    "name",
    "description",
    "category",
    "values",
    "initial_value",
    "persistent",
    "validate",
];
const VALIDATION_REENTRANT_ERR: &str = "session option validation is already in progress";

#[derive(Clone)]
pub(crate) struct PendingSessionOptions {
    plugin: Arc<str>,
    generation: u64,
    definitions: Arc<Mutex<Vec<SessionOptionDefinition>>>,
    validators: Arc<Mutex<BTreeMap<Arc<str>, Function>>>,
}

#[derive(Default)]
pub(crate) struct SessionOptionStore {
    generations: BTreeMap<Arc<str>, u64>,
}

#[derive(Default)]
pub(crate) struct SessionOptionValidators(BTreeMap<(Arc<str>, u64, Arc<str>), Function>);

pub(crate) struct SessionOptionValidation(Arc<AtomicBool>);

impl Default for SessionOptionValidation {
    fn default() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }
}

struct ValidationGuard(Arc<AtomicBool>);

impl Drop for ValidationGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

#[derive(Clone)]
struct SessionOptionHandle {
    plugin: Arc<str>,
    generation: u64,
    id: Arc<str>,
    ui_action_tx: Option<flume::Sender<UiAction>>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl PendingSessionOptions {
    pub(crate) fn new(plugin: Arc<str>, generation: u64) -> Self {
        Self {
            plugin,
            generation,
            definitions: Arc::default(),
            validators: Arc::default(),
        }
    }

    pub(crate) fn definitions(&self) -> Vec<SessionOptionDefinition> {
        lock(&self.definitions).clone()
    }

    pub(crate) fn validators(&self) -> BTreeMap<Arc<str>, Function> {
        lock(&self.validators).clone()
    }

    pub(crate) fn insert_validator(&self, id: Arc<str>, function: Function) {
        lock(&self.validators).insert(id, function);
    }
}

impl SessionOptionStore {
    pub(crate) fn next_generation(&self, plugin: &str) -> u64 {
        self.generations.get(plugin).copied().unwrap_or(0) + 1
    }

    pub(crate) fn commit(&mut self, plugin: Arc<str>, generation: u64) {
        self.generations.insert(plugin, generation);
    }

    pub(crate) fn remove(&mut self, plugin: &str) {
        self.generations.remove(plugin);
    }

    fn is_current(&self, plugin: &str, generation: u64) -> bool {
        self.generations.get(plugin).copied() == Some(generation)
    }
}

fn spec_error(id: &str, message: impl AsRef<str>) -> mlua::Error {
    mlua::Error::runtime(format!(
        "register_session_option: option {id:?}: {}",
        message.as_ref()
    ))
}

fn required_string(spec: &Table, id: &str, key: &str) -> LuaResult<Arc<str>> {
    spec.get::<Option<String>>(key)?
        .filter(|value| !value.is_empty())
        .map(Arc::from)
        .ok_or_else(|| spec_error(id, format!("{key} is required")))
}

fn parse_values(spec: &Table, id: &str) -> LuaResult<Arc<[SessionOptionValue]>> {
    let values = spec
        .get::<Table>("values")?
        .sequence_values::<Table>()
        .map(|entry| {
            let entry = entry?;
            Ok(SessionOptionValue {
                value: required_string(&entry, id, "value")?,
                name: required_string(&entry, id, "name")?,
            })
        })
        .collect::<LuaResult<Vec<_>>>()?;
    if values.is_empty() {
        return Err(spec_error(id, "values must not be empty"));
    }
    let unique = values
        .iter()
        .map(|value| value.value.as_ref())
        .collect::<BTreeSet<_>>();
    if unique.len() != values.len() {
        return Err(spec_error(id, "value ids must be unique"));
    }
    Ok(values.into())
}

fn parse_definition(
    pending: &PendingSessionOptions,
    spec: &Table,
) -> LuaResult<(SessionOptionDefinition, Option<Function>)> {
    let id = required_string(spec, "<unknown>", "id")?;
    for pair in spec.clone().pairs::<String, Value>() {
        let (key, _) = pair.map_err(|_| spec_error(&id, "spec keys must be strings"))?;
        if !SPEC_KEYS.contains(&key.as_str()) {
            return Err(spec_error(&id, format!("unknown spec key {key:?}")));
        }
    }
    let category = match spec.get::<String>("category")?.as_str() {
        "model" => SessionOptionCategory::Model,
        "mode" => SessionOptionCategory::Mode,
        category => return Err(spec_error(&id, format!("unknown category {category:?}"))),
    };
    let definition = SessionOptionDefinition {
        id: Arc::clone(&id),
        owner: SessionOptionOwner::Plugin {
            plugin: Arc::clone(&pending.plugin),
            generation: pending.generation,
        },
        name: required_string(spec, &id, "name")?,
        description: required_string(spec, &id, "description")?,
        category,
        values: parse_values(spec, &id)?,
        initial_value: required_string(spec, &id, "initial_value")?,
        persistent: spec.get::<Option<bool>>("persistent")?.unwrap_or(false),
    };
    definition
        .validate()
        .map_err(|error| spec_error(&id, error.to_string()))?;
    Ok((definition, spec.get("validate")?))
}

fn ensure_current(lua: &Lua, handle: &SessionOptionHandle) -> Result<(), String> {
    if lua
        .app_data_ref::<SessionOptionStore>()
        .is_some_and(|store| store.is_current(&handle.plugin, handle.generation))
    {
        Ok(())
    } else {
        Err(format!(
            "session option was replaced by a newer plugin generation: {}",
            handle.id
        ))
    }
}

fn validation_guard(lua: &Lua) -> Result<ValidationGuard, SessionOptionError> {
    let state = if let Some(state) = lua
        .app_data_ref::<SessionOptionValidation>()
        .map(|state| Arc::clone(&state.0))
    {
        state
    } else {
        lua.set_app_data(SessionOptionValidation::default());
        Arc::clone(
            &lua.app_data_ref::<SessionOptionValidation>()
                .expect("session validation state was just installed")
                .0,
        )
    };
    state
        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .map(|_| ValidationGuard(state))
        .map_err(|_| SessionOptionError::CallbackFailed(Arc::from(VALIDATION_REENTRANT_ERR)))
}

fn validator_for(lua: &Lua, plugin: &str, generation: u64, id: &str) -> Option<Function> {
    lua.app_data_ref::<SessionOptionValidators>()
        .and_then(|validators| {
            validators
                .0
                .get(&(Arc::from(plugin), generation, Arc::from(id)))
                .cloned()
        })
}

pub(crate) fn ensure_validation_not_in_progress(lua: &Lua) -> Result<(), SessionOptionError> {
    let in_progress = lua
        .app_data_ref::<SessionOptionValidation>()
        .is_some_and(|state| state.0.load(Ordering::Acquire));
    if in_progress {
        Err(SessionOptionError::CallbackFailed(Arc::from(
            VALIDATION_REENTRANT_ERR,
        )))
    } else {
        Ok(())
    }
}

pub(crate) fn validate_option_value(
    lua: &Lua,
    option: &SessionOptionState,
    value: &str,
) -> Result<(), SessionOptionError> {
    match &option.definition.owner {
        SessionOptionOwner::Builtin => Ok(()),
        SessionOptionOwner::Plugin { plugin, generation } => {
            validate_value(lua, plugin, *generation, &option.definition.id, value)
        }
    }
}

fn validate_function(lua: &Lua, function: Function, value: &str) -> Result<(), SessionOptionError> {
    let _guard = validation_guard(lua)?;
    let mut values: MultiValue = function
        .call((value,))
        .map_err(|error| SessionOptionError::CallbackFailed(Arc::from(error.to_string())))?;
    match values.pop_front() {
        Some(Value::Boolean(true)) => Ok(()),
        Some(Value::Boolean(false)) | Some(Value::Nil) | None => {
            let message = values
                .pop_front()
                .and_then(|value| match value {
                    Value::String(message) => {
                        message.to_str().ok().map(|message| message.to_owned())
                    }
                    _ => None,
                })
                .unwrap_or_else(|| "validator rejected value".to_owned());
            Err(SessionOptionError::CallbackFailed(Arc::from(message)))
        }
        Some(Value::String(message)) => Err(SessionOptionError::CallbackFailed(Arc::from(
            message
                .to_str()
                .map_or_else(|_| "validator rejected value".to_owned(), |s| s.to_owned()),
        ))),
        _ => Err(SessionOptionError::CallbackFailed(Arc::from(
            "validator rejected value",
        ))),
    }
}

fn validate_value(
    lua: &Lua,
    plugin: &str,
    generation: u64,
    id: &str,
    value: &str,
) -> Result<(), SessionOptionError> {
    let Some(function) = validator_for(lua, plugin, generation, id) else {
        return Ok(());
    };
    validate_function(lua, function, value)
}

impl UserData for SessionOptionHandle {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_async_method("get", |lua, this, opts: Option<Table>| async move {
            if let Err(error) = ensure_current(&lua, &this) {
                return Ok(err_pair(error));
            }
            let coordinator =
                match resolve_coordinator(&lua, this.ui_action_tx.as_ref(), opts.as_ref()).await {
                    Ok(coordinator) => coordinator,
                    Err(error) => return Ok(err_pair(error)),
                };
            let value = coordinator
                .read()
                .options()
                .options
                .iter()
                .find(|option| option.definition.id == this.id)
                .map(|option| option.current_value.to_string());
            Ok(match value {
                Some(value) => (Some(value), None),
                None => err_pair(format!("unknown session option: {}", this.id)),
            })
        });
        methods.add_async_method(
            "set",
            |lua, this, (value, opts): (String, Option<Table>)| async move {
                if let Err(error) = ensure_current(&lua, &this) {
                    return Ok(err_pair(error));
                }
                if let Err(error) = ensure_validation_not_in_progress(&lua) {
                    return Ok(err_pair(error));
                }
                let coordinator = match resolve_coordinator(
                    &lua,
                    this.ui_action_tx.as_ref(),
                    opts.as_ref(),
                )
                .await
                {
                    Ok(coordinator) => coordinator,
                    Err(error) => return Ok(err_pair(error)),
                };
                let snapshot = coordinator.read().options();
                if let Err(error) =
                    validate_value(&lua, &this.plugin, this.generation, &this.id, &value)
                {
                    return Ok(err_pair(error));
                }
                match coordinator
                    .set_option_if_version(Arc::clone(&this.id), value, Some(snapshot.version))
                    .await
                {
                    Ok(_) => Ok((Some(true), None)),
                    Err(error) => Ok(err_pair(error)),
                }
            },
        );
    }
}

/// Registers one selectable session option owned by the loading plugin.
///
/// The `id` must use the plugin namespace, such as `bash.auto_mode`. Core owns
/// unqualified ids. The definition supplies `name`, `description`, `category`,
/// an ordered non-empty `values` list, and `initial_value`. Set `persistent` to
/// retain each session's value across reloads.
///
/// An optional synchronous `validate(value)` callback returns `true` to accept
/// the candidate. It returns `false, message` or raises an error to reject it.
/// Rejection leaves every session and the active plugin generation unchanged.
/// Validation cannot yield or mutate session options.
///
/// Compatible plugin replacement retains current session values. The returned
/// handle becomes stale after replacement or unload. Runtime failures from
/// `get(opts?)` and `set(value, opts?)` use the normal `(value, err)` convention.
/// Both methods accept an optional `{ session = id }` target.
///
/// @param spec table Session option definition and optional validation callback.
/// @return userdata Generation-bound handle with `get(opts?)` and `set(value, opts?)`.
#[lua_fn]
fn register_session_option(
    lua: &Lua,
    #[ctx] pending: PendingSessionOptions,
    #[ctx] ui_action_tx: Option<flume::Sender<UiAction>>,
    spec: Table,
) -> LuaResult<mlua::AnyUserData> {
    let (definition, validator) = parse_definition(&pending, &spec)?;
    let mut definitions = lock(&pending.definitions);
    if definitions
        .iter()
        .any(|current| current.id == definition.id)
    {
        return Err(spec_error(
            &definition.id,
            "option id registered more than once",
        ));
    }
    let handle = SessionOptionHandle {
        plugin: Arc::clone(&pending.plugin),
        generation: pending.generation,
        id: Arc::clone(&definition.id),
        ui_action_tx,
    };
    definitions.push(definition);
    drop(definitions);
    if let Some(validator) = validator {
        pending.insert_validator(Arc::clone(&handle.id), validator);
    }
    lua.create_userdata(handle)
}

lua_table! {
    extend "maki.api" => pub(crate) fn add_session_option_fn(
        pending: PendingSessionOptions,
        ui_action_tx: Option<flume::Sender<UiAction>>
    ), DOCS [register_session_option(pending, ui_action_tx)]
}

pub(crate) async fn commit_pending(
    lua: &Lua,
    catalog: &SessionOptionCatalog,
    pending: &PendingSessionOptions,
) -> Result<(), String> {
    let validators = pending.validators();
    catalog
        .replace_plugin_options_with_validator(
            Arc::clone(&pending.plugin),
            pending.definitions(),
            |_, id, value| {
                let lua = lua.clone();
                let plugin = Arc::clone(&pending.plugin);
                let generation = pending.generation;
                let validator = validators.get(&id).cloned();
                async move {
                    let Some(validator) = validator else {
                        return Ok(());
                    };
                    validate_function(&lua, validator, value.as_ref()).map_err(|error| {
                        SessionCoordinatorError::Option(SessionOptionError::CallbackFailed(
                            Arc::from(format!(
                                "{plugin} generation {generation}, option {id}: {error}"
                            )),
                        ))
                    })
                }
            },
        )
        .await
        .map_err(|error| error.to_string())?;
    if let Some(mut active) = lua.app_data_mut::<SessionOptionValidators>() {
        active
            .0
            .retain(|(plugin, _, _), _| plugin != &pending.plugin);
        for (id, validator) in validators {
            active.0.insert(
                (Arc::clone(&pending.plugin), pending.generation, id),
                validator,
            );
        }
    }
    Ok(())
}

pub(crate) async fn unload(
    lua: &Lua,
    catalog: &SessionOptionCatalog,
    plugin: &str,
) -> Result<(), String> {
    catalog
        .replace_plugin_options(plugin, Vec::new())
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())?;
    if let Some(mut validators) = lua.app_data_mut::<SessionOptionValidators>() {
        validators
            .0
            .retain(|(owner, _, _), _| owner.as_ref() != plugin);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use maki_agent::SessionMailbox;
    use maki_agent::session_coordinator::{
        DirectoryAdoptionFuture, ModelAdoptionFuture, SessionCheckpoint, SessionCoordinatorHandle,
        SessionCoordinatorParams, builtin_option_definitions,
    };
    use maki_providers::Model;
    use maki_storage::checkpoint::{
        CheckpointAck, CheckpointFuture, CheckpointRequest, CheckpointWriter,
    };
    use maki_storage::id::MakiId;

    use super::*;

    fn coordinator(id: MakiId, catalog: &SessionOptionCatalog) -> SessionCoordinatorHandle {
        let checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>> =
            Arc::new(|request: CheckpointRequest<SessionCheckpoint>| {
                Box::pin(async move {
                    Ok(CheckpointAck {
                        session_id: request.session_id,
                        version: request.version,
                    })
                }) as CheckpointFuture
            });
        SessionCoordinatorHandle::register(SessionCoordinatorParams {
            session_id: id,
            catalog: catalog.clone(),
            definitions: builtin_option_definitions(
                "test/model",
                [Arc::from("test/model")],
                false,
                false,
                false,
            ),
            persisted_options: Default::default(),
            history: Vec::new(),
            model: Arc::from("test/model"),
            cwd: PathBuf::from("/project"),
            model_policy: Arc::default(),
            model_adopter: Arc::new(|_: Model| Box::pin(async { Ok(()) }) as ModelAdoptionFuture),
            directory_adopter: Arc::new(|path: PathBuf| {
                Box::pin(async move { Ok(path) }) as DirectoryAdoptionFuture
            }),
            checkpoint,
            mailbox: SessionMailbox::new(id),
        })
        .unwrap()
    }

    fn option_spec(lua: &Lua) -> Table {
        lua.load(
            r#"return {
                id = "test.choice",
                name = "Choice",
                description = "Test choice",
                category = "mode",
                values = {
                    { value = "a", name = "A" },
                    { value = "b", name = "B" },
                },
                initial_value = "a",
                persistent = true,
            }"#,
        )
        .eval()
        .unwrap()
    }

    #[test]
    fn generation_bound_handle_targets_explicit_sessions_and_becomes_stale() {
        smol::block_on(async {
            let first_id = MakiId::generate();
            let second_id = MakiId::generate();
            let catalog = SessionOptionCatalog::default();
            let first = coordinator(first_id, &catalog);
            let second = coordinator(second_id, &catalog);
            let lua = Lua::new();
            lua.set_app_data(SessionOptionStore::default());
            lua.set_app_data(SessionOptionValidators::default());
            lua.set_app_data(SessionOptionValidation::default());
            let pending = PendingSessionOptions::new(Arc::from("test"), 1);
            let api = lua.create_table().unwrap();
            add_session_option_fn(&api, &lua, pending.clone(), None).unwrap();
            lua.globals().set("api", api).unwrap();
            lua.globals().set("first", first_id.to_string()).unwrap();
            lua.globals().set("second", second_id.to_string()).unwrap();
            lua.globals().set("spec", option_spec(&lua)).unwrap();
            lua.load("handle = api.register_session_option(spec)")
                .exec()
                .unwrap();
            commit_pending(&lua, &catalog, &pending).await.unwrap();
            lua.app_data_mut::<SessionOptionStore>()
                .unwrap()
                .commit(Arc::from("test"), 1);

            let (ok, error): (bool, Option<String>) = lua
                .load("return handle:set('b', { session = first })")
                .eval_async()
                .await
                .unwrap();
            assert!(ok);
            assert_eq!(error, None);
            let (first_value, error): (String, Option<String>) = lua
                .load("return handle:get({ session = first })")
                .eval_async()
                .await
                .unwrap();
            assert_eq!(first_value, "b");
            assert_eq!(error, None);
            let (second_value, error): (String, Option<String>) = lua
                .load("return handle:get({ session = second })")
                .eval_async()
                .await
                .unwrap();
            assert_eq!(second_value, "a");
            assert_eq!(error, None);

            lua.app_data_mut::<SessionOptionStore>()
                .unwrap()
                .commit(Arc::from("test"), 2);
            let (value, error): (Value, Option<String>) = lua
                .load("return handle:get({ session = first })")
                .eval_async()
                .await
                .unwrap();
            assert!(value.is_nil());
            assert!(error.is_some_and(|error| error.contains("newer plugin generation")));
            first.close().await.unwrap();
            second.close().await.unwrap();
        });
    }

    #[test]
    fn validator_failure_preserves_sessions_and_previous_generation() {
        smol::block_on(async {
            let first_id = MakiId::generate();
            let second_id = MakiId::generate();
            let catalog = SessionOptionCatalog::default();
            let first = coordinator(first_id, &catalog);
            let second = coordinator(second_id, &catalog);
            let lua = Lua::new();
            lua.set_app_data(SessionOptionStore::default());
            lua.set_app_data(SessionOptionValidators::default());
            lua.set_app_data(SessionOptionValidation::default());
            lua.globals().set("first", first_id.to_string()).unwrap();
            lua.globals().set("second", second_id.to_string()).unwrap();

            let pending = PendingSessionOptions::new(Arc::from("test"), 1);
            let api = lua.create_table().unwrap();
            add_session_option_fn(&api, &lua, pending.clone(), None).unwrap();
            lua.globals().set("api", api).unwrap();
            let spec = option_spec(&lua);
            lua.globals().set("reject", false).unwrap();
            spec.set(
                "validate",
                lua.load(
                    r#"function(value)
                        if reject then return false, "value rejected" end
                        return true
                    end"#,
                )
                .eval::<Function>()
                .unwrap(),
            )
            .unwrap();
            lua.globals().set("spec", spec).unwrap();
            lua.load("handle = api.register_session_option(spec)")
                .exec()
                .unwrap();
            commit_pending(&lua, &catalog, &pending).await.unwrap();
            lua.app_data_mut::<SessionOptionStore>()
                .unwrap()
                .commit(Arc::from("test"), 1);

            lua.globals().set("reject", true).unwrap();
            let (value, error): (Value, Option<String>) = lua
                .load("return handle:set('b', { session = first })")
                .eval_async()
                .await
                .unwrap();
            assert!(value.is_nil());
            assert!(
                error
                    .as_deref()
                    .is_some_and(|error| error.contains("value rejected")),
                "unexpected validator error: {error:?}"
            );
            for session in [&first, &second] {
                assert_eq!(
                    session
                        .read()
                        .options()
                        .options
                        .iter()
                        .find(|option| option.definition.id.as_ref() == "test.choice")
                        .unwrap()
                        .current_value
                        .as_ref(),
                    "a"
                );
            }

            let next = PendingSessionOptions::new(Arc::from("test"), 2);
            let next_api = lua.create_table().unwrap();
            add_session_option_fn(&next_api, &lua, next.clone(), None).unwrap();
            lua.globals().set("next_api", next_api).unwrap();
            let next_spec = option_spec(&lua);
            next_spec.set("initial_value", "b").unwrap();
            next_spec
                .set(
                    "values",
                    lua.load(r#"{{ value = "b", name = "B" }}"#)
                        .eval::<Table>()
                        .unwrap(),
                )
                .unwrap();
            next_spec
                .set(
                    "validate",
                    lua.load(r#"function() return false, "replacement rejected" end"#)
                        .eval::<Function>()
                        .unwrap(),
                )
                .unwrap();
            lua.globals().set("next_spec", next_spec).unwrap();
            lua.load("next_handle = next_api.register_session_option(next_spec)")
                .exec()
                .unwrap();

            let error = commit_pending(&lua, &catalog, &next).await.unwrap_err();
            assert!(error.contains("replacement rejected"));
            assert!(
                lua.app_data_ref::<SessionOptionStore>()
                    .unwrap()
                    .is_current("test", 1)
            );
            assert!(
                !lua.app_data_ref::<SessionOptionStore>()
                    .unwrap()
                    .is_current("test", 2)
            );
            lua.globals().set("reject", false).unwrap();
            let (ok, error): (bool, Option<String>) = lua
                .load("return handle:set('b', { session = first })")
                .eval_async()
                .await
                .unwrap();
            assert!(ok);
            assert_eq!(error, None);
            assert_eq!(
                first
                    .read()
                    .options()
                    .options
                    .iter()
                    .find(|option| option.definition.id.as_ref() == "test.choice")
                    .unwrap()
                    .current_value
                    .as_ref(),
                "b"
            );
            assert_eq!(
                second
                    .read()
                    .options()
                    .options
                    .iter()
                    .find(|option| option.definition.id.as_ref() == "test.choice")
                    .unwrap()
                    .current_value
                    .as_ref(),
                "a"
            );
            first.close().await.unwrap();
            second.close().await.unwrap();
        });
    }

    #[test]
    fn invalid_registration_is_a_programmer_error() {
        let lua = Lua::new();
        let pending = PendingSessionOptions::new(Arc::from("test"), 1);
        let api = lua.create_table().unwrap();
        add_session_option_fn(&api, &lua, pending, None).unwrap();
        lua.globals().set("api", api).unwrap();

        let error = lua
            .load(
                r#"return api.register_session_option({
                    id = "other.choice",
                    name = "Choice",
                    description = "Bad owner",
                    category = "mode",
                    values = {{ value = "a", name = "A" }},
                    initial_value = "a",
                })"#,
            )
            .eval::<Value>()
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("option ID does not match its owner")
        );
    }
}
