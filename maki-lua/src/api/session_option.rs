use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use maki_agent::session_coordinator::{
    SessionCoordinatorError, SessionOptionCatalog, settable_builtin_definitions,
};
use maki_agent::session_options::{
    SessionOptionCategory, SessionOptionDefinition, SessionOptionError, SessionOptionOwner,
    SessionOptionState, SessionOptionValue,
};
use maki_lua_macro::{lua_fn, lua_table};
use mlua::{Function, Lua, MultiValue, Result as LuaResult, Table, Value};

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

    pub(crate) fn invalidate(&mut self, plugin: &str) {
        if let Some(generation) = self.generations.get_mut(plugin) {
            *generation += 1;
        }
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

fn required_category(spec: &Table, id: &str) -> LuaResult<SessionOptionCategory> {
    match required_string(spec, id, "category")?.as_ref() {
        "model" => Ok(SessionOptionCategory::Model),
        "mode" => Ok(SessionOptionCategory::Mode),
        category => Err(spec_error(id, format!("unknown category {category:?}"))),
    }
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
    let category = required_category(spec, &id)?;
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
        free_value: None,
        initial_value: required_string(spec, &id, "initial_value")?,
        persistent: spec.get::<Option<bool>>("persistent")?.unwrap_or(false),
    };
    definition
        .validate()
        .map_err(|error| spec_error(&id, error.to_string()))?;
    Ok((definition, spec.get("validate")?))
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

/// Registers one selectable session option owned by the loading plugin.
///
/// The `id` must use the plugin namespace, such as `bash.auto_mode`. Global
/// `init.lua` uses `maki_init.global.*`, and project `init.lua` uses
/// `maki_init.project.*`. Core owns
/// unqualified ids. The definition supplies `name`, `description`, `category`,
/// an ordered non-empty `values` list, and `initial_value`. Set `persistent` to
/// retain each session's value across reloads.
///
/// An optional synchronous `validate(value)` callback returns `true` to accept
/// the candidate. It returns `false, message` or raises an error to reject it.
/// Rejection leaves every session and the active plugin generation unchanged.
/// Validation cannot yield or mutate session options.
///
/// Compatible plugin replacement retains current session values. Read a
/// value through `Agent:config(ctx).options[id]` and change it with
/// `Agent:set_option(ctx, id, value)`.
///
/// @param spec table Session option definition and optional validation callback.
/// @return
#[lua_fn]
fn register_session_option(
    lua: &Lua,
    #[ctx] pending: PendingSessionOptions,
    spec: Table,
) -> LuaResult<()> {
    crate::runtime::require_plugin_load(lua, &pending.plugin, "register_session_option")?;
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
    let id = Arc::clone(&definition.id);
    definitions.push(definition);
    drop(definitions);
    if let Some(validator) = validator {
        pending.insert_validator(id, validator);
    }
    Ok(())
}

/// Lists the definitions of every option `Agent:set_option` accepts: the
/// built-in `workflow` and `yolo` toggles and every plugin's options. A
/// session's current values are in `Agent:config(ctx).options`.
///
/// @return table Array of `{id, name, description, category, values, owner, persistent}`, where `values` is an array of `{value, name}` and `owner` is "builtin" or the plugin name.
/// @example
/// for _, option in ipairs(maki.api.get_session_options()) do
///   print(option.id, option.name)
/// end
#[lua_fn]
fn get_session_options(lua: &Lua) -> LuaResult<Table> {
    let plugins = lua
        .app_data_ref::<SessionOptionCatalog>()
        .map(|catalog| catalog.definitions())
        .unwrap_or_default();
    let definitions = settable_builtin_definitions().into_iter().chain(plugins);
    let list = lua.create_table()?;
    for definition in definitions {
        list.push(definition_table(lua, &definition)?)?;
    }
    Ok(list)
}

fn definition_table(lua: &Lua, definition: &SessionOptionDefinition) -> LuaResult<Table> {
    let option = lua.create_table()?;
    option.set("id", definition.id.as_ref())?;
    option.set("name", definition.name.as_ref())?;
    option.set("description", definition.description.as_ref())?;
    option.set(
        "category",
        match definition.category {
            SessionOptionCategory::Model => "model",
            SessionOptionCategory::Mode => "mode",
        },
    )?;
    option.set("persistent", definition.persistent)?;
    option.set(
        "owner",
        match &definition.owner {
            SessionOptionOwner::Builtin => "builtin",
            SessionOptionOwner::Plugin { plugin, .. } => plugin.as_ref(),
        },
    )?;
    let values = lua.create_table()?;
    for value in definition.values.iter() {
        let item = lua.create_table()?;
        item.set("value", value.value.as_ref())?;
        item.set("name", value.name.as_ref())?;
        values.push(item)?;
    }
    option.set("values", values)?;
    Ok(option)
}

lua_table! {
    extend "maki.api" => pub(crate) fn add_session_option_fn(
        pending: PendingSessionOptions
    ), DOCS [register_session_option(pending), get_session_options]
}

pub(crate) async fn commit_catalog(
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
    Ok(())
}

pub(crate) fn activate_pending(lua: &Lua, pending: &PendingSessionOptions) {
    if let Some(mut active) = lua.app_data_mut::<SessionOptionValidators>() {
        active
            .0
            .retain(|(plugin, _, _), _| plugin != &pending.plugin);
        for (id, validator) in pending.validators() {
            active.0.insert(
                (Arc::clone(&pending.plugin), pending.generation, id),
                validator,
            );
        }
    }
}

#[cfg(test)]
pub(crate) async fn commit_pending(
    lua: &Lua,
    catalog: &SessionOptionCatalog,
    pending: &PendingSessionOptions,
) -> Result<(), String> {
    commit_catalog(lua, catalog, pending).await?;
    activate_pending(lua, pending);
    Ok(())
}

pub(crate) async fn restore(
    catalog: &SessionOptionCatalog,
    plugin: &str,
    definitions: Vec<SessionOptionDefinition>,
) -> Result<(), String> {
    catalog
        .replace_plugin_options(plugin, definitions)
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
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

    use maki_agent::SessionMailbox;
    use maki_agent::session_coordinator::{
        SessionCheckpoint, SessionCoordinatorHandle, SessionCoordinatorParams,
        builtin_option_definitions,
    };
    use maki_storage::checkpoint::{CheckpointWriter, accept_all};
    use maki_storage::id::MakiId;
    use test_case::test_case;

    use super::*;

    fn coordinator(id: MakiId, catalog: &SessionOptionCatalog) -> SessionCoordinatorHandle {
        let checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>> = accept_all();
        SessionCoordinatorHandle::register(SessionCoordinatorParams {
            session_id: id,
            catalog: catalog.clone(),
            definitions: builtin_option_definitions(
                "test/model",
                [Arc::from("test/model")],
                false,
                false,
                false,
                maki_agent::ThinkingConfig::Off,
            ),
            persisted_options: Default::default(),
            model_policy: Arc::default(),
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
    fn generations_remain_monotonic_across_unload_and_reload() {
        let mut store = SessionOptionStore::default();
        let plugin = Arc::from("test");

        let first = store.next_generation(&plugin);
        store.commit(Arc::clone(&plugin), first);
        store.invalidate(&plugin);
        let reloaded = store.next_generation(&plugin);
        store.commit(plugin, reloaded);

        assert!(reloaded > first);
    }

    #[test]
    fn listing_has_the_settable_builtins_then_plugin_options() {
        smol::block_on(async {
            let catalog = SessionOptionCatalog::default();
            let lua = Lua::new();
            lua.set_app_data(crate::runtime::LoadingPlugin(Arc::from("test"), 1));
            lua.set_app_data(SessionOptionValidators::default());
            lua.set_app_data(catalog.clone());
            let pending = PendingSessionOptions::new(Arc::from("test"), 1);
            let api = lua.create_table().unwrap();
            add_session_option_fn(&api, &lua, pending.clone()).unwrap();
            lua.globals().set("api", api).unwrap();
            lua.globals().set("spec", option_spec(&lua)).unwrap();
            lua.load("api.register_session_option(spec)")
                .exec()
                .unwrap();
            commit_pending(&lua, &catalog, &pending).await.unwrap();

            let ids: Vec<String> = lua
                .load(
                    "local ids = {} for _, o in ipairs(api.get_session_options()) do ids[#ids + 1] = o.id .. '@' .. o.owner end return ids",
                )
                .eval::<Vec<String>>()
                .unwrap();
            assert_eq!(
                ids,
                ["yolo@builtin", "workflow@builtin", "test.choice@test"]
            );
        });
    }

    /// The validated set `Agent:set_option` runs once it has the session.
    async fn set_choice(
        lua: &Lua,
        session: &SessionCoordinatorHandle,
        value: &str,
    ) -> Result<(), String> {
        let snapshot = session.read().options();
        let option = snapshot
            .options
            .iter()
            .find(|option| option.definition.id.as_ref() == "test.choice")
            .unwrap();
        validate_option_value(lua, option, value).map_err(|error| error.to_string())?;
        session
            .set_option_if_version(
                Arc::from("test.choice"),
                Arc::from(value),
                Some(snapshot.version),
            )
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
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
            lua.set_app_data(crate::runtime::LoadingPlugin(Arc::from("test"), 1));
            lua.set_app_data(SessionOptionStore::default());
            lua.set_app_data(SessionOptionValidators::default());
            lua.set_app_data(SessionOptionValidation::default());
            lua.globals().set("first", first_id.to_string()).unwrap();
            lua.globals().set("second", second_id.to_string()).unwrap();

            let pending = PendingSessionOptions::new(Arc::from("test"), 1);
            let api = lua.create_table().unwrap();
            add_session_option_fn(&api, &lua, pending.clone()).unwrap();
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
            lua.load("api.register_session_option(spec)")
                .exec()
                .unwrap();
            commit_pending(&lua, &catalog, &pending).await.unwrap();
            lua.app_data_mut::<SessionOptionStore>()
                .unwrap()
                .commit(Arc::from("test"), 1);

            lua.globals().set("reject", true).unwrap();
            let error = set_choice(&lua, &first, "b").await.unwrap_err();
            assert!(
                error.contains("value rejected"),
                "unexpected validator error: {error}"
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
            add_session_option_fn(&next_api, &lua, next.clone()).unwrap();
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
            lua.load("next_api.register_session_option(next_spec)")
                .exec()
                .unwrap();

            let error = commit_pending(&lua, &catalog, &next).await.unwrap_err();
            assert!(error.contains("replacement rejected"));
            lua.globals().set("reject", false).unwrap();
            set_choice(&lua, &first, "b").await.unwrap();
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

    #[test_case("return {}", "validator rejected value"; "table")]
    #[test_case("return 1", "validator rejected value"; "number")]
    #[test_case("return 'invalid result', 'ignored'", "invalid result"; "string_message")]
    fn validator_rejects_invalid_return_values(body: &str, expected: &str) {
        let lua = Lua::new();
        let function = lua
            .load(format!("function() {body} end"))
            .eval::<Function>()
            .unwrap();

        let error = validate_function(&lua, function, "a").unwrap_err();
        assert!(error.to_string().contains(expected));
    }

    #[test]
    fn validator_reentrancy_is_rejected_and_guard_recovers() {
        let lua = Lua::new();
        let nested = lua
            .load("function() return true end")
            .eval::<Function>()
            .unwrap();
        let call_nested = lua
            .create_function(move |lua, ()| {
                validate_function(lua, nested.clone(), "a")
                    .map_err(|error| mlua::Error::runtime(error.to_string()))
            })
            .unwrap();
        lua.globals().set("call_nested", call_nested).unwrap();
        let outer = lua
            .load("function() call_nested(); return true end")
            .eval::<Function>()
            .unwrap();

        let error = validate_function(&lua, outer, "a").unwrap_err();
        assert!(error.to_string().contains(VALIDATION_REENTRANT_ERR));
        let valid = lua
            .load("function() return true end")
            .eval::<Function>()
            .unwrap();
        assert!(validate_function(&lua, valid, "a").is_ok());
    }

    const MISSING_CATEGORY_ERR: &str =
        "register_session_option: option \"test.choice\": category is required";

    #[test_case("spec.category = nil"; "missing")]
    #[test_case("spec.category = ''"; "empty")]
    fn missing_category_is_a_programmer_error(mutation: &str) {
        let lua = Lua::new();
        lua.set_app_data(crate::runtime::LoadingPlugin(Arc::from("test"), 1));
        let pending = PendingSessionOptions::new(Arc::from("test"), 1);
        let api = lua.create_table().unwrap();
        add_session_option_fn(&api, &lua, pending).unwrap();
        lua.globals().set("api", api).unwrap();
        lua.globals().set("spec", option_spec(&lua)).unwrap();

        let error = lua
            .load(format!(
                "{mutation}; return api.register_session_option(spec)"
            ))
            .eval::<Value>()
            .unwrap_err();
        assert!(error.to_string().contains(MISSING_CATEGORY_ERR));
    }

    #[test]
    fn invalid_registration_is_a_programmer_error() {
        let lua = Lua::new();
        lua.set_app_data(crate::runtime::LoadingPlugin(Arc::from("test"), 1));
        let pending = PendingSessionOptions::new(Arc::from("test"), 1);
        let api = lua.create_table().unwrap();
        add_session_option_fn(&api, &lua, pending).unwrap();
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
