use std::sync::Arc;

use maki_agent::tools::ToolRegistry;
use maki_lua::PluginHost;

#[test]
fn provisional_provider_type_fields_are_present() {
    let source = include_str!("../../plugins/lib/maki/provider_types.lua");
    let normalized = source.split_whitespace().collect::<Vec<_>>().join(" ");
    for field in [
        "export type ModelFamily = \"claude\" | \"generic\" | \"gemini\" | \"glm\" | \"gpt\" | \"synthetic\"",
        "family: ModelFamily?",
        "family: ModelFamily?, supports_thinking: boolean?",
        "protocol: (\"openai\" | \"openai-responses\" | \"anthropic\" | \"google\")?",
        "supports_vision: boolean?, base: string?, path_prefix: string?, models: { [string]: OverrideFields }?",
        "export type PricingWindow = { start: number, [\"end\"]: number }",
        "ty: string?, min: number?",
        "media_type: \"image/png\" | \"image/jpeg\" | \"image/gif\" | \"image/webp\"",
    ] {
        assert!(
            normalized.contains(field),
            "missing provider type declaration: {field}"
        );
    }
    assert!(!source.contains("default: OverrideFields?"));
}

#[test]
fn annotated_provider_contract_examples_load() {
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source(
        "provider_types_test",
        r#"
local Types = require("maki.provider_types")
assert(Types == require("maki.provider_types"))
assert(table.isfrozen(Types))
type Model = Types.Model
type Provider = Types.Provider
type ProviderContext = Types.ProviderContext
type ProviderRequest = Types.ProviderRequest
type ProviderResponse = Types.ProviderResponse
type ProviderError = Types.ProviderError
type ProviderConfig = Types.ProviderConfig
type ProviderInstance = Types.ProviderInstance
type AuthContext = Types.AuthContext
type AuthInteraction = Types.AuthInteraction
type Emit = Types.Emit

local function stream(_ctx: ProviderContext, _request: ProviderRequest, _emit: Emit): (ProviderResponse?, ProviderError?)
  return nil, { kind = "closed", message = "closed" }
end
local function refresh(_ctx: ProviderContext): (boolean?, ProviderError?)
  return true, nil
end
local function prepare(_ctx: ProviderContext, model: Model, _config: ProviderConfig): (ProviderInstance?, ProviderError?)
  return { model = model, stream = stream, refresh_auth = refresh }, nil
end
local function login(_ctx: AuthContext, _ui: AuthInteraction): (boolean?, ProviderError?)
  return true, nil
end
local function logout(_ctx: AuthContext, _config: ProviderConfig): (boolean?, ProviderError?)
  return true, nil
end
local function discover(_ctx: ProviderContext, _config: ProviderConfig): ({ Model }?, ProviderError?)
  return { { id = "example" } }, nil
end
local provider: Provider = {
  id = "example", display_name = "Example", models = { { id = "example" } },
  auth = { { id = "example", label = "Example", login = login } },
  discover = discover, prepare = prepare, logout = logout,
}
assert(provider.models[1].id == "example")
local instance, prepare_err = provider.prepare({} :: any, provider.models[1], {})
assert(instance and not prepare_err)
assert(instance.refresh_auth({} :: any))
local response, stream_err = instance.stream({} :: any, {} :: any, function() return true, nil end)
assert(response == nil and stream_err.kind == "closed")
local discovered, discover_err = provider.discover({} :: any, {})
assert(discovered and not discover_err and discovered[1].id == "example")
assert(provider.auth[1].login({} :: any, {} :: any))
assert(provider.logout({} :: any, {}))
local message: Types.Message = {
  role = "user", content = {},
  observation_source = { kind = "child_result", agent_id = "agent", turn_id = "turn" },
}
assert(message.observation_source.turn_id == "turn")
local err: ProviderError = { kind = "api", message = "overflow", overflow = { kind = "budget", prompt = 10, limit = 20 } }
assert(err.overflow.limit == 20)
"#,
    )
    .unwrap();
}
