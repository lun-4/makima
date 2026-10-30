use std::fs;
use std::path::Path;

use maki_domain::MODEL_OPTION_SPECS;
use test_case::test_case;

const SHARED_CONVERSION_REQUIRED: &str =
    "delegate model selections to prepare_selection or ConfigPatch::from_model_selection";
const LOCAL_OPTION_MAPPING: &str =
    "local model-option mapping belongs in ConfigPatch::from_model_selection";
const SHARED_IDENTITY_REQUIRED: &str = "delegate model identity resolution to resolve_model_spec";
const LOCAL_IDENTITY_RESOLUTION: &str =
    "local model identity resolution belongs in resolve_model_spec";
const COMPLETION_IMPL: &[&str] = &["impl", "CommandCompletion", "for", "ModelArgSource"];
const TUI_SELECTION_WRAPPER: &str = "commit_model_selection";
const CONVERSION_ENTRY_POINTS: &[(&str, &str, Option<&str>)] = &[
    (
        "maki-ui/src/event_loop.rs",
        "dispatch_model_selection",
        Some(TUI_SELECTION_WRAPPER),
    ),
    (
        "maki-ui/src/event_loop/approval.rs",
        "prepare_plan_approval",
        None,
    ),
    (
        "maki-agent/src/session_coordinator.rs",
        "set_model_with_claim",
        None,
    ),
    (
        "maki-lua/src/orchestration/command_host.rs",
        "select_model",
        None,
    ),
    (
        "maki-lua/src/api/agent/handles/agent_methods.rs",
        "agent_set_model",
        None,
    ),
];

fn source_tokens(source: &str) -> Vec<&str> {
    let mut tokens = Vec::new();
    let mut offset = 0;
    while offset < source.len() {
        let rest = &source[offset..];
        let character = rest.chars().next().unwrap();
        if character.is_whitespace() {
            offset += character.len_utf8();
            continue;
        }
        if rest.starts_with("//") {
            offset += rest.find('\n').unwrap_or(rest.len());
            continue;
        }
        if rest.starts_with("/*") {
            offset += 2;
            let mut depth = 1;
            while depth != 0 && offset < source.len() {
                if source[offset..].starts_with("/*") {
                    depth += 1;
                    offset += 2;
                } else if source[offset..].starts_with("*/") {
                    depth -= 1;
                    offset += 2;
                } else {
                    offset += source[offset..].chars().next().unwrap().len_utf8();
                }
            }
            continue;
        }
        let start = offset;
        let hashes = rest.strip_prefix('r').map(|raw| {
            raw.bytes()
                .take_while(|character| *character == b'#')
                .count()
        });
        if let Some(hashes) = hashes
            && rest.as_bytes().get(hashes + 1) == Some(&b'"')
        {
            let opening = hashes + 2;
            let closing = format!("\"{}", "#".repeat(hashes));
            offset += opening
                + rest[opening..]
                    .find(&closing)
                    .map_or(rest.len() - opening, |end| end + closing.len());
        } else if character == '"'
            || (character == '\''
                && (rest.as_bytes().get(2) == Some(&b'\'') || rest.starts_with("'\\")))
        {
            offset += 1;
            while offset < source.len() {
                let next = source[offset..].chars().next().unwrap();
                offset += next.len_utf8();
                if next == '\\' {
                    offset += source[offset..].chars().next().map_or(0, char::len_utf8);
                } else if next == character {
                    break;
                }
            }
        } else if character.is_alphanumeric() || character == '_' {
            offset += rest
                .find(|next: char| !next.is_alphanumeric() && next != '_')
                .unwrap_or(rest.len());
        } else {
            offset += character.len_utf8();
        }
        tokens.push(&source[start..offset]);
    }
    tokens
}

fn contains(tokens: &[&str], pattern: &[&str]) -> bool {
    tokens
        .windows(pattern.len())
        .any(|window| window == pattern)
}

fn block_end(tokens: &[&str], opening: usize) -> Result<usize, String> {
    let mut depth = 0;
    for (index, token) in tokens.iter().enumerate().skip(opening) {
        match *token {
            "{" => depth += 1,
            "}" => {
                depth -= 1;
                if depth == 0 {
                    return Ok(index);
                }
            }
            _ => {}
        }
    }
    Err("unterminated source block".to_owned())
}

fn body_after<'a>(tokens: &'a [&'a str], marker: &[&str]) -> Result<&'a [&'a str], String> {
    let matches = tokens
        .windows(marker.len())
        .enumerate()
        .filter_map(|(index, window)| (window == marker).then_some(index))
        .collect::<Vec<_>>();
    let [start] = matches.as_slice() else {
        return Err(format!(
            "expected one source marker {marker:?}, found {}",
            matches.len()
        ));
    };
    let opening = tokens[*start + marker.len()..]
        .iter()
        .position(|token| *token == "{")
        .map(|index| start + marker.len() + index)
        .ok_or_else(|| format!("missing body after {marker:?}"))?;
    Ok(&tokens[opening + 1..block_end(tokens, opening)?])
}

fn function_body<'a>(tokens: &'a [&'a str], name: &str) -> Result<&'a [&'a str], String> {
    body_after(tokens, &["fn", name, "("])
}

fn calls(tokens: &[&str], name: &str) -> bool {
    contains(tokens, &[name, "("])
}

fn check_option_mapping(body: &[&str]) -> Result<(), String> {
    for (index, token) in body.iter().enumerate() {
        if *token != "ConfigPatch" || body.get(index + 1) != Some(&"{") {
            continue;
        }
        let fields = &body[index + 2..block_end(body, index + 1)?];
        let mut depth = 0;
        let mut field_start = true;
        for token in fields {
            if depth == 0 && field_start {
                if MODEL_OPTION_SPECS.iter().any(|spec| spec.key == *token) {
                    return Err(format!("{LOCAL_OPTION_MAPPING}: {token}"));
                }
                field_start = false;
            }
            match *token {
                "{" | "(" | "[" => depth += 1,
                "}" | ")" | "]" => depth -= 1,
                "," if depth == 0 => field_start = true,
                _ => {}
            }
        }
    }
    for spec in MODEL_OPTION_SPECS {
        if body
            .windows(4)
            .any(|window| window[..3] == [".", spec.key, "="] && window[3] != "=")
        {
            return Err(format!("{LOCAL_OPTION_MAPPING}: {} assignment", spec.key));
        }
    }
    Ok(())
}

fn check_conversion(body: &[&str]) -> Result<(), String> {
    check_option_mapping(body)?;
    if calls(body, "prepare_selection")
        || contains(
            body,
            &["ConfigPatch", ":", ":", "from_model_selection", "("],
        )
    {
        Ok(())
    } else {
        Err(SHARED_CONVERSION_REQUIRED.to_owned())
    }
}

fn check_conversion_entry(
    tokens: &[&str],
    entry: &str,
    wrapper: Option<&str>,
) -> Result<(), String> {
    let body = function_body(tokens, entry)?;
    check_option_mapping(body)?;
    if let Some(wrapper) = wrapper
        && calls(body, wrapper)
    {
        return check_conversion(function_body(tokens, wrapper)?);
    }
    check_conversion(body)
}

fn check_identity_algorithm(body: &[&str]) -> Result<(), String> {
    for forbidden in [
        "fuzzy_resolve_candidates",
        "fuzzy_resolve",
        "MatchCandidate",
        "Resolution",
    ] {
        if body.contains(&forbidden) {
            return Err(format!("{LOCAL_IDENTITY_RESOLUTION}: {forbidden}"));
        }
    }
    if contains(body, &["Model", ":", ":", "from_spec", "("])
        || contains(body, &["fields", ":"])
        || ["split_once", "split", "splitn"].iter().any(|method| {
            contains(body, &[".", method, "(", "'/'"])
                || contains(body, &[".", method, "(", "\"/\""])
        })
    {
        return Err(format!(
            "{LOCAL_IDENTITY_RESOLUTION}: exact parsing or candidate fields"
        ));
    }
    for (index, token) in body.iter().enumerate() {
        if !matches!(*token, "if" | "match") {
            continue;
        }
        let Some(opening) = body[index + 1..]
            .iter()
            .position(|token| *token == "{")
            .map(|offset| index + 1 + offset)
        else {
            continue;
        };
        let condition = &body[index + 1..opening];
        if !condition
            .iter()
            .any(|token| matches!(*token, "'/'" | "\"/\""))
        {
            continue;
        }
        let branch = &body[opening + 1..block_end(body, opening)?];
        if calls(branch, "Ok")
            || calls(branch, "Err")
            || contains(branch, &[".", "spec", "="])
            || !contains(branch, &["[", "]"])
        {
            return Err(format!(
                "{LOCAL_IDENTITY_RESOLUTION}: slash branch resolves identity"
            ));
        }
    }
    Ok(())
}

fn check_identity(body: &[&str]) -> Result<(), String> {
    check_identity_algorithm(body)?;
    if calls(body, "resolve_model_spec") {
        Ok(())
    } else {
        Err(SHARED_IDENTITY_REQUIRED.to_owned())
    }
}

fn check_completion_identity(tokens: &[&str]) -> Result<(), String> {
    let implementation = body_after(tokens, COMPLETION_IMPL)?;
    let complete = function_body(implementation, "complete")?;
    check_identity(complete)
}

fn read_source(relative: &str) -> String {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    fs::read_to_string(workspace.join(relative))
        .unwrap_or_else(|error| panic!("cannot read {relative}: {error}"))
}

#[test]
fn structured_model_changes_delegate_to_shared_conversion() {
    for &(path, function, wrapper) in CONVERSION_ENTRY_POINTS {
        let source = read_source(path);
        let tokens = source_tokens(&source);
        if let Err(error) = check_conversion_entry(&tokens, function, wrapper) {
            panic!("{path}::{function}: {error}");
        }
    }
}

#[test]
fn model_identity_consumers_delegate_to_shared_resolver() {
    let command = read_source("maki-agent/src/command.rs");
    let tokens = source_tokens(&command);
    let implementation = body_after(
        &tokens,
        &["impl", "CommandBehavior", "for", "BuiltinBehavior"],
    )
    .unwrap();
    let execute = function_body(implementation, "execute").unwrap();
    let model = body_after(execute, &["BuiltinId", ":", ":", "Model", "=", ">"]).unwrap();
    check_identity(model)
        .unwrap_or_else(|error| panic!("BuiltinBehavior::execute model branch: {error}"));

    let completion = read_source("maki-ui/src/components/arg_completion.rs");
    check_completion_identity(&source_tokens(&completion))
        .unwrap_or_else(|error| panic!("ModelArgSource::complete: {error}"));
}

#[test_case("{field}: selection.{field},"; "explicit field")]
#[test_case("{field},"; "shorthand field")]
fn copied_option_initializers_fail_even_with_shared_delegation(mapping: &str) {
    for spec in MODEL_OPTION_SPECS {
        let mapping = mapping.replace("{field}", spec.key);
        let source = format!(
            "fn select_model() {{ prepare_selection(&selection, policy, prepare); \
             let patch = ConfigPatch {{ model: prepared, {mapping} ..Default::default() }}; }}"
        );
        let tokens = source_tokens(&source);
        let error = check_conversion_entry(&tokens, "select_model", None).unwrap_err();
        assert!(error.starts_with(LOCAL_OPTION_MAPPING), "{error}");
        assert!(error.contains(spec.key), "{error}");
    }
}

#[test]
fn copied_option_assignments_fail_even_with_shared_delegation() {
    for spec in MODEL_OPTION_SPECS {
        let source = format!(
            "fn select_model() {{ let mut patch = ConfigPatch::from_model_selection(&selection); \
             patch.{field} = selection.{field}; }}",
            field = spec.key,
        );
        let tokens = source_tokens(&source);
        let error = check_conversion_entry(&tokens, "select_model", None).unwrap_err();
        assert!(error.starts_with(LOCAL_OPTION_MAPPING), "{error}");
        assert!(error.contains(spec.key), "{error}");
    }
}

#[test_case("prepare_selection(&selection, policy, prepare).await"; "preparation")]
#[test_case("ConfigPatch::from_model_selection(&selection)"; "conversion")]
fn shared_conversion_passes_with_option_reads_and_unrelated_mappings(conversion: &str) {
    for spec in MODEL_OPTION_SPECS {
        let source = format!(
            "fn select_model() {{ if selection.{field}.is_some() {{ record(); }} \
             let patch = {conversion}; }} \
             fn unrelated() {{ let patch = ConfigPatch {{ {field}: legacy, ..Default::default() }}; }}",
            field = spec.key,
        );
        let tokens = source_tokens(&source);
        assert_eq!(
            check_conversion_entry(&tokens, "select_model", None),
            Ok(())
        );
    }
}

#[test]
fn designated_conversion_wrapper_is_checked_as_well_as_its_caller() {
    let source = "fn dispatch_model_selection() { commit_model_selection(&selection); }
        async fn commit_model_selection() { prepare_selection(&selection, policy, prepare); }";
    let tokens = source_tokens(source);
    assert_eq!(
        check_conversion_entry(
            &tokens,
            "dispatch_model_selection",
            Some(TUI_SELECTION_WRAPPER)
        ),
        Ok(())
    );
    for spec in MODEL_OPTION_SPECS {
        let mapping = format!(
            "let patch = ConfigPatch {{ {}, ..Default::default() }};",
            spec.key
        );
        for marker in [
            "commit_model_selection(&selection);",
            "prepare_selection(&selection, policy, prepare);",
        ] {
            let mutated = source.replacen(marker, &format!("{marker} {mapping}"), 1);
            let tokens = source_tokens(&mutated);
            let error = check_conversion_entry(
                &tokens,
                "dispatch_model_selection",
                Some(TUI_SELECTION_WRAPPER),
            )
            .unwrap_err();
            assert!(error.starts_with(LOCAL_OPTION_MAPPING), "{error}");
        }
    }
}

#[test_case("let candidates = specs.iter().map(|spec| MatchCandidate { value: spec, fields: vec![spec, provider, model_id] }).collect();"; "candidate construction")]
#[test_case("let (provider, model_id) = spec.split_once('/').unwrap();"; "candidate identity fields")]
#[test_case("match fuzzy_resolve_candidates(argument, &candidates) { Resolution::Unique(index) => Ok(specs[index].clone()) }"; "fuzzy resolution")]
#[test_case("if argument.contains('/') { Model::from_spec(argument)?; return Ok(Arc::from(argument)); }"; "exact parsing")]
#[test_case("if argument.contains('/') { return Ok(Arc::from(argument)); }"; "exact return")]
#[test_case("let identity = if argument.contains('/') { Arc::from(argument) } else { resolve_model_spec(argument, specs)? };"; "exact expression")]
#[test_case("if argument.contains('/') { return Err(invalid_identity()); }"; "exact error")]
#[test_case("if argument.contains('/') { selection.spec = Some(Arc::from(argument)); }"; "exact assignment")]
fn copied_identity_logic_fails_even_with_shared_delegation(copy: &str) {
    let source = format!("fn resolve_identity() {{ {copy} resolve_model_spec(argument, &specs) }}");
    let tokens = source_tokens(&source);
    let error = check_identity(function_body(&tokens, "resolve_identity").unwrap()).unwrap_err();
    assert!(error.starts_with(LOCAL_IDENTITY_RESOLUTION), "{error}");
}

#[test]
fn identity_context_shortcut_and_unrelated_fuzzy_matching_remain_allowed() {
    let source = "fn execute() { match id {
        BuiltinId::Model => {
            let specs = if model.is_empty() || model.contains('/') { Arc::from([]) }
                else { host_context(ModelSpecs).await? };
            if !model.is_empty() { selection.spec = Some(resolve_model_spec(model, &specs)?); }
        }
        BuiltinId::Theme => { fuzzy_resolve(theme, &names); }
    }}";
    let tokens = source_tokens(source);
    let execute = function_body(&tokens, "execute").unwrap();
    let model = body_after(execute, &["BuiltinId", ":", ":", "Model", "=", ">"]).unwrap();
    assert_eq!(check_identity(model), Ok(()));
}

#[test]
fn completion_identity_check_is_bounded_to_model_completion() {
    let source = "impl CommandCompletion for ModelArgSource {
        fn complete() { resolve_model_spec(argument, specs); }
    }
    impl CommandCompletion for ThemeArgSource {
        fn complete() { fuzzy_resolve(argument, specs); }
    }";
    assert_eq!(check_completion_identity(&source_tokens(source)), Ok(()));
    let mutated = source.replacen(
        "resolve_model_spec(argument, specs);",
        "fuzzy_resolve(argument, specs); resolve_model_spec(argument, specs);",
        1,
    );
    let tokens = source_tokens(&mutated);
    let error = check_completion_identity(&tokens).unwrap_err();
    assert!(error.starts_with(LOCAL_IDENTITY_RESOLUTION), "{error}");
}

#[test]
fn helper_names_in_comments_and_literals_do_not_count_as_delegation() {
    let source = r###"fn select_model() {
        // prepare_selection(&selection, policy, prepare);
        /* outer { /* ConfigPatch::from_model_selection(&selection) */ } */
        let hint = "prepare_selection() { }";
        let raw = r#"ConfigPatch::from_model_selection() { }"#;
    }
    fn resolve_identity() { let hint = "resolve_model_spec()"; }
    fn unrelated() { prepare_selection(); resolve_model_spec(); }
    "###;
    let tokens = source_tokens(source);
    assert_eq!(
        check_conversion_entry(&tokens, "select_model", None),
        Err(SHARED_CONVERSION_REQUIRED.to_owned()),
    );
    assert_eq!(
        check_identity(function_body(&tokens, "resolve_identity").unwrap()),
        Err(SHARED_IDENTITY_REQUIRED.to_owned()),
    );
}

#[test]
fn missing_or_ambiguous_extraction_points_fail_explicitly() {
    let missing = source_tokens("fn unrelated() { prepare_selection(); }");
    assert!(function_body(&missing, "select_model").is_err());
    let duplicated = source_tokens("fn select_model() {} fn select_model() {}");
    assert!(function_body(&duplicated, "select_model").is_err());
}
