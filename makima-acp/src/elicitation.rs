use agent_client_protocol_schema::{
    CreateElicitationRequest, CreateElicitationResponse, ElicitationAction,
    ElicitationContentValue, ElicitationFormMode, ElicitationPropertySchema, ElicitationSchema,
    ElicitationScope, ElicitationSessionScope, EnumOption, MultiSelectPropertySchema, SessionId,
    StringPropertySchema, ToolCallId,
};
use serde_json::Value;

const FORM_TITLE: &str = "Makima question";
const FORM_MESSAGE: &str = "Makima needs your input";

fn labels(options: Option<&Vec<Value>>) -> Vec<String> {
    options
        .map(|opts| {
            opts.iter()
                .filter_map(|o| o.get("label").and_then(Value::as_str).map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// Maps the `question` tool input onto an ACP v1 elicitation form: one
/// property per question, single-select enums for option questions,
/// multi-select arrays for `multiSelect`, plain strings for free text.
pub fn build_form(
    questions: &Value,
    sid: &SessionId,
    tool_call_id: &str,
) -> Option<CreateElicitationRequest> {
    let list = questions.as_array()?;
    let mut schema = ElicitationSchema::new().title(FORM_TITLE);
    for (i, q) in list.iter().enumerate() {
        let key = format!("q{}", i + 1);
        let question = q.get("question").and_then(Value::as_str).unwrap_or("");
        let header = q.get("header").and_then(Value::as_str).unwrap_or("");
        let title = if header.is_empty() {
            question.to_string()
        } else {
            format!("{header}: {question}")
        };
        let multiple = q
            .get("multiSelect")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let options = labels(q.get("options").and_then(Value::as_array));
        let property: ElicitationPropertySchema = if multiple {
            MultiSelectPropertySchema::new(options).title(title).into()
        } else if !options.is_empty() {
            let options: Vec<EnumOption> = options
                .iter()
                .map(|label| EnumOption::new(label.clone(), label.clone()))
                .collect();
            StringPropertySchema::new()
                .title(title)
                .one_of(options)
                .into()
        } else {
            StringPropertySchema::new().title(title).into()
        };
        schema = schema.property(key, property, true);
    }
    Some(CreateElicitationRequest::new(
        ElicitationFormMode::new(
            ElicitationScope::Session(
                ElicitationSessionScope::new(sid.clone())
                    .tool_call_id(ToolCallId::from(tool_call_id.to_string())),
            ),
            schema,
        ),
        FORM_MESSAGE,
    ))
}

/// Encodes the client's answer for the awaiting `question` tool: a dense
/// answers array on `accept`, a dismiss marker on `decline`/`cancel`.
pub fn response_payload(response: CreateElicitationResponse) -> String {
    let payload = match response.action {
        ElicitationAction::Accept(accept) => {
            let mut answers: Vec<Vec<String>> = Vec::new();
            if let Some(content) = accept.content {
                let mut entries: Vec<(String, ElicitationContentValue)> =
                    content.into_iter().collect();
                entries
                    .sort_by_key(|(k, _)| k.trim_start_matches('q').parse::<usize>().unwrap_or(0));
                answers = entries
                    .into_iter()
                    .map(|(_, v)| match v {
                        ElicitationContentValue::String(s) => vec![s],
                        ElicitationContentValue::StringArray(vs) => vs,
                        _ => Vec::new(),
                    })
                    .collect();
            }
            serde_json::json!({ "answers": answers })
        }
        _ => serde_json::json!({ "dismissed": true }),
    };
    payload.to_string()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use agent_client_protocol_schema::{
        CreateElicitationResponse, ElicitationAcceptAction, ElicitationAction,
        ElicitationContentValue, ElicitationMode, ElicitationPropertySchema, ElicitationScope,
    };
    use serde_json::{Value, json};
    use test_case::test_case;

    use super::*;

    #[test]
    fn build_form_maps_option_multiselect_and_text_questions() {
        let sid = SessionId::from("sess_abc");
        let questions = json!([
            {
                "question": "Pick one",
                "header": "Choice",
                "options": [{ "label": "a" }, { "label": "b" }],
            },
            {
                "question": "Pick many",
                "multiSelect": true,
                "options": [{ "label": "x" }, { "label": "y" }],
            },
            { "question": "Free text" },
        ]);

        let request = build_form(&questions, &sid, "tool_1").unwrap();
        let ElicitationMode::Form(form) = &request.mode else {
            panic!("expected form mode");
        };
        let schema = &form.requested_schema;
        assert_eq!(schema.properties.len(), 3);
        assert_eq!(
            schema.required,
            Some(vec!["q1".into(), "q2".into(), "q3".into()])
        );
        let ElicitationPropertySchema::String(single) = &schema.properties["q1"] else {
            panic!("q1 must be a single-select string");
        };
        assert_eq!(single.one_of.as_ref().map(|o| o.len()), Some(2));
        assert!(matches!(
            &schema.properties["q2"],
            ElicitationPropertySchema::Array(_)
        ));
        assert!(matches!(
            &schema.properties["q3"],
            ElicitationPropertySchema::String(_)
        ));
        let ElicitationScope::Session(scope) = &form.scope else {
            panic!("expected session scope");
        };
        assert_eq!(
            scope.tool_call_id.as_ref().map(|id| id.0.as_ref()),
            Some("tool_1")
        );
    }

    #[test]
    fn build_form_rejects_non_array_input() {
        assert!(build_form(&json!({ "questions": [] }), &SessionId::from("s"), "t").is_none());
    }

    #[test]
    fn accept_maps_content_to_dense_answers() {
        let mut content = BTreeMap::new();
        content.insert("q1".into(), ElicitationContentValue::String("a".into()));
        content.insert(
            "q2".into(),
            ElicitationContentValue::StringArray(vec!["x".into(), "y".into()]),
        );
        let response = CreateElicitationResponse::new(ElicitationAction::Accept(
            ElicitationAcceptAction::new().content(content),
        ));
        let payload: Value = serde_json::from_str(&response_payload(response)).unwrap();
        assert_eq!(payload["answers"], json!([["a"], ["x", "y"]]));
    }

    #[test_case(ElicitationAction::Decline; "decline")]
    #[test_case(ElicitationAction::Cancel; "cancel")]
    fn non_accept_actions_dismiss(action: ElicitationAction) {
        let response = CreateElicitationResponse::new(action);
        let payload: Value = serde_json::from_str(&response_payload(response)).unwrap();
        assert_eq!(payload["dismissed"], true);
    }
}
