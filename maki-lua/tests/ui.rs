use maki_lua::UiAction;
use maki_lua::test_support::spawn_host_for_tests;

#[test]
fn test_opt_bool_truthiness_visible_window() {
    let (_handle, guard) = spawn_host_for_tests(&[]);
    let actions = guard.host().ui_action_rx();

    // 1. Window with omitted `visible` option must default to visible = true.
    guard
        .host()
        .load_source(
            "test_win_default_visible",
            r#"
            local buf = maki.ui.buf()
            maki.ui.open_win(buf, { title = "DefaultVisible" })
            "#,
        )
        .unwrap();

    let action = actions.recv().expect("expected OpenWin action");
    let UiAction::OpenWin { config, .. } = action else {
        panic!("expected OpenWin action");
    };
    assert_eq!(config.title, "DefaultVisible");
    assert!(
        config.visible,
        "omitted visible option must default to true"
    );

    // 2. Window with explicit `visible = false` option must set visible = false.
    guard
        .host()
        .load_source(
            "test_win_explicit_hidden",
            r#"
            local buf = maki.ui.buf()
            maki.ui.open_win(buf, { title = "ExplicitHidden", visible = false })
            "#,
        )
        .unwrap();

    let action = actions.recv().expect("expected OpenWin action");
    let UiAction::OpenWin { config, .. } = action else {
        panic!("expected OpenWin action");
    };
    assert_eq!(config.title, "ExplicitHidden");
    assert!(
        !config.visible,
        "explicit visible = false must set visible to false"
    );
}
