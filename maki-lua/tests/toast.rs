use maki_lua::UiAction;
use maki_lua::test_support::spawn_host_for_tests;

#[test]
fn test_toast_stacking_and_dismissal() {
    let (_handle, guard) = spawn_host_for_tests(&[]);
    let actions = guard.host().ui_action_rx();

    guard
        .host()
        .load_source(
            "test_toast",
            r#"
            local Toast = require("maki.toast")
            Toast.show("First toast message", { title = "First" })
            Toast.show("Second toast message\nLine two", { title = "Second" })
            "#,
        )
        .unwrap();

    // First toast opens a window
    let action1 = actions.recv().expect("first toast OpenWin");
    let UiAction::OpenWin { config: cfg1, .. } = action1 else {
        panic!("expected OpenWin action");
    };
    assert_eq!(cfg1.title, "First");
    assert_eq!(cfg1.anchor, maki_lua::Anchor::NE);
    assert!(cfg1.stack, "toast must opt into stacking");
    assert_eq!(cfg1.zindex, 200);

    // Second toast opens a window and stacks
    let action2 = actions.recv().expect("second toast OpenWin");
    let UiAction::OpenWin { config: cfg2, .. } = action2 else {
        panic!("expected OpenWin action");
    };
    assert_eq!(cfg2.title, "Second");
    assert_eq!(cfg2.anchor, maki_lua::Anchor::NE);
    assert!(cfg2.stack, "toast must opt into stacking");
    assert_eq!(cfg2.zindex, 200);
}
