const SESSIONS_SRC: &str = include_str!("../../plugins/sessions/init.lua");

#[test]
fn sessions_init_wires_filter_and_selection_reconciliation() {
    assert!(SESSIONS_SRC.contains("Helpers.filter_rows"));
    assert!(SESSIONS_SRC.contains("maki.match.completion"));
    assert!(SESSIONS_SRC.contains("maki.match.compare"));
    assert!(SESSIONS_SRC.contains("Helpers.reconcile_selection"));
    assert!(!SESSIONS_SRC.contains("board.sel_id = nil\n  board.confirm = nil\n  apply_filter()"));
}
