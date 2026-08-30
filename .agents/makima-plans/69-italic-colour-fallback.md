### Goal

Make italic-only Markdown remain visually distinct in terminals whose fonts do not render italics by applying an optional theme colour while preserving the italic attribute. Preserve existing bold-italic colour behavior unless a theme explicitly overrides it.

### Implementation Summary

Implement the fallback in `maki-ui/src/theme.rs`, where `Theme::from_toml` resolves the Markdown `italic` and `bold_italic` styles consumed by `maki-ui/src/markdown.rs`. Derive the default italic-only foreground from the theme’s existing `markup.italic` syntax scope, allow a theme-TOML `[ui].italic` entry to override the derived style, and unconditionally add `Modifier::ITALIC` after resolution so a colour-only override cannot remove the existing attribute.

Keep the existing bold-italic fallback contract: when theme-TOML `[ui].bold_italic` is absent, preserve the entire resolved `bold_style` (including foreground, background, and modifiers) and add only italic, including the existing edge case where a colour-only `[ui].bold` omits bold. When `[ui].bold_italic` is present, treat it as an explicit combined semantic style and unconditionally add both bold and italic modifiers. This avoids recolouring all bundled `***text***` spans and preserves the bold-derived foreground for custom themes that have `markup.bold` but no `markup.italic`.

Document the optional theme-TOML `[ui].italic` and `[ui].bold_italic` keys in the generated configuration reference. Do not alter `maki-markdown` parsing, syntax-highlighted code token styles, unrelated uses of italic modifiers, or all bundled TOML files: all 34 bundled themes already define `markup.italic`, so derivation supplies their intended italic-only fallback colour centrally.

### Implementation Plan

1. Refactor Markdown emphasis style construction in `maki-ui/src/theme.rs` near `Theme::from_toml`.
   - Build a resolved `italic_style` before constructing `Theme`, using theme-TOML `[ui].italic` when present and otherwise deriving the foreground from `markup.italic`.
   - Add `Modifier::ITALIC` after either resolution path. This makes preserving italics an invariant rather than requiring theme authors to repeat `modifiers = ["italic"]` in a colour override.
   - Preserve the existing no-override bold-italic basis exactly: start from `bold_style`, then add only `Modifier::ITALIC`. Do not separately force bold in this path, so a custom colour-only `[ui].bold` keeps its current modifier semantics.
   - If theme-TOML `[ui].bold_italic` is present, resolve that explicit combined style and then add `Modifier::BOLD | Modifier::ITALIC`. This keeps explicit combined-style control while preventing a colour-only combined entry from dropping either semantic modifier.
   - Preserve the no-colour fallback for custom themes that define neither `[ui].italic` nor `markup.italic`: italic remains modifier-only. Preserve a custom theme’s bold-derived foreground for bold-italic when it defines `markup.bold` but no italic colour source.
   - Assign the precomputed styles to `Theme::italic` and `Theme::bold_italic`; `maki-ui/src/markdown.rs::apply_emphasis` can continue consuming those fields without a separate rendering branch.

2. Add focused unit coverage in `maki-ui/src/theme.rs`.
   - Test that a theme with `markup.italic` but no `[ui].italic` derives that foreground and retains `Modifier::ITALIC`.
   - Test that a colour-only theme-TOML `[ui].italic` overrides the syntax-derived colour but still retains `Modifier::ITALIC`.
   - Test that the default bold-italic style preserves the entire resolved `bold_style` and adds only `Modifier::ITALIC`. Cover both a `markup.bold`-derived style with no `markup.italic` and a colour-only `[ui].bold` override that deliberately omits the bold modifier.
   - Test that a colour-only theme-TOML `[ui].bold_italic` takes precedence while still containing both semantic modifiers.
   - Test the compatibility edge case with no italic colour source: italic foreground remains unset and retains `Modifier::ITALIC`; bold-italic retains an available bold foreground and the modifiers from `bold_style`, plus `Modifier::ITALIC`.
   - Strengthen the existing Dracula derivation assertion to pin its `markup.italic` colour as the runtime Markdown italic-only fallback and its current bold-derived colour as the default bold-italic foreground.

3. Strengthen rendering-level regression coverage in `maki-ui/src/markdown.rs`.
   - Extend the italic Markdown test to assert both the italic modifier and a deterministic bundled theme’s fallback foreground.
   - Add or extend a bold-italic Markdown case (`***text***`) to assert the inherited `bold_style` fields and modifiers plus `Modifier::ITALIC`. With deterministic Dracula, this includes bold because Dracula’s `bold_style` contains it; do not make that a universal guarantee for colour-only `[ui].bold`.
   - In colour-sensitive renderer tests, import `crate::theme::ThemesProvider`, acquire `theme::theme_test_guard()`, install a known bundled theme such as Dracula through `InMemoryThemesProvider`, and capture expected styles while holding the guard. This prevents concurrent tests from changing the mutable global theme between render and assertion.

4. Update and test generated user documentation.
   - In `maki-docgen/src/gen_config.rs::write_theme_section`, clearly identify `italic` and `bold_italic` as keys inside a theme TOML file’s `[ui]` table, not fields in Makima’s Lua `[ui]` configuration.
   - Describe `italic`’s `markup.italic` fallback, explicit override precedence, and the existing bold-derived fallback for `bold_italic`. State precisely that absent `[ui].bold_italic` preserves `bold_style` and adds italic, while an explicit colour-only `[ui].bold_italic` receives both bold and italic. Include a small TOML example if that is clearer than prose alone.
   - Add a focused `#[cfg(test)]` test in `maki-docgen/src/gen_config.rs`, named `generated_docs_describe_italic_theme_fallback`, that calls `generate()` and asserts stable contract fragments for the theme-file context, `[ui]` keys, `markup.italic`, fallback precedence, and modifier preservation.
   - Regenerate `site/docs/content/configuration/_index.md` with `just gen-docs` rather than editing the generated page by hand.

5. Validate the implementation from narrow to broad.
   - Run `cargo test -p maki-ui theme::tests` and the relevant `markdown::tests` filters while iterating.
   - Run the focused generator test with `cargo test -p maki-docgen generated_docs_describe_italic_theme_fallback` (the binary crate reports it under `gen_config::tests`).
   - Run `cargo check -p maki-ui --tests` and `cargo check -p maki-docgen --tests`.
   - Run `cargo fmt --all -- --check` and `cargo clippy -p maki-ui --all-targets -- -D warnings`; include `maki-docgen` in clippy coverage or use repository-wide `just lint`.
   - Run `just gen-docs-check` to ensure generated documentation is current.
   - Run `cargo nextest run -p maki-ui -p maki-docgen` as the affected-crate regression suite; escalate to `just test` if changes or failures indicate cross-workspace impact.

### Acceptance Criteria

- **AC.1:** Italic-only Markdown rendered under a theme with `markup.italic` and no theme-TOML `[ui].italic` uses that scope’s foreground colour and still has `Modifier::ITALIC`.
- **AC.2:** A colour-only theme-TOML `[ui].italic` entry overrides the derived foreground without removing `Modifier::ITALIC`.
- **AC.3:** Without theme-TOML `[ui].bold_italic`, bold-italic Markdown preserves the entire resolved `bold_style` and adds only `Modifier::ITALIC`; a colour-only explicit `[ui].bold_italic` overrides the style and contains both `Modifier::BOLD` and `Modifier::ITALIC`.
- **AC.4:** A custom theme with no italic colour source remains backward-compatible: italic adds `Modifier::ITALIC` without forcing a foreground, while default bold-italic preserves every field and modifier from `bold_style` and adds only `Modifier::ITALIC`, including when a colour-only `[ui].bold` omits bold.
- **AC.5:** The configuration reference clearly explains the theme-TOML location, fallback and override rules, and modifier invariants for italic and bold-italic; generated documentation is synchronized with its source.
- **AC.6:** The affected crates pass formatting, compilation, linting, documentation-generation, and regression checks.

### Test Strategy

| Acceptance criterion | Named regression test or check |
|---|---|
| AC.1 | `theme::tests::italic_derives_markup_italic_colour_and_modifier`; deterministic `markdown::tests::italic_uses_theme_colour_and_italic_modifier` |
| AC.2 | `theme::tests::ui_italic_colour_overrides_derivation_and_preserves_modifier` |
| AC.3 | `theme::tests::bold_italic_preserves_bold_style_and_adds_italic`; `theme::tests::ui_bold_italic_colour_overrides_fallback_and_preserves_both_modifiers`; deterministic `markdown::tests::bold_italic_preserves_theme_bold_style_and_adds_italic` |
| AC.4 | `theme::tests::missing_italic_colour_preserves_modifier_only_italic_and_bold_style_fallback`, with `markup.bold`/no `markup.italic` and colour-only `[ui].bold` fixtures that compare the inherited style fields and exact modifier set plus ITALIC |
| AC.5 | `gen_config::tests::generated_docs_describe_italic_theme_fallback` via `cargo test -p maki-docgen generated_docs_describe_italic_theme_fallback`; `just gen-docs-check` |
| AC.6 | `cargo check -p maki-ui --tests`; `cargo check -p maki-docgen --tests`; `cargo fmt --all -- --check`; clippy with warnings denied for both affected crates; `cargo nextest run -p maki-ui -p maki-docgen`; `just gen-docs-check` |

The theme parser tests isolate source precedence and modifier invariants. The Markdown renderer tests cover observable `ratatui::Span` output under a locked, deterministic theme. The doc-generator test fails if the new contract disappears even when generated files are internally synchronized. No terminal-font integration harness is needed because terminal support only affects whether the already-emitted italic attribute is visible; the fallback requirement is observable in emitted foreground and modifier flags.

### Review Strategy

Before handoff, run `plan-reviewer` against this plan and resolve or rebut all critical/high findings, repeating review if necessary.

After implementation and all automated checks, dispatch `general-purpose` (or the repository’s preferred reviewer if one is documented) to review the focused diff for correctness, compatibility, test quality, and unnecessary scope. Fix or explicitly rebut every finding; repeat review after any critical finding until no critical findings remain.

### Documentation Strategy

Update the canonical generated configuration reference through `maki-docgen/src/gen_config.rs`, add generator-level coverage for the new contract, then regenerate `site/docs/content/configuration/_index.md`. No separate guide or README change is needed: this is an optional theme-reference contract, and the configuration reference is its canonical Diátaxis home.

### Risks, Blockers, and Required Decisions

- The issue does not explicitly define bold-italic recolouring. This plan deliberately preserves the existing bold-derived foreground by default, limiting the new colour fallback to italic-only text. Bold-italic already remains visually distinct through bold even when a font lacks italics, and theme authors retain explicit `[ui].bold_italic` control.
- Theme style resolution currently tolerates unresolved colour names by omitting the foreground. The new logic should preserve that leniency and still add semantic modifiers.
- Headings deliberately preserve their heading colour in `maki-ui/src/markdown.rs::apply_emphasis`; they should continue to gain modifiers without being recoloured. Tests should avoid interpreting this existing heading-specific behavior as a failure of the plain-text fallback.
- Syntax-highlighted code carries its own colours and modifiers and bypasses Markdown emphasis colour application. That behavior is outside issue #69 and should remain unchanged.
- The global active theme is mutable across tests. Colour-sensitive renderer tests must use the existing theme test lock and a known installed theme to avoid flakiness.
- No blocker or operator decision remains. The narrowed behavior satisfies the issue while preserving the current combined-style colour contract.