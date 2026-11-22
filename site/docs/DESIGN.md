# Docs design system

**Terminal-editorial.** The one personality channel is Makima's TUI heritage. If you cannot name a reason for a new element, do not add it.

## Principles

1. Text is the interface: fix measure, leading, hierarchy first.
2. Accent means something: links, active state, hover. Never decoration.
3. Density over marketing whitespace.
4. Earn your ornaments; what we omit is the credibility signal.
5. Perf is sacred: zero external requests, static everything.

## Type

- Body: system sans stack (`--font-body`), 16px, line-height 1.6, `--content-max: 680px`. Zero downloads.
- Headings, nav chrome, labels, `th`, code: JetBrains Mono (`--font-mono`), self-hosted variable woff2 latin subset in `static/fonts/` (31KB + 22KB lazy italic). The only loaded font.
- h1 1.5-1.8rem clamp, h2 1.25rem + hairline rule, h3 1rem. Max 3 levels; hierarchy by weight + space, not size.
- `text-wrap: balance` on headings, `pretty` on paragraphs. Space-after only.
- Labels (eyebrow, sidebar groups, toc, th, pn-dir): mono uppercase, 0.64-0.7rem, letter-spacing 0.12-0.14em, muted ink.

## Color

"Warm surfaces, red ink." Two neutrals + one accent, defined once in `base.html` `:root` / `[data-theme="dark"]`; components only use vars.

| | Light | Dark |
|---|---|---|
| Surface | warm paper `#f5eded` | makima `#120c0c` |
| Ink | warm brown `#2a1818` | `#e9dede` |
| Accent | crimson `#e03030` | red `#ff4545` |

- Accent only on: links, active items, hover, focus ring, search marks, note-callout strong.
- Neutral tints: muted mauve `#7a5c5c`, borders `#241818` (dark) / `#e0cfcf` (light).
- Callouts: tip rose (hue ~10), warn amber (hue ~35). Never pure #000/#fff.

## Code blocks

Dark panes (#1E1E2E) in both themes. Custom `makima` syntax theme (`extra/makima.json`, VSCode format, `extra_themes`, zola >= 0.22); single `theme` key so giallo emits plain hex. Tokens, all roman, no italics:

| ink | comment | keyword | operator | function/command | type | string | const |
|---|---|---|---|---|---|---|---|
| `#DDE0EA` | `#878DA9` | `#A2B5FA` | `#A6ADC8` | `#F7A87D` | `#A9D5E3` | `#ABD695` | `#EDA8C2` |

Salmon is the only saturated token (echoes the accent). Shell unquoted args map to ink.

## Banned (AI-slop tells)

Rotation display serifs (Fraunces, Instrument Serif, Space Grotesk...), rounded sans (Nunito), purple/indigo accents, gradients, glassmorphism, border-radius above 2px, hover lifts, card grids with icons, decorative starfields/washes, accent underline bars, colored eyebrows, mac-dots terminal mockups, pill badges, emoji, box-shadows on nav (border shift only; modal shadow excepted).

## Components

- Vocabulary: flat surfaces, 1px hairline borders, 2px accent left-edge for active. Nothing else.
- Radius rule: small typographic chips (inline code, keycaps) get 2px to avoid corner glints at text scale; everything architectural (panes, tables, cards, nav, buttons) stays square.
- Inline code chips: tight padding (0.06rem 0.28rem).
- Docs index: dense list rows (10.5rem title column + muted desc), no cards.
- Tables: mono uppercase `th`. Keycaps: chip + 2px bottom shadow.
- `a code`: chip stripped, accent + underline only.
- Code blocks: hover copy button; no title bar, lang label, or dots.
- Idiosyncratic details, exactly two: hover `#` anchors; `~N tokens` per page (injected by `build.sh`, hidden in local `zola serve`).

## Perf

- No external requests; preload the normal mono face only.
- Animations: transform/opacity only, one rAF scroll handler, reduced-motion guard.
- Lazy `search.json`, hover prefetch, MPA view transitions 140ms.
- All CSS/JS inline in `base.html` (~64KB/page, one request).

## Verifying

`cd site && ./build.sh` (full: search.json + token spans) or `zola build` in `site/docs` (fast). Check both themes, 390px and 1600px. Screenshot previews can lie about color; pixel-sample PNGs. Landing page not yet migrated.
 can lie about color; pixel-sample PNGs. Landing page not yet migrated.
