<img src="./banner.png">

makima is a harness for AI/LLM coding agents targeting stability and extensibility with a Rust core and Luau plugins.

primary features include but are not limited to:
- async and nested subagents
   - you can also select specific models, e.g. you can type `@6.2-sol`, press TAB, and watch as your main model creates a completely separate subagent with the model you want.
- extensible LLM providers via a dedicated Luau API
- see images the model is fetching if your terminal supports it
- ACP support so you can use e.g Zed

## installation procedure

makima supports linux, windows, and macos.

right now, makima is in constant flux and used by some friends of ours. we recommend users either.
1. build from source

```
git clone https://github.com/lun-4/makima
cd makima
cargo build --release
```

2. get prebuilt binaries from [the latest release](https://github.com/lun-4/makima/releases/latest).

## inspirations

- [maki](https://github.com/tontinton/maki) as it's the core codebase we've hardforked from. _maki is good! I just have different opinions that I don't think are upstreamable!_
- [polytoken](https://polytoken.dev)'s plan mode which we've copied.

## supported providers

_makima supports way more than just these providers, but we can confirm that these providers will work out-of-the-box as
we work with their models on a day-to-day basis_

- OpenAI Codex
- OpenRouter
- Synthetic
- Opencode Go
- Claude Agent (so you can use your subscription, inspired by [opencode](https://github.com/anomalyco/opencode))
- Any OpenAI-compatible provider (which includes llama.cpp, Strata, ninfer-all, and other local backends)

## other installation procedures

### with `cargo install`

latest `mistress`:

```sh
cargo install --locked --git https://github.com/lun-4/makima.git makima
```

### with nix

```sh
nix run github:lun-4/makima
```

## documentation

more info at the [official docs](https://lun-4.github.io/makima/docs).
