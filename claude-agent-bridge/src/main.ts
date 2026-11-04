#!/usr/bin/env node
import { execFileSync } from "node:child_process"
import { homedir } from "node:os"
import { join } from "node:path"
import { parseArgs } from "node:util"
import { mkdir } from "node:fs/promises"
import { Conversations } from "./conversations.ts"
import { base } from "./options.ts"
import { fileRecords } from "./records.ts"
import { handler, listen } from "./server.ts"
import { probe, startSession } from "./session.ts"

const PROBE_TIMEOUT_MS = 30_000
const USAGE = `Usage: maki-claude-agent-bridge [--socket PATH] [--state DIR] [--claude PATH]

  --state   Where the bridge keeps its socket, conversation records and the
            directory Claude Code runs in. Default: Maki's state directory
            plus /claude-agent, as Maki's claude-agent provider expects.
  --socket  Listen here instead of <state>/bridge.sock.
  --claude  The Claude Code executable. Default: \`claude\` on PATH.`

// Maki's state directory (maki-storage paths.rs), so the provider finds the
// socket without configuration.
function defaultState(): string {
  const base = process.env.XDG_STATE_HOME || join(homedir(), ".local", "state")
  return join(base, "makima", "claude-agent")
}

function which(name: string): string {
  try {
    return execFileSync("sh", ["-c", 'command -v "$1"', "sh", name], { encoding: "utf8" }).trim()
  } catch {
    throw new Error(`${name} is not on PATH; install Claude Code or pass --claude`)
  }
}

async function main(): Promise<void> {
  const { values } = parseArgs({
    options: {
      socket: { type: "string" },
      state: { type: "string" },
      claude: { type: "string" },
      help: { type: "boolean", short: "h" },
    },
  })
  if (values.help) {
    console.log(USAGE)
    return
  }
  const state = values.state ?? defaultState()
  const socket = values.socket ?? join(state, "bridge.sock")
  const workdir = join(state, "workdir")
  await mkdir(workdir, { recursive: true, mode: 0o700 })
  const environment = { executable: values.claude ?? which("claude"), workdir }
  const conversations = new Conversations(environment, startSession, fileRecords(join(state, "conversations")))
  const server = await listen(
    socket,
    handler(conversations, (contextWindows) => probe(base(environment), PROBE_TIMEOUT_MS, contextWindows)),
  )
  console.error(`claude-agent bridge listening on ${socket} (claude: ${environment.executable})`)

  const shutdown = async () => {
    server.close()
    await conversations.close()
    process.exit(0)
  }
  process.once("SIGINT", () => void shutdown())
  process.once("SIGTERM", () => void shutdown())
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : error)
  process.exit(1)
})
