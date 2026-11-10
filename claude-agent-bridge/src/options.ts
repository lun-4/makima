import { randomUUID } from "node:crypto"
import type { Options } from "@anthropic-ai/claude-agent-sdk"
import type { StepRequest } from "./protocol.ts"
import type { Record } from "./records.ts"
import type { Effort } from "./session.ts"
import { hooks, PREFIX, PendingTools, SERVER_NAME, server } from "./tools.ts"

// Claude Code features Makima has its own version of, or that would let the
// user's Claude configuration leak into Makima's requests.
const ISOLATION_ENV: { [name: string]: string | undefined } = {
  CLAUDE_CODE_DISABLE_CLAUDE_MDS: "1",
  CLAUDE_CODE_DISABLE_AUTO_MEMORY: "1",
  DISABLE_AUTO_COMPACT: "1",
  CLAUDE_CODE_ENABLE_AWAY_SUMMARY: "0",
  ENABLE_TOOL_SEARCH: "false",
  CLAUDE_CODE_DISABLE_ADVISOR_TOOL: "1",
  // Raised so Makima's own output limits apply instead.
  MAX_MCP_OUTPUT_TOKENS: "200000",
  // Skips OAuth, so a subscription login would not be used.
  CLAUDE_CODE_SIMPLE: undefined,
  // Would override the effort Makima asks for.
  CLAUDE_CODE_EFFORT_LEVEL: undefined,
  // Would cut off Makima tools that wait on the user, such as a permission prompt.
  MCP_TOOL_TIMEOUT: undefined,
}

const EFFORTS: { [level: string]: Effort } = { minimal: "low", low: "low", medium: "medium", high: "high", xhigh: "xhigh", max: "max" }

export interface Environment {
  readonly executable: string
  readonly workdir: string
}

export function effort(request: StepRequest): Effort | undefined {
  return request.thinking.kind === "effort" ? EFFORTS[request.thinking.level] : undefined
}

function thinking(request: StepRequest): Options["thinking"] {
  switch (request.thinking.kind) {
    case "off":
      return { type: "disabled" }
    case "budget":
      return { type: "enabled", budgetTokens: request.thinking.tokens, display: "summarized" }
    default:
      // Without summaries Makima would have no reasoning to show.
      return { type: "adaptive", display: "summarized" }
  }
}

// What a running process cannot change: a different launch key needs a new
// one. Model and effort change on the running process.
export function launchKey(request: StepRequest): string {
  const { kind } = request.thinking
  const budget = request.thinking.kind === "budget" ? request.thinking.tokens : undefined
  return JSON.stringify([request.system, request.tools, kind === "off" ? kind : (budget ?? "adaptive")])
}

// Settings for one step's process, with every Claude Code feature that
// duplicates Makima's turned off and no configuration read from the user.
export function base(environment: Environment): Options {
  return {
    pathToClaudeCodeExecutable: environment.executable,
    cwd: environment.workdir,
    env: { ...process.env, ...ISOLATION_ENV },
    settingSources: [],
    settings: { disableAllHooks: true },
    strictMcpConfig: true,
    skills: [],
    tools: [],
    permissionMode: "dontAsk",
    allowedTools: [`${PREFIX}*`],
    verbatimPrompts: true,
  }
}

export function sessionOptions(
  environment: Environment,
  request: StepRequest,
  record: Record,
  pending: PendingTools,
  persist: boolean,
): Options {
  const options: Options = {
    ...base(environment),
    model: request.model.id,
    effort: effort(request),
    thinking: thinking(request),
    systemPrompt: request.system,
    includePartialMessages: true,
    persistSession: persist,
    hooks: hooks(pending),
    mcpServers: request.tools.length > 0 ? { [SERVER_NAME]: server(request.tools, pending) } : {},
  }
  if (record.session) {
    options.resume = record.session
  } else if (record.fork) {
    options.resume = record.fork.session
    options.resumeSessionAt = record.fork.at
    options.forkSession = true
  } else {
    options.sessionId = randomUUID()
  }
  return options
}
