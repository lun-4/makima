import { createHash } from "node:crypto"
import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises"
import { join } from "node:path"
import type { Message } from "./protocol.ts"

const EMPTY_RESPONSE_MARKER = "(empty)"

// A point in an SDK session where Claude's transcript holds exactly the
// first {count} Makima messages, ending with a finished reply. A fork from it
// needs only the messages after {count}.
export interface Boundary {
  readonly count: number
  readonly session: string
  readonly uuid: string
  readonly replyHash?: string | null
}

export interface Record {
  // The SDK session, once Claude Code has named it.
  session?: string
  // Where a new session starts from instead of empty.
  fork?: { session: string; at: string }
  // How many Makima messages Claude has seen.
  seen: number
  boundaries: Boundary[]
  // Hash of what the last step's process could not change, so a fork only
  // reuses a session that offered the same tools and system prompt.
  launch?: string
  // Tool calls of the reply that ended the last step.
  waiting?: string[]
  // An interrupted turn is replayed unless Makima explicitly ended it as cancelled.
  interrupted?: { tools: string[]; cancelled?: true }
}

export function freshRecord(): Record {
  return { seen: 0, boundaries: [] }
}

export function replyHash(message: Message): string | null {
  const text = message.content.flatMap((block) => (block.type === "text" ? [block.text] : []))
  if (text.some((part) => part.trim() !== "")) return createHash("sha256").update(JSON.stringify(text)).digest("hex")
  const tools = message.content.flatMap((block) => (block.type === "tool_use" ? [block.id] : []))
  return tools.length > 0 ? createHash("sha256").update(JSON.stringify({ tools })).digest("hex") : null
}

export function hasReply(message: Message | undefined, hash?: string | null): boolean {
  if (message?.role !== "assistant") return false
  if (hash === null) {
    const [block] = message.content
    return message.content.length === 1 && block?.type === "text" && block.text === EMPTY_RESPONSE_MARKER
  }
  return hash === undefined || replyHash(message) === hash
}

export function forkPoint(record: Record, messages: readonly Message[], shared = messages.length): Boundary | undefined {
  return record.boundaries.findLast((boundary) => boundary.count <= shared && hasReply(messages[boundary.count - 1], boundary.replyHash))
}

export interface Records {
  load(id: string): Promise<Record | undefined>
  save(id: string, record: Record): Promise<void>
  remove(id: string): Promise<void>
}

export function memoryRecords(): Records & { readonly saved: Map<string, Record> } {
  const saved = new Map<string, Record>()
  return {
    saved,
    load: async (id) => structuredClone(saved.get(id)),
    save: async (id, record) => void saved.set(id, structuredClone(record)),
    remove: async (id) => void saved.delete(id),
  }
}

// One JSON file per conversation, so an SDK session can be resumed after a
// bridge restart. Makima ids are base62, but the name is encoded anyway.
export function fileRecords(dir: string): Records {
  const path = (id: string) => join(dir, `${encodeURIComponent(id)}.json`)
  return {
    async load(id) {
      try {
        return JSON.parse(await readFile(path(id), "utf8")) as Record
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined
        throw error
      }
    },
    async save(id, record) {
      await mkdir(dir, { recursive: true, mode: 0o700 })
      const temporary = `${path(id)}.tmp`
      await writeFile(temporary, JSON.stringify(record), { mode: 0o600 })
      await rename(temporary, path(id))
    },
    async remove(id) {
      await rm(path(id), { force: true })
    },
  }
}
