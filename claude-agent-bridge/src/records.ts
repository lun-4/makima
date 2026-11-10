import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises"
import { join } from "node:path"

// A point in an SDK session where Claude's transcript holds exactly the
// first {count} Makima messages, ending with a finished reply. A fork from it
// needs only the messages after {count}.
export interface Boundary {
  readonly count: number
  readonly session: string
  readonly uuid: string
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
  // The last turn was interrupted, so Claude's transcript holds the partial
  // reply and the cancelled tool results itself.
  interrupted?: { tools: string[] }
}

export function freshRecord(): Record {
  return { seen: 0, boundaries: [] }
}

// The last boundary within the first {shared} messages.
export function forkPoint(record: Record, shared: number): Boundary | undefined {
  return record.boundaries.findLast((boundary) => boundary.count <= shared)
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
