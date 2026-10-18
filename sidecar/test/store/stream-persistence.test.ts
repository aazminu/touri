import { Database } from "bun:sqlite"
import { afterEach, expect, test } from "bun:test"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { openDatabase } from "../../src/kernel/db"
import { streamPersistenceWorkerSource } from "../../src/store/stream-persistence-worker"

const cleanups = new Set<() => void | Promise<void>>()

afterEach(async () => {
  for (const cleanup of Array.from(cleanups).reverse()) await cleanup()
  cleanups.clear()
})

test("a restarted worker replays a committed batch without duplicating its journal or event", async () => {
  const root = await mkdtemp(join(tmpdir(), "touri-stream-persistence-"))
  cleanups.add(() => rm(root, { recursive: true, force: true }))
  const filename = join(root, "touri.db")
  const database = openDatabase({ filename })
  database
    .query(
      "INSERT INTO bots(id,name,provider,home_dir,created_at,updated_at) VALUES ('bot','Synthetic','codex','.',1,1)",
    )
    .run()
  database
    .query("INSERT INTO threads(id,bot_id,title,created_at,updated_at) VALUES ('thread','bot','Synthetic',1,1)")
    .run()
  database
    .query(`INSERT INTO messages(id,thread_id,role,kind,status,text,parts_json,created_at,updated_at)
      VALUES ('reply','thread','assistant','text','streaming','','[{"type":"text","text":""}]',1,1)`)
    .run()
  database.close()
  const batch = {
    id: "stable-batch",
    events: [
      {
        type: "message.delta",
        threadId: "thread",
        messageId: "reply",
        partIndex: 0,
        partType: "text",
        delta: "durable",
        offset: 0,
        generation: 1,
        updatedAt: 2,
      },
    ],
  }

  await persistBatch({ filename, batch })
  await persistBatch({ filename, batch })

  const recovered = new Database(filename, { readonly: true })
  cleanups.add(() => recovered.close())
  const journalRows = recovered
    .query("SELECT COUNT(*) AS count FROM transcript_entries WHERE json_extract(event_json,'$.type')='message.delta'")
    .get() as { count: number }
  const eventRows = recovered
    .query("SELECT COUNT(*) AS count FROM ui_event_log WHERE json_extract(event_json,'$.type')='message.delta'")
    .get() as { count: number }
  const projection = recovered.query("SELECT text FROM messages WHERE id = 'reply'").get()
  expect(journalRows).toEqual({ count: 1 })
  expect(eventRows).toEqual({ count: 1 })
  expect(projection).toEqual({ text: "durable" })
})

const persistBatch = async ({ filename, batch }: { filename: string; batch: unknown }): Promise<void> => {
  const workerUrl = URL.createObjectURL(new Blob([streamPersistenceWorkerSource], { type: "application/javascript" }))
  const worker = new Worker(workerUrl, { smol: true, ref: false })
  try {
    await new Promise<void>((resolve, reject) => {
      worker.onmessage = ({ data }: MessageEvent<{ type?: string; batch?: unknown }>): void => {
        if (data.type === "ready") {
          worker.postMessage({ type: "batch", batch })
          return
        }
        if (data.type === "committed") {
          resolve()
          return
        }
        reject(new Error("Stream persistence worker failed"))
      }
      worker.onerror = (): void => reject(new Error("Stream persistence worker failed"))
      worker.postMessage({ type: "start", filename })
    })
  } finally {
    worker.terminate()
    URL.revokeObjectURL(workerUrl)
  }
}
