import { Database } from "bun:sqlite"
import { afterEach, expect, spyOn, test } from "bun:test"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import type { Message } from "@touri/contracts"
import { createKernel } from "../../src/kernel"
import { captureTranscript } from "../../src/kernel/transcript-events"
import { compactTranscript } from "../../src/kernel/transcript-storage"
import { init as initStore } from "../../src/store"
import { createStreamWriter } from "../../src/store/stream-writer"

const cleanups = new Set<() => void | Promise<void>>()
afterEach(async () => {
  for (const cleanup of Array.from(cleanups).reverse()) await cleanup()
  cleanups.clear()
})

test("a stream batch materializes each concurrent reply after commit", async () => {
  const fixture = await getFixture()
  fixture.writer.appendDelta({ threadId: "thread", messageId: "first", partIndex: 0, delta: "first" })
  fixture.writer.appendDelta({ threadId: "thread", messageId: "second", partIndex: 0, delta: "second" })

  fixture.writer.flush()
  await fixture.writer.drainAsync()
  const deltas = fixture.database
    .query("SELECT count(*) AS n FROM transcript_entries WHERE event_json LIKE '%message.delta%'")
    .get()

  expect(fixture.getPending()).toBe(0)
  expect(fixture.getTexts()).toEqual(["first", "second"])
  expect(deltas).toEqual({ n: 2 })
})

test("committed stream batches preserve offsets across concurrent replies", async () => {
  const fixture = await getFixture()
  fixture.writer.appendDelta({ threadId: "thread", messageId: "first", partIndex: 0, delta: "first" })
  fixture.writer.appendDelta({ threadId: "thread", messageId: "second", partIndex: 0, delta: "second" })
  fixture.writer.flush()
  await fixture.writer.drainAsync()

  fixture.writer.appendDelta({ threadId: "thread", messageId: "first", partIndex: 0, delta: " more" })
  fixture.writer.appendDelta({ threadId: "thread", messageId: "second", partIndex: 0, delta: " more" })
  fixture.writer.flush()
  await fixture.writer.drainAsync()

  expect(fixture.getPending()).toBe(0)
  expect(fixture.getTexts()).toEqual(["first more", "second more"])
  expect(fixture.getOffsets()).toEqual([0, 0, 5, 6])
})

test("a busy stream batch retries without losing either reply", async () => {
  const fixture = await getFixture()
  await fixture.writer.ready
  fixture.writer.appendDelta({ threadId: "thread", messageId: "first", partIndex: 0, delta: "first" })
  fixture.writer.appendDelta({ threadId: "thread", messageId: "second", partIndex: 0, delta: "second" })
  const blocker = new Database(fixture.database.filename)
  cleanups.add(() => blocker.close())
  blocker.exec("BEGIN IMMEDIATE")
  fixture.writer.flush()
  await Bun.sleep(20)

  expect(fixture.getPending()).toBe(0)
  expect(fixture.getTexts()).toEqual(["", ""])

  blocker.exec("ROLLBACK")
  await fixture.writer.drainAsync()

  expect(fixture.getPending()).toBe(0)
  expect(fixture.getTexts()).toEqual(["first", "second"])
})

test("retaining old history does not materialize unrelated pending replies", async () => {
  const fixture = await getFixture()
  const thread = fixture.database.query("SELECT updated_seq, generation FROM threads WHERE id = 'thread'").get() as {
    updated_seq: number
    generation: number
  }
  const replyIds = ["first", "second"]
  fixture.database.transaction(() => {
    replyIds.forEach((messageId, index) => {
      const event = {
        type: "message.delta",
        threadId: "thread",
        messageId,
        partIndex: 0,
        partType: "text",
        delta: "live",
        offset: 0,
        generation: thread.generation,
        updatedSeq: thread.updated_seq + index + 1,
        updatedAt: Date.now(),
      }
      fixture.database.query("UPDATE threads SET updated_seq = ? WHERE id = 'thread'").run(event.updatedSeq)
      fixture.database
        .query("INSERT INTO transcript_entries(thread_id, seq, generation, event_json, at) VALUES (?, ?, ?, ?, ?)")
        .run("thread", event.updatedSeq, thread.generation, JSON.stringify(event), event.updatedAt)
    })
  })()
  captureTranscript(fixture.database)
  const queries = spyOn(fixture.database, "query")
  cleanups.add(() => {
    queries.mockRestore()
  })

  compactTranscript({ database: fixture.database, entriesPerThread: 4 })
  const statements = queries.mock.calls
    .map(([sql]) => sql)
    .filter((sql) => sql.startsWith("SELECT 1 FROM transcript_pending_deltas p"))
  const plans = statements.flatMap(
    (sql) => fixture.database.query(`EXPLAIN QUERY PLAN ${sql}`).all("thread", 1, 1) as { detail: string }[],
  )

  expect(fixture.getPending()).toBe(2)
  expect(fixture.getTexts()).toEqual(["", ""])
  expect(statements).toHaveLength(2)
  expect(plans.some(({ detail }) => /SCAN (p|m|e|part)\b/.test(detail))).toBe(false)
  console.info(`stream-retention-query-plans ${JSON.stringify(plans)}`)
})

const getFixture = async () => {
  const root = await mkdtemp(join(tmpdir(), "touri-checkpoint-bounded-"))
  const runtime = createKernel({ appData: root })
  runtime.kernel.services.use("storage.maintenance")?.dispose()
  await initStore(runtime.kernel)
  const database = runtime.kernel.db as Database
  database.exec(`INSERT INTO bots(id,name,provider,home_dir,created_at,updated_at)
    VALUES ('bot','Synthetic','codex','/tmp',1,1);
    INSERT INTO threads(id,bot_id,title,created_at,updated_at) VALUES ('thread','bot','Synthetic',1,1)`)
  const store = runtime.kernel.services.use("store.api")
  if (!store) throw new Error("Fixture store missing")

  const messageIds = ["old", "first", "second"]
  messageIds.forEach((id) => {
    const result = store.insertMessage({ message: getMessage(id) })
    if (!result.success) throw new Error("Fixture message missing")
  })
  const at = 0
  const getStoredMessage = ({ threadId, messageId }: { threadId: string; messageId: string }): Message | null => {
    const row = database
      .query("SELECT payload FROM transcript_message_events WHERE thread_id = ? AND id = ?")
      .get(threadId, messageId) as { payload: string } | null
    if (!row) return null

    return (JSON.parse(row.payload) as { message: Message }).message
  }
  const writer = createStreamWriter({
    kernel: runtime.kernel,
    database,
    getMessage: getStoredMessage,
    getThread: store.getThread,
    getNow: () => at,
  })
  await writer.ready
  cleanups.add(async () => {
    await writer.drainAsync({ stopping: true })
    await runtime.stop()
    await rm(root, { recursive: true, force: true })
  })

  return {
    database,
    writer,
    getPending: () => (database.query("SELECT count(*) AS n FROM transcript_pending_deltas").get() as { n: number }).n,
    getOffsets: () =>
      (
        database
          .query(
            "SELECT json_extract(event_json, '$.offset') AS offset FROM transcript_entries WHERE json_extract(event_json, '$.type') = 'message.delta' ORDER BY seq",
          )
          .all() as { offset: number }[]
      ).map(({ offset }) => offset),
    getTexts: () =>
      (
        database.query("SELECT text FROM messages WHERE id IN ('first','second') ORDER BY id").all() as {
          text: string
        }[]
      ).map(({ text }) => text),
  }
}

const getMessage = (id: string): Omit<Message, "createdAt" | "updatedAt"> => ({
  id,
  threadId: "thread",
  parentId: null,
  role: "assistant",
  kind: "text",
  status: id === "old" ? "complete" : "streaming",
  runId: null,
  text: "",
  parts: [{ type: "text", text: "" }],
  sender: null,
  source: "local",
})
