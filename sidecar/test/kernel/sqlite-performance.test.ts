import { Database } from "bun:sqlite"
import { afterEach, expect, mock, test } from "bun:test"
import type { Kernel } from "@touri/contracts"
import { createPerformanceDiagnostics, createPerformanceStageTracker } from "../../src/kernel/performance"
import { createSlowStatementTracker, instrumentMainConnection } from "../../src/kernel/sqlite-performance"

const databases = new Set<Database>()
const cleanups = new Set<() => void>()

afterEach(() => {
  cleanups.forEach((cleanup) => {
    cleanup()
  })
  cleanups.clear()
  databases.forEach((database) => {
    database.close()
  })
  databases.clear()
})

test("main connection reports normalized statements at the 250 ms threshold", () => {
  const log = mock<Kernel["log"]>()
  let times = [0, 0, 0, 249]
  const database = getDatabase({ log, now: () => times.shift() ?? 249 })

  database
    .query("SELECT id AS \"privatevalue\" FROM fixture WHERE label = 'private-value' AND id = 123 -- private-comment")
    .get()

  expect(log).not.toHaveBeenCalled()

  times = [0, 0, 0, 250]
  database
    .prepare("SELECT id AS \"privatevalue\" FROM fixture WHERE label = 'private-value' AND id = 123 -- private-comment")
    .values()

  expect(log.mock.calls).toHaveLength(1)
  expect(log.mock.calls[0]?.[0]).toMatchObject({
    level: "warn",
    module: "performance",
    msg: "SQLite statement lento",
    data: {
      stage: "workspace_snapshot",
      sql: "SELECT id AS ? FROM fixture WHERE label = ? AND id = ?",
      durationMs: 250,
    },
  })
  expect(JSON.stringify(log.mock.calls)).not.toContain("private-")
})

test("the main connection preserves Bun's cached statement identity", () => {
  const log = mock<Kernel["log"]>()
  const database = getDatabase({ log, now: () => 0 })

  const first = database.query("SELECT id FROM fixture")
  const second = database.query("SELECT id FROM fixture")

  expect(second).toBe(first)
})

test("slow statement totals appear in the event-loop lag report", () => {
  let at = 0
  let sample = () => {}
  const log = mock<Kernel["log"]>()
  const stageTracker = createPerformanceStageTracker(() => at)
  const slowStatements = createSlowStatementTracker({ log, getStage: () => "unattributed", now: () => at })
  const diagnostics = createPerformanceDiagnostics({
    log,
    getContext: () => ({ activeRuns: 0, sseClients: 0, websocketClients: 0 }),
    now: () => at,
    stageTracker,
    slowStatements,
    sampleMs: 10,
    lagThresholdMs: 5,
    startSampling: (callback) => {
      sample = callback
      return () => {}
    },
  })
  cleanups.add(diagnostics.stop)

  slowStatements.run("SELECT id FROM fixture WHERE id = 1", () => {
    at += 300
  })
  stageTracker.run("workspace_snapshot", () => {
    at += 1_000
  })
  sample()

  const report = log.mock.calls.map(([entry]) => entry).find((entry) => entry.msg === "Atraso no event loop")

  expect(report?.data?.slowStatements).toEqual([
    {
      stage: "unattributed",
      sql: "SELECT id FROM fixture WHERE id = ?",
      calls: 1,
      totalMs: 300,
      maxMs: 300,
    },
  ])
})

test("the stage tracker exposes the active stage through async nesting", async () => {
  const stageTracker = createPerformanceStageTracker(() => 0)

  stageTracker.run("stream_batch", () => {
    expect(stageTracker.getCurrentStage()).toBe("stream_batch")
    stageTracker.run("workspace_snapshot", () => {
      expect(stageTracker.getCurrentStage()).toBe("workspace_snapshot")
    })
    expect(stageTracker.getCurrentStage()).toBe("stream_batch")
  })

  const stageAfterAwait = await stageTracker.run("workspace_snapshot", async () => {
    await Promise.resolve()
    return stageTracker.getCurrentStage()
  })

  expect(stageAfterAwait).toBe("workspace_snapshot")
  expect(stageTracker.getCurrentStage()).toBeUndefined()
})

const getDatabase = ({ log, now }: { log: Kernel["log"]; now: () => number }): Database => {
  const rawDatabase = new Database(":memory:")
  databases.add(rawDatabase)
  rawDatabase.exec("CREATE TABLE fixture (id INTEGER PRIMARY KEY, label TEXT)")
  const tracker = createSlowStatementTracker({ log, getStage: () => "workspace_snapshot", now })

  return instrumentMainConnection(rawDatabase, tracker)
}
