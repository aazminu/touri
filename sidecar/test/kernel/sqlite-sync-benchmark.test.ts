import { afterEach, expect, test } from "bun:test"
import { mkdtemp, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { fileURLToPath } from "node:url"
import { openDatabase } from "../../src/kernel/db"
import { streamPersistenceWorkerSource } from "../../src/store/stream-persistence-worker"

const canRunSyncBenchmark = process.platform === "linux" && Bun.which("cc") !== null
const compilerPath = Bun.which("cc")
const cleanups = new Set<() => Promise<void>>()

afterEach(async () => {
  await Array.from(cleanups).reduceRight(async (previous, cleanup) => {
    await previous
    await cleanup()
  }, Promise.resolve())
  cleanups.clear()
})

const syncShim = `#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stdatomic.h>
#include <time.h>

typedef int (*SyncFunction)(int);

static atomic_int recording = 0;
static atomic_int sync_count = 0;
static atomic_int sync_delay_ms = 20;

static int delay_sync(const char *name, int descriptor) {
  SyncFunction original = (SyncFunction)dlsym(RTLD_NEXT, name);
  if (!original) {
    errno = ENOSYS;
    return -1;
  }
  if (atomic_load(&recording)) {
    const int delay_ms = atomic_load(&sync_delay_ms);
    struct timespec delay = {delay_ms / 1000, (delay_ms % 1000) * 1000 * 1000};
    atomic_fetch_add(&sync_count, 1);
    nanosleep(&delay, NULL);
  }
  return original(descriptor);
}

int fsync(int descriptor) { return delay_sync("fsync", descriptor); }
int fdatasync(int descriptor) { return delay_sync("fdatasync", descriptor); }

int touri_start_sync_recording(void) {
  atomic_store(&sync_count, 0);
  atomic_store(&recording, 1);
  return 0;
}

int touri_set_sync_delay_ms(int delay_ms) {
  atomic_store(&sync_delay_ms, delay_ms);
  return 0;
}

int touri_stop_sync_recording(void) {
  atomic_store(&recording, 0);
  return atomic_load(&sync_count);
}
`

const benchmark = `import { Database } from "bun:sqlite";
import { dlopen } from "bun:ffi";
import { join } from "node:path";

const directory = process.env.TOURI_BENCH_DIR;
const shimPath = process.env.TOURI_SYNC_SHIM;
if (!directory || !shimPath) throw new Error("Benchmark paths are missing");

const library = dlopen(shimPath, {
  touri_start_sync_recording: { args: [], returns: "i32" },
  touri_stop_sync_recording: { args: [], returns: "i32" },
});

const run = (mode, synchronous) => {
  const database = new Database(join(directory, mode + ".db"), { create: true });
  database.exec("PRAGMA journal_mode = WAL; PRAGMA wal_autocheckpoint = 0; CREATE TABLE records (value INTEGER NOT NULL)");
  database.exec("PRAGMA synchronous = " + synchronous);
  const insert = database.query("INSERT INTO records(value) VALUES (?)");
  insert.run(-1);
  library.symbols.touri_start_sync_recording();
  const startedAt = performance.now();
  // Keep one synchronous commit per row to isolate the cost paid by stream batches.
  for (let index = 0; index < 32; index++) insert.run(index);
  const elapsedMs = performance.now() - startedAt;
  const syncCalls = library.symbols.touri_stop_sync_recording();
  database.close();
  return { mode, transactions: 32, syncCalls, elapsedMs: Math.round(elapsedMs) };
};

console.log(JSON.stringify({ full: run("full", 2), normal: run("normal", 1) }));
`

test.skipIf(!canRunSyncBenchmark)(
  "WAL NORMAL batches avoid per-commit fsync latency under a slow sync shim",
  async () => {
    const root = await mkdtemp(join(tmpdir(), "touri-sqlite-sync-"))
    cleanups.add(() => rm(root, { recursive: true, force: true }))
    const sourcePath = join(root, "slow-sync.c")
    const shimPath = join(root, "slow-sync.so")
    await writeFile(sourcePath, syncShim)
    if (!compilerPath) throw new Error("A C compiler is required for the sync benchmark")

    const compiler = Bun.spawn({
      cmd: [compilerPath, "-std=c11", "-shared", "-fPIC", "-o", shimPath, sourcePath, "-ldl"],
      stdout: "ignore",
      stderr: "pipe",
    })
    const [compilerExit, compilerError] = await Promise.all([compiler.exited, new Response(compiler.stderr).text()])
    if (compilerExit !== 0) throw new Error(`Could not compile the sync shim: ${compilerError}`)

    const child = Bun.spawn({
      cmd: [process.execPath, "-e", benchmark],
      cwd: root,
      env: {
        HOME: root,
        LD_PRELOAD: shimPath,
        PATH: process.env.PATH ?? "/usr/bin:/bin",
        TMPDIR: root,
        TOURI_BENCH_DIR: root,
        TOURI_SYNC_SHIM: shimPath,
      },
      stdout: "pipe",
      stderr: "pipe",
    })
    const [childExit, output, childError] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ])
    if (childExit !== 0) throw new Error(`The sync benchmark failed: ${childError}`)

    const measured = JSON.parse(output) as {
      full: { transactions: number; syncCalls: number; elapsedMs: number }
      normal: { transactions: number; syncCalls: number; elapsedMs: number }
    }
    console.info(`sqlite-sync-benchmark ${JSON.stringify({ delayMsPerSync: 20, ...measured })}`)

    expect(measured.full.syncCalls).toBeGreaterThanOrEqual(measured.full.transactions)
    expect(measured.normal.syncCalls).toBeLessThanOrEqual(1)
    expect(measured.normal.elapsedMs).toBeLessThan(250)
    expect(measured.normal.elapsedMs).toBeLessThan(measured.full.elapsedMs / 3)
  },
  15_000,
)

const workerBenchmark = `import { Database } from "bun:sqlite";
import { dlopen } from "bun:ffi";

const library = dlopen(process.env.TOURI_SYNC_SHIM, {
  touri_start_sync_recording: { args: [], returns: "i32" },
  touri_stop_sync_recording: { args: [], returns: "i32" },
  touri_set_sync_delay_ms: { args: ["i32"], returns: "i32" },
});
const beforePath = process.env.TOURI_BEFORE_DB;
const afterPath = process.env.TOURI_AFTER_DB;
const authPath = process.env.TOURI_AUTH_DB;
const authModulePath = process.env.TOURI_AUTH_STORE;
const source = process.env.TOURI_STREAM_WORKER;
const event = (index) => ({
  type: "message.delta",
  threadId: "thread",
  messageId: "reply",
  partIndex: 0,
  partType: "text",
  delta: "x",
  offset: index,
  generation: 1,
  updatedAt: index + 1,
});
const events = Array.from({ length: 32 }, (_, index) => event(index));
const measure = async (operation) => {
  let eventLoopLagMs = 0;
  let resolveSample;
  const sampled = new Promise((resolve) => { resolveSample = resolve; });
  const expectedAt = performance.now() + 5;
  setTimeout(() => {
    eventLoopLagMs = Math.max(0, performance.now() - expectedAt);
    resolveSample();
  }, 5);
  const startedAt = performance.now();
  await operation();
  await sampled;
  return { batchLatencyMs: Math.round(performance.now() - startedAt), eventLoopLagMs: Math.round(eventLoopLagMs) };
};
const beforeDatabase = new Database(beforePath);
beforeDatabase.exec("PRAGMA busy_timeout = 0; PRAGMA synchronous = FULL; PRAGMA wal_autocheckpoint = 0");
library.symbols.touri_set_sync_delay_ms(200);
const beforeInsert = beforeDatabase.query(
  "INSERT INTO transcript_entries(thread_id,seq,generation,event_json,at) VALUES ('thread',?,?,?,?)",
);
const before = await measure(() => {
  library.symbols.touri_start_sync_recording();
  beforeDatabase.transaction(() => {
    events.forEach((value, index) => {
      const seq = index + 2;
      const saved = { ...value, updatedSeq: seq };
      beforeDatabase.query("UPDATE threads SET updated_seq = ? WHERE id = 'thread'").run(seq);
      beforeInsert.run(seq, 1, JSON.stringify(saved), saved.updatedAt);
    });
  })();
});
before.syncCalls = library.symbols.touri_stop_sync_recording();
library.symbols.touri_start_sync_recording();
const beforeCheckpoint = await measure(() => beforeDatabase.exec("PRAGMA wal_checkpoint(TRUNCATE)"));
beforeCheckpoint.syncCalls = library.symbols.touri_stop_sync_recording();
beforeDatabase.close();
const { openAuthStore } = await import(authModulePath);
const authStore = openAuthStore({ filename: authPath });
const pairing = authStore.createPairingCode();
library.symbols.touri_start_sync_recording();
const auth = await measure(() => authStore.claimPairingCode({ code: pairing.code, deviceName: "Synthetic device" }));
auth.syncCalls = library.symbols.touri_stop_sync_recording();
authStore.close();

const runWorker = async () => {
  const url = URL.createObjectURL(new Blob([source], { type: "application/javascript" }));
  const worker = new Worker(url, { smol: true, ref: false });
  const result = await new Promise((resolve, reject) => {
    let batchStartedAt = 0;
    let batchExpectedAt = 0;
    let batchEventLoopLagMs = 0;
    let batchLatencyMs = 0;
    let batchSyncCalls = 0;
    let checkpointStartedAt = 0;
    let checkpointExpectedAt = 0;
    let checkpointEventLoopLagMs = 0;
    worker.onmessage = ({ data }) => {
      if (data.type === "ready") {
        library.symbols.touri_start_sync_recording();
        batchStartedAt = performance.now();
        batchExpectedAt = batchStartedAt + 5;
        setTimeout(() => { cancellationObserved = true; }, 10);
        setTimeout(() => {
          batchEventLoopLagMs = Math.max(0, performance.now() - batchExpectedAt);
        }, 5);
        worker.postMessage({
          type: "batch",
          batch: { id: "slow-sync-benchmark", events },
        });
        return;
      }
      if (data.type === "committed") {
        batchLatencyMs = Math.round(performance.now() - batchStartedAt);
        batchSyncCalls = library.symbols.touri_stop_sync_recording();
        library.symbols.touri_start_sync_recording();
        checkpointStartedAt = performance.now();
        checkpointExpectedAt = checkpointStartedAt + 5;
        setTimeout(() => {
          checkpointEventLoopLagMs = Math.max(0, performance.now() - checkpointExpectedAt);
        }, 5);
        worker.postMessage({ type: "checkpoint" });
        return;
      }
      if (data.type === "checkpointed") {
        data.batchLatencyMs = batchLatencyMs;
        data.batchEventLoopLagMs = Math.round(batchEventLoopLagMs);
        data.batchSyncCalls = batchSyncCalls;
        data.checkpointLatencyMs = Math.round(performance.now() - checkpointStartedAt);
        data.checkpointEventLoopLagMs = Math.round(checkpointEventLoopLagMs);
        data.checkpointSyncCalls = library.symbols.touri_stop_sync_recording();
        worker.postMessage({ type: "stop" });
        resolve(data);
        return;
      }
      if (data.type === "startup-error" || data.type === "batch-error" || data.type === "checkpoint-error")
        reject(new Error("Worker persistence failed"));
    };
    worker.onerror = () => reject(new Error("Worker persistence failed"));
    worker.postMessage({ type: "start", filename: afterPath, synchronous: "FULL" });
  });
  worker.terminate();
  URL.revokeObjectURL(url);
  return result;
};
let cancellationObserved = false;
const after = await runWorker();
after.cancellationObserved = cancellationObserved;
const verify = new Database(afterPath, { readonly: true });
after.journalRows = verify.query(
  "SELECT COUNT(*) AS count FROM transcript_entries WHERE json_extract(event_json,'$.type') = 'message.delta'",
).get().count;
after.text = verify.query("SELECT text FROM messages WHERE id = 'reply'").get().text;
verify.close();
console.log(JSON.stringify({ delayMs: 200, before, beforeCheckpoint, auth, after, afterCheckpoint: {
  syncCalls: after.checkpointSyncCalls,
  batchLatencyMs: after.checkpointLatencyMs,
  eventLoopLagMs: after.checkpointEventLoopLagMs,
} }));
`

test.skipIf(!canRunSyncBenchmark)(
  "stream persistence commits off the event loop under a slow sync shim",
  async () => {
    const root = await mkdtemp(join(tmpdir(), "touri-stream-sync-"))
    cleanups.add(() => rm(root, { recursive: true, force: true }))
    const sourcePath = join(root, "slow-sync.c")
    const shimPath = join(root, "slow-sync.so")
    const beforePath = join(root, "before.db")
    const afterPath = join(root, "after.db")
    const authPath = join(root, "auth.db")
    await writeFile(sourcePath, syncShim)
    if (!compilerPath) throw new Error("A C compiler is required for the sync benchmark")
    for (const filename of [beforePath, afterPath]) getStreamBenchmarkDatabase(filename)

    const compiler = Bun.spawn({
      cmd: [compilerPath, "-std=c11", "-shared", "-fPIC", "-o", shimPath, sourcePath, "-ldl"],
      stdout: "ignore",
      stderr: "pipe",
    })
    const [compilerExit, compilerError] = await Promise.all([compiler.exited, new Response(compiler.stderr).text()])
    if (compilerExit !== 0) throw new Error(`Could not compile the sync shim: ${compilerError}`)
    const child = Bun.spawn({
      cmd: [process.execPath, "-e", workerBenchmark],
      cwd: root,
      env: {
        HOME: root,
        LD_PRELOAD: shimPath,
        PATH: process.env.PATH ?? "/usr/bin:/bin",
        TMPDIR: root,
        TOURI_AFTER_DB: afterPath,
        TOURI_AUTH_DB: authPath,
        TOURI_AUTH_STORE: fileURLToPath(new URL("../../src/cli/auth/store.ts", import.meta.url)),
        TOURI_BEFORE_DB: beforePath,
        TOURI_STREAM_WORKER: streamPersistenceWorkerSource,
        TOURI_SYNC_SHIM: shimPath,
      },
      stdout: "pipe",
      stderr: "pipe",
    })
    const [childExit, output, childError] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ])
    if (childExit !== 0) throw new Error(`The stream sync benchmark failed: ${childError}`)
    const measured = JSON.parse(output) as {
      before: { syncCalls: number; batchLatencyMs: number; eventLoopLagMs: number }
      beforeCheckpoint: { syncCalls: number; batchLatencyMs: number; eventLoopLagMs: number }
      auth: { syncCalls: number; batchLatencyMs: number; eventLoopLagMs: number }
      after: {
        batchLatencyMs: number
        batchEventLoopLagMs: number
        batchSyncCalls: number
        checkpointLatencyMs: number
        checkpointEventLoopLagMs: number
        checkpointSyncCalls: number
        cancellationObserved: boolean
        journalRows: number
        text: string
      }
      afterCheckpoint: { syncCalls: number; batchLatencyMs: number; eventLoopLagMs: number }
    }
    console.info(`stream-sync-benchmark ${JSON.stringify({ delayMs: 200, ...measured })}`)

    expect(measured.before.syncCalls).toBeGreaterThanOrEqual(1)
    expect(measured.before.eventLoopLagMs).toBeGreaterThan(100)
    expect(measured.beforeCheckpoint.syncCalls).toBeGreaterThanOrEqual(1)
    expect(measured.beforeCheckpoint.batchLatencyMs).toBeGreaterThan(100)
    expect(measured.beforeCheckpoint.eventLoopLagMs).toBeGreaterThan(100)
    expect(measured.auth.syncCalls).toBeGreaterThanOrEqual(2)
    expect(measured.auth.batchLatencyMs).toBeGreaterThan(100)
    expect(measured.auth.eventLoopLagMs).toBeGreaterThan(100)
    expect(measured.after.batchSyncCalls).toBeGreaterThanOrEqual(1)
    expect(measured.after.batchLatencyMs).toBeGreaterThan(100)
    expect(measured.after.batchEventLoopLagMs).toBeLessThan(50)
    expect(measured.after.checkpointSyncCalls).toBeGreaterThanOrEqual(1)
    expect(measured.after.checkpointLatencyMs).toBeGreaterThan(100)
    expect(measured.after.checkpointEventLoopLagMs).toBeLessThan(50)
    expect(measured.afterCheckpoint.syncCalls).toBeGreaterThanOrEqual(1)
    expect(measured.afterCheckpoint.batchLatencyMs).toBeGreaterThan(100)
    expect(measured.afterCheckpoint.eventLoopLagMs).toBeLessThan(50)
    expect(measured.after.cancellationObserved).toBe(true)
    expect(measured.after.journalRows).toBe(32)
    expect(measured.after.text).toBe("x".repeat(32))
  },
  15_000,
)

const getStreamBenchmarkDatabase = (filename: string): void => {
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
}
