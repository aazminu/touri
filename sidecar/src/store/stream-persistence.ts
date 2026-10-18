import type { Database } from "bun:sqlite"
import type { UiEvent } from "@touri/contracts"
import { streamPersistenceWorkerSource } from "./stream-persistence-worker"

type StreamEvent = Extract<UiEvent, { type: "message.delta" | "message.parts" }>
export type RejectionWrite = {
  type: "stream.rejection"
  threadId: string
  messageId: string
  partIndex: number
  generation?: number
  expectedSeq?: number
  reason: "validation" | "oversize" | "stale_generation" | "seq_conflict"
}
type StreamWriteEvent = StreamEvent | RejectionWrite
type CommittedBatch = {
  type: "committed"
  id: string
  events: StreamEvent[]
  rejected: Array<Extract<UiEvent, { type: "transcript.rejected" }>>
  published: UiEvent[]
}
type WorkerMessage =
  | CommittedBatch
  | { type: "ready" | "stopped" }
  | { type: "startup-error"; busy: boolean }
  | { type: "batch-error"; id: string; busy: boolean }
type Batch = { id: string; events: StreamWriteEvent[] }
type PendingBatch = {
  batch: Batch
  resolve: (result: CommittedBatch) => void
  reject: (error: Error) => void
}

const RETRY_DELAY_MS = 100
const STARTUP_TIMEOUT_MS = 15_000

export const createStreamPersistence = ({
  database,
  publish,
  onError,
  createWorker = (url) => new Worker(url, { smol: true, ref: false }),
}: {
  database: Database
  publish: (event: UiEvent) => void
  onError: () => void
  createWorker?: (url: string) => Worker
}) => {
  const queue: PendingBatch[] = []
  const workerUrl = URL.createObjectURL(new Blob([streamPersistenceWorkerSource], { type: "application/javascript" }))
  let worker: Worker | undefined
  let startupTimer: ReturnType<typeof setTimeout> | undefined
  let retryTimer: ReturnType<typeof setTimeout> | undefined
  let stopTimer: ReturnType<typeof setTimeout> | undefined
  let workerNumber = 0
  let ready = false
  let running = false
  let stopping = false
  let disposed = false
  let reportedError = false
  let resolveReady: () => void = () => {}
  const readyPromise = new Promise<void>((resolve) => {
    resolveReady = resolve
  })

  const reportError = (): void => {
    if (reportedError) return
    reportedError = true
    try {
      onError()
    } catch {
      // Diagnostics must not interrupt retrying durable stream writes.
    }
  }

  const scheduleRetry = (): void => {
    if (disposed || retryTimer) return
    retryTimer = setTimeout(() => {
      retryTimer = undefined
      startWorker()
    }, RETRY_DELAY_MS)
    retryTimer.unref()
  }

  const finishWorker = (number: number): void => {
    if (number !== workerNumber) return
    clearTimeout(startupTimer)
    worker?.terminate()
    worker = undefined
    ready = false
    running = false
    if (disposed) return
    scheduleRetry()
  }

  const commitHead = (): void => {
    if (!ready || running || !queue.length) return
    const head = queue[0]
    if (!head || !worker) return
    running = true
    try {
      worker.postMessage({ type: "batch", batch: head.batch })
    } catch {
      finishWorker(workerNumber)
    }
  }

  const startWorker = (): void => {
    if (disposed || worker) return
    try {
      workerNumber += 1
      const number = workerNumber
      worker = createWorker(workerUrl)
      worker.onmessage = ({ data }: MessageEvent<WorkerMessage>): void => {
        if (number !== workerNumber || disposed) return
        if (data?.type === "ready") {
          clearTimeout(startupTimer)
          ready = true
          resolveReady()
          commitHead()
          return
        }
        if (data?.type === "startup-error") {
          reportError()
          resolveReady()
          finishWorker(number)
          return
        }
        if (data?.type === "batch-error") {
          if (!data.busy) reportError()
          running = false
          if (data.busy) {
            retryTimer = setTimeout(() => {
              retryTimer = undefined
              commitHead()
            }, RETRY_DELAY_MS)
            retryTimer.unref()
            return
          }
          finishWorker(number)
          return
        }
        if (data?.type !== "committed") return
        const head = queue[0]
        if (!head || head.batch.id !== data.id) {
          reportError()
          finishWorker(number)
          return
        }
        queue.shift()
        running = false
        reportedError = false
        data.published.forEach(publish)
        head.resolve(data)
        commitHead()
      }
      worker.onerror = (event): void => {
        event.preventDefault()
        reportError()
        resolveReady()
        finishWorker(number)
      }
      worker.addEventListener("close", () => {
        if (number !== workerNumber || disposed) return
        reportError()
        finishWorker(number)
      })
      startupTimer = setTimeout(() => {
        reportError()
        resolveReady()
        finishWorker(number)
      }, STARTUP_TIMEOUT_MS)
      startupTimer.unref()
      worker.postMessage({ type: "start", filename: database.filename })
    } catch {
      reportError()
      resolveReady()
      finishWorker(workerNumber)
    }
  }

  const submit = (events: StreamWriteEvent[]): Promise<CommittedBatch> => {
    if (disposed || stopping) return Promise.reject(new Error("Stream persistence is closed"))
    const batch = { id: crypto.randomUUID(), events: structuredClone(events) }
    const promise = new Promise<CommittedBatch>((resolve, reject) => queue.push({ batch, resolve, reject }))
    commitHead()
    return promise
  }

  const drain = async ({ timeoutMs = 5_000 }: { timeoutMs?: number } = {}): Promise<boolean> => {
    if (!queue.length) return true
    const startedAt = Date.now()
    while (queue.length && Date.now() - startedAt < timeoutMs) await new Promise((resolve) => setTimeout(resolve, 20))
    return queue.length === 0
  }

  const close = async ({ closing = false }: { closing?: boolean } = {}): Promise<void> => {
    if (disposed) return
    stopping = true
    clearTimeout(retryTimer)
    clearTimeout(startupTimer)
    const drained = await drain({ timeoutMs: 1_000 })
    if (!drained) reportError()
    disposed = true
    clearTimeout(retryTimer)
    clearTimeout(startupTimer)
    clearTimeout(stopTimer)
    if (!drained) {
      const abandoned = queue.splice(0)
      abandoned.forEach(({ reject }) => {
        reject(new Error("Stream persistence stopped before commit"))
      })
    }
    if (worker) {
      const current = worker
      const number = workerNumber
      if (drained) {
        await new Promise<void>((resolve) => {
          stopTimer = setTimeout(resolve, 1_000)
          stopTimer.unref()
          current.addEventListener("message", ({ data }: MessageEvent<{ type?: string }>) => {
            if (data?.type !== "stopped") return
            clearTimeout(stopTimer)
            resolve()
          })
          try {
            current.postMessage({ type: "stop", closing })
          } catch {
            resolve()
          }
        })
      }
      if (number === workerNumber) current.terminate()
      worker = undefined
    }
    URL.revokeObjectURL(workerUrl)
  }

  const closeIfIdle = (): boolean => {
    if (disposed) return true
    if (queue.length || running) return false
    stopping = true
    disposed = true
    clearTimeout(retryTimer)
    clearTimeout(startupTimer)
    clearTimeout(stopTimer)
    resolveReady()
    worker?.terminate()
    worker = undefined
    ready = false
    running = false
    URL.revokeObjectURL(workerUrl)
    return true
  }

  startWorker()

  return { submit, drain, close, closeIfIdle, ready: readyPromise }
}
