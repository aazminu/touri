import type { Database, Statement } from "bun:sqlite"
import type { Kernel } from "@touri/contracts"

export type SlowStatement = {
  stage: string
  sql: string
  calls: number
  totalMs: number
  maxMs: number
}

export type SlowStatementTracker = {
  run: <Value>(sql: string, action: () => Value) => Value
  now: () => number
  recordDuration: (sql: string, durationMs: number) => void
  snapshot: () => Array<SlowStatement>
  reset: () => void
}

const DEFAULT_THRESHOLD_MS = 250
const MAX_NORMALIZED_SQL_LENGTH = 512

export const createSlowStatementTracker = ({
  log,
  getStage,
  now = performance.now.bind(performance),
  thresholdMs = DEFAULT_THRESHOLD_MS,
}: {
  log: Kernel["log"]
  getStage: () => string
  now?: () => number
  thresholdMs?: number
}): SlowStatementTracker => {
  let timings: Array<SlowStatement> = []

  const recordDuration = (sql: string, durationMs: number): void => {
    if (durationMs < thresholdMs) return

    const statement = normalizeSql(sql)
    const stage = getStage()
    const timing = { stage, sql: statement, durationMs }
    logSlowStatement({ log, timing })
    timings = recordSlowStatement(timings, timing)
  }

  const run = <Value>(sql: string, action: () => Value): Value => {
    const startedAt = now()
    try {
      return action()
    } finally {
      recordDuration(sql, now() - startedAt)
    }
  }

  return {
    run,
    now,
    recordDuration,
    snapshot: () => timings.map((timing) => ({ ...timing })),
    reset: () => {
      timings = []
    },
  }
}

export const instrumentMainConnection = <Connection extends Database>(
  database: Connection,
  tracker: SlowStatementTracker,
): Connection => {
  // Bun caches prepared statements by SQL; keep each wrapped statement's identity.
  const statementProxies = new WeakMap<Statement, Statement>()
  const connection = Object.create(Object.getPrototypeOf(database)) as Connection

  getDatabaseMembers(database).forEach(({ property, descriptor }) => {
    const value = Reflect.get(database, property, database) as unknown
    if (typeof value === "function") {
      Object.defineProperty(connection, property, {
        configurable: true,
        enumerable: descriptor.enumerable ?? false,
        writable: true,
        value: instrumentDatabaseMethod({ database, property, value, tracker, statementProxies }),
      })
      return
    }

    Object.defineProperty(connection, property, {
      configurable: true,
      enumerable: descriptor.enumerable ?? false,
      get: () => Reflect.get(database, property, database) as unknown,
      set: (next) => Reflect.set(database, property, next, database),
    })
  })

  return connection
}

const getDatabaseMembers = (database: Database): Array<{ property: PropertyKey; descriptor: PropertyDescriptor }> => {
  const members = new Map<PropertyKey, PropertyDescriptor>()
  const ownMembers = Reflect.ownKeys(database).flatMap((property) => {
    const descriptor = Object.getOwnPropertyDescriptor(database, property)
    return descriptor ? [[property, descriptor] as const] : []
  })
  ownMembers.forEach(([property, descriptor]) => {
    members.set(property, descriptor)
  })

  let prototype = Object.getPrototypeOf(database) as object | null
  while (prototype && prototype !== Object.prototype) {
    Reflect.ownKeys(prototype).forEach((property) => {
      if (property === "constructor" || members.has(property)) return
      const descriptor = Object.getOwnPropertyDescriptor(prototype, property)
      if (descriptor) members.set(property, descriptor)
    })
    prototype = Object.getPrototypeOf(prototype) as object | null
  }

  return Array.from(members, ([property, descriptor]) => ({ property, descriptor }))
}

const instrumentDatabaseMethod = ({
  database,
  property,
  value,
  tracker,
  statementProxies,
}: {
  database: Database
  property: PropertyKey
  value: unknown
  tracker: SlowStatementTracker
  statementProxies: WeakMap<Statement, Statement>
}): unknown => {
  const method = value as (...args: Array<unknown>) => unknown
  if (property === "query" || property === "prepare")
    return (sql: string, ...args: Array<unknown>) =>
      tracker.run(sql, () => {
        const statement = Reflect.apply(method, database, [sql, ...args]) as Statement
        return instrumentStatement(statement, sql, tracker, statementProxies)
      })
  if (property === "run" || property === "exec")
    return (sql: string, ...args: Array<unknown>) =>
      tracker.run(sql, () => Reflect.apply(method, database, [sql, ...args]))
  if (property === "transaction")
    return (action: (...args: Array<unknown>) => unknown) => {
      const transaction = Reflect.apply(method, database, [action])
      return instrumentTransaction(transaction, tracker)
    }

  return method.bind(database)
}

const instrumentStatement = (
  statement: Statement,
  sql: string,
  tracker: SlowStatementTracker,
  statementProxies: WeakMap<Statement, Statement>,
): Statement => {
  const existing = statementProxies.get(statement)
  if (existing) return existing

  const proxy = new Proxy(statement, {
    get: (target, property) => {
      const value = Reflect.get(target, property, target) as unknown
      if (["run", "get", "all", "values", "raw"].includes(String(property)))
        return (...args: Array<unknown>) =>
          tracker.run(sql, () => Reflect.apply(value as (...values: Array<unknown>) => unknown, target, args))
      if (property === "iterate" || property === Symbol.iterator)
        return (...args: Array<unknown>) =>
          instrumentIterator(
            Reflect.apply(value as (...values: Array<unknown>) => IterableIterator<unknown>, target, args),
            sql,
            tracker,
          )
      if (property === "as")
        return (...args: Array<unknown>) =>
          instrumentStatement(
            Reflect.apply(value as (...values: Array<unknown>) => Statement, target, args),
            sql,
            tracker,
            statementProxies,
          )
      if (typeof value === "function") return value.bind(target)

      return value
    },
  })

  statementProxies.set(statement, proxy)
  return proxy
}

const instrumentTransaction = (transaction: unknown, tracker: SlowStatementTracker): unknown => {
  if (typeof transaction !== "function") return transaction

  return new Proxy(transaction, {
    apply: (target, thisArgument, args) => tracker.run("TRANSACTION", () => Reflect.apply(target, thisArgument, args)),
    get: (target, property, receiver) => {
      const value = Reflect.get(target, property, receiver) as unknown
      if (typeof value !== "function") return value

      return new Proxy(value, {
        apply: (mode, _thisArgument, args) => tracker.run("TRANSACTION", () => Reflect.apply(mode, transaction, args)),
      })
    },
  })
}

const instrumentIterator = (
  iterator: IterableIterator<unknown>,
  sql: string,
  tracker: SlowStatementTracker,
): IterableIterator<unknown> => {
  let startedAt: number | undefined
  let finished = false
  let totalMs = 0

  const finish = (): void => {
    if (finished || startedAt === undefined) return
    finished = true
    tracker.recordDuration(sql, totalMs)
  }

  const next = (...args: Array<unknown>): IteratorResult<unknown> => {
    startedAt ??= tracker.now()
    const callStartedAt = tracker.now()
    try {
      const result = Reflect.apply(iterator.next, iterator, args) as IteratorResult<unknown>
      totalMs += tracker.now() - callStartedAt
      if (result.done) finish()
      return result
    } catch (error) {
      totalMs += tracker.now() - callStartedAt
      finish()
      throw error
    }
  }

  return {
    next,
    return: (value?: unknown) => {
      finish()
      return iterator.return ? Reflect.apply(iterator.return, iterator, [value]) : { done: true, value }
    },
    [Symbol.iterator]() {
      return this
    },
  }
}

const logSlowStatement = ({
  log,
  timing,
}: {
  log: Kernel["log"]
  timing: { stage: string; sql: string; durationMs: number }
}): void => {
  try {
    log({
      level: "warn",
      module: "performance",
      msg: "SQLite statement lento",
      data: { stage: timing.stage, sql: timing.sql, durationMs: getMilliseconds(timing.durationMs) },
    })
  } catch {
    // Diagnostics must not interrupt a database operation when logging is unavailable.
  }
}

const recordSlowStatement = (
  timings: Array<SlowStatement>,
  timing: { stage: string; sql: string; durationMs: number },
): Array<SlowStatement> => {
  const existing = timings.find((item) => item.stage === timing.stage && item.sql === timing.sql)
  if (existing)
    return timings.map((item) =>
      item === existing
        ? {
            ...item,
            calls: item.calls + 1,
            totalMs: getMilliseconds(item.totalMs + timing.durationMs),
            maxMs: Math.max(item.maxMs, getMilliseconds(timing.durationMs)),
          }
        : item,
    )

  return [
    ...timings,
    {
      stage: timing.stage,
      sql: timing.sql,
      calls: 1,
      totalMs: getMilliseconds(timing.durationMs),
      maxMs: getMilliseconds(timing.durationMs),
    },
  ].slice(-32)
}

const normalizeSql = (sql: string): string => {
  const tokens: Array<string> = []
  let index = 0
  while (index < sql.length) {
    const character = sql[index]
    const next = sql[index + 1]
    if (character === "-" && next === "-") {
      index = skipLineComment(sql, index + 2)
      tokens.push(" ")
      continue
    }
    if (character === "/" && next === "*") {
      index = skipBlockComment(sql, index + 2)
      tokens.push(" ")
      continue
    }
    if (character === "'") {
      index = skipQuoted(sql, index + 1, "'")
      tokens.push("?")
      continue
    }
    if (character === '"' || character === "`" || character === "[") {
      index = skipQuoted(sql, index + 1, character === "[" ? "]" : character)
      tokens.push("?")
      continue
    }
    tokens.push(character ?? "")
    index += 1
  }

  return tokens
    .join("")
    .replace(/\b(?:0x[\da-f]+|\d+(?:\.\d+)?(?:e[+-]?\d+)?)\b/gi, "?")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, MAX_NORMALIZED_SQL_LENGTH)
}

const skipLineComment = (sql: string, start: number): number => {
  const end = sql.indexOf("\n", start)

  return end < 0 ? sql.length : end + 1
}

const skipBlockComment = (sql: string, start: number): number => {
  const end = sql.indexOf("*/", start)

  return end < 0 ? sql.length : end + 2
}

const skipQuoted = (sql: string, start: number, closing: string): number => readQuoted(sql, start, closing).nextIndex

const readQuoted = (sql: string, start: number, closing: string): { nextIndex: number } => {
  let index = start
  while (index < sql.length) {
    const character = sql[index]
    if (character !== closing) {
      index += 1
      continue
    }
    if (sql[index + 1] === closing) {
      index += 2
      continue
    }

    return { nextIndex: index + 1 }
  }

  return { nextIndex: sql.length }
}

const getMilliseconds = (value: number): number => Math.round(value * 1_000) / 1_000
