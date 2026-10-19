import type { Database } from "bun:sqlite"

const isBusy = (error: unknown): boolean =>
  error instanceof Error &&
  "code" in error &&
  typeof error.code === "string" &&
  (error.code.startsWith("SQLITE_BUSY") || error.code.startsWith("SQLITE_LOCKED"))

/** Acquire the writer slot before reading, and yield between bounded lock attempts. */
export const writeDatabaseAsync = async <Value>(database: Database, write: () => Value): Promise<Value> => {
  const deadline = performance.now() + 5_000
  const transaction = database.transaction(write)
  for (;;) {
    try {
      return transaction.immediate()
    } catch (error) {
      if (!isBusy(error) || performance.now() >= deadline) throw error
      await new Promise<void>((resolve) => setTimeout(resolve, 10))
    }
  }
}
