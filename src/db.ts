import { readFile } from "node:fs/promises";
import pg from "pg";

// bigint columns (seq, step) are bounded to the JS safe-integer range by
// validation, so plain numbers are exact.
pg.types.setTypeParser(20, Number);

export type Pool = pg.Pool;
export type Tx = pg.PoolClient;

export function createPool(databaseUrl: string): Pool {
  const pool = new pg.Pool({ connectionString: databaseUrl, max: 20 });
  // An idle connection dying must not crash the process.
  pool.on("error", () => {});
  return pool;
}

export async function applySchema(pool: Pool): Promise<void> {
  const sql = await readFile(new URL("../db/schema.sql", import.meta.url), "utf8");
  const client = await pool.connect();
  try {
    // Several app instances may boot at once; serialize the DDL.
    await client.query("SELECT pg_advisory_lock(727001)");
    await client.query(sql);
  } finally {
    await client.query("SELECT pg_advisory_unlock(727001)").catch(() => {});
    client.release();
  }
}

/** All-or-nothing: any throw rolls the whole transaction back. */
export async function withTx<T>(pool: Pool, fn: (tx: Tx) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await fn(client);
    await client.query("COMMIT");
    return result;
  } catch (err) {
    await client.query("ROLLBACK").catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}
