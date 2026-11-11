export interface Config {
  port: number;
  databaseUrl: string;
  adminToken: string;
  /** Max events in one ingest batch. */
  maxBatch: number;
  /** Max request body in bytes. */
  bodyLimit: number;
  /** Max points per SSE frame = max rows held in memory per stream. */
  streamPage: number;
  /** Fallback re-read interval when no NOTIFY arrives; also heartbeat + key re-check. */
  streamPollMs: number;
  /** A client that accepts no bytes for this long is disconnected. */
  streamStallMs: number;
  maxStreamsPerProject: number;
}

export function loadConfig(overrides: Partial<Config> = {}): Config {
  const env = process.env;
  const databaseUrl = overrides.databaseUrl ?? env.DATABASE_URL;
  if (!databaseUrl) throw new Error("DATABASE_URL is required");
  const adminToken = overrides.adminToken ?? env.ADMIN_TOKEN;
  if (!adminToken) throw new Error("ADMIN_TOKEN is required");
  return {
    port: Number(env.PORT ?? 8080),
    maxBatch: 1000,
    bodyLimit: 1024 * 1024,
    streamPage: 500,
    streamPollMs: 3000,
    streamStallMs: 15000,
    maxStreamsPerProject: 200,
    ...overrides,
    databaseUrl,
    adminToken,
  };
}
