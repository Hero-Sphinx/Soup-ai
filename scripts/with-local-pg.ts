// Dev convenience for machines without Docker: boots a throwaway real
// Postgres (embedded-postgres downloads the official binaries), sets
// DATABASE_URL / ADMIN_TOKEN, runs the given command, then stops Postgres.
//
//   npm run test:local        npm run dev:local        npm run loadtest:local
import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import EmbeddedPostgres from "embedded-postgres";

const dir = ".pgdata";
const port = Number(process.env.LOCAL_PG_PORT ?? 54329);
const pg = new EmbeddedPostgres({
  databaseDir: dir,
  user: "soup",
  password: "soup",
  port,
  persistent: true,
  onLog: () => {},
  onError: () => {},
});

if (!existsSync(`${dir}/PG_VERSION`)) await pg.initialise();
await pg.start();
try {
  await pg.createDatabase("soup");
} catch {
  // already exists
}

const [cmd, ...args] = process.argv.slice(2);
if (!cmd) {
  console.error("usage: with-local-pg <command> [args...]");
  await pg.stop();
  process.exit(2);
}

const child = spawn([cmd, ...args].join(" "), {
  stdio: "inherit",
  shell: true,
  env: {
    ADMIN_TOKEN: "dev-admin-token",
    PORT: "8090", // not 8080: other desktop software often sits on it
    ...process.env,
    DATABASE_URL: `postgres://soup:soup@127.0.0.1:${port}/soup`,
  },
});
const stop = async (code: number) => {
  await pg.stop().catch(() => {});
  process.exit(code);
};
child.on("exit", (code) => void stop(code ?? 1));
process.on("SIGINT", () => child.kill("SIGINT"));
