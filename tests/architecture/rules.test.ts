// House rules enforced by reading the source tree. A change that breaks one
// cannot pass CI. Each failure message says how to fix it.
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative, sep } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { PUBLIC_ROUTES } from "../../src/http.js";
import { startApp, type TestApp } from "../helpers.js";

const ROOT = join(import.meta.dirname, "..", "..");
const SKIP = new Set(["node_modules", ".git", ".pgdata", "test-output", "package-lock.json"]);

function walk(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    if (SKIP.has(name)) return [];
    const full = join(dir, name);
    return statSync(full).isDirectory() ? walk(full) : [full];
  });
}
const rel = (f: string) => relative(ROOT, f).split(sep).join("/");
const files = walk(ROOT);
const read = (f: string) => readFileSync(f, "utf8");
const src = files.filter((f) => rel(f).startsWith("src/"));
const client = files.filter((f) => rel(f).startsWith("public/") && f.endsWith(".js"));

/** Lines matching `pattern`, as "file:line" strings. */
function offenders(list: string[], pattern: RegExp, except: (file: string, line: string) => boolean = () => false) {
  return list.flatMap((f) =>
    read(f)
      .split("\n")
      .map((line, i) => ({ line, at: `${rel(f)}:${i + 1}` }))
      .filter(({ line }) => pattern.test(line) && !except(rel(f), line))
      .map(({ at }) => at),
  );
}

let t: TestApp;
beforeAll(async () => {
  t = await startApp();
});
afterAll(() => t.close());

describe("every route is authenticated on purpose", () => {
  it("routes are only registered through projectRoute / adminRoute / publicRoute", () => {
    const bad = offenders(src, /\bapp\.(get|post|put|patch|delete|head|all|route)\s*\(/, (f) => f === "src/http.ts");
    expect(bad, "Register routes with projectRoute/adminRoute/publicRoute from src/http.ts, never app.get/app.route directly.").toEqual([]);
  });

  it("the set of public routes is exactly the reviewed allowlist", () => {
    const publicRoutes = t.built.ctx.routes.filter((r) => r.auth === "public").map((r) => `${r.method} ${r.url}`);
    expect(publicRoutes, "A new unauthenticated route needs a reviewed entry in PUBLIC_ROUTES (src/http.ts). Prefer projectRoute.").toEqual(PUBLIC_ROUTES);
    expect(t.built.ctx.routes.length).toBeGreaterThanOrEqual(9);
  });

  it("request bodies and query strings are only read through parseWith (zod)", () => {
    const bad = offenders(src, /req\.(body|query)\b/, (_f, line) => line.includes("parseWith(") || line.includes("after_seq?: unknown"));
    expect(bad, "Validate input with parseWith(schema, req.body). Raw req.body / req.query is not allowed.").toEqual([]);
  });
});

describe("dangerous patterns", () => {
  it("SQL is never built by string interpolation or concatenation", () => {
    const text = src.map((f) => [rel(f), read(f)] as const);
    const bad = text.filter(([f, body]) => {
      if (f === "src/notify.ts") body = body.replace("`LISTEN ${CHANNEL}`", ""); // constant identifier, cannot be a parameter
      return /query\(\s*`[^`]*\$\{/.test(body) || /query\(\s*["'][^"']*["']\s*\+/.test(body);
    });
    expect(bad.map(([f]) => f), "Use $1-style parameters. Never put values into SQL text.").toEqual([]);
  });

  it("the browser code never turns strings into HTML or code", () => {
    const bad = offenders(client, /innerHTML|outerHTML|insertAdjacentHTML|document\.write|\beval\s*\(|new Function/);
    expect(bad, "Run names and metric keys are attacker-controlled. Use textContent / createElement.").toEqual([]);
  });

  it("the server never logs raw requests", () => {
    const bad = offenders(src, /log\(.*(req\.headers|req\.url|req\.raw|authorization)/i);
    expect(bad, "Log the route pattern and ids only. Headers and URLs can carry API keys.").toEqual([]);
  });
});

describe("secrets", () => {
  it("no secret-looking value or .env file is in the tree", () => {
    const patterns: [string, RegExp][] = [
      ["API key", /sk_[A-Za-z0-9_-]{40,}/],
      ["postgres URL with a real password", /postgres(ql)?:\/\/(?!soup:soup@|admin:hunter2@|user:pass@)[^\s:@/]+:[^\s@/]+@/],
      ["private key", /-----BEGIN [A-Z ]*PRIVATE KEY-----/],
      ["Google key", /AIza[0-9A-Za-z_-]{35}/],
      ["Neon password", /npg_[A-Za-z0-9]{16,}/],
    ];
    const bad: string[] = [];
    for (const f of files) {
      const name = rel(f);
      if (/(^|\/)\.env(\..*)?$/.test(name) && !name.endsWith(".env.example")) bad.push(`${name}: .env file must not be committed`);
      if (/\.(png|jpg|ico)$/.test(name) || name === "tests/architecture/rules.test.ts") continue;
      const body = read(f);
      for (const [label, re] of patterns) if (re.test(body)) bad.push(`${name}: looks like a ${label}`);
    }
    expect(bad, "Move the secret to an environment variable and rotate it.").toEqual([]);
  });
});
