import { buildApp } from "./app.js";
import { loadConfig } from "./config.js";

const config = loadConfig();
const built = await buildApp({ config });
await built.app.listen({ port: config.port, host: "0.0.0.0" });
built.ctx.log({ msg: "listening", port: config.port });

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => {
    built.close().finally(() => process.exit(0));
  });
}
