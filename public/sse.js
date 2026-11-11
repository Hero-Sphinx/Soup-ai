// SSE over fetch() instead of EventSource, for one reason: EventSource cannot
// send an Authorization header, and putting the API key in the URL would leak
// it into logs and browser history. Works in browsers and in Node 22.
//
// Reconnect rule: the resume cursor is always read from the caller's applied
// state (getCursor), never from "the last id seen on the wire". A frame is
// only dispatched once complete, so a connection cut mid-frame loses nothing:
// the half frame is dropped and asked for again.

export function openStream({
  url,
  apiKey,
  getCursor,
  onPoints, // (points) => boolean; false = gap, reconnect from cursor
  onSync, // (seq) => boolean; false = we disagree with the server, reconnect
  onReset = () => {},
  onStatus = (_status) => {},
  idleTimeoutMs = 10000, // server heartbeats every ~3s; silence means a dead link
  retryMinMs = 250,
  retryMaxMs = 5000,
}) {
  let stopped = false;
  let ctrl = null;
  let failures = 0;
  let wakeRetry = null;

  const dispatch = (frame) => {
    let event = "message";
    let data = "";
    for (const line of frame.split("\n")) {
      if (line.startsWith("event:")) event = line.slice(6).trim();
      else if (line.startsWith("data:")) data += line.slice(5).trim();
    }
    // Any complete frame is progress: the next retry starts from the short
    // delay again. (Resetting only on "sync" made the delay grow on a link
    // that kept dropping while it was still delivering data.)
    if (event === "points" || event === "sync") failures = 0;
    if (event === "points") return onPoints(JSON.parse(data)) ? "ok" : "resync";
    if (event === "sync") return onSync(JSON.parse(data).seq) ? "ok" : "resync";
    if (event === "reset") onReset();
    if (event === "revoked") return "revoked";
    return "ok";
  };

  const run = async () => {
    while (!stopped) {
      ctrl = new AbortController();
      const signal = ctrl.signal;
      let idle = null;
      const arm = () => {
        clearTimeout(idle);
        idle = setTimeout(() => ctrl.abort(), idleTimeoutMs);
      };
      try {
        onStatus(failures === 0 ? "connecting" : "reconnecting");
        arm();
        const res = await fetch(url, {
          headers: {
            authorization: `Bearer ${apiKey}`,
            accept: "text/event-stream",
            "last-event-id": String(getCursor()),
          },
          cache: "no-store",
          signal,
        });
        if (res.status === 401 || res.status === 404) {
          stopped = true;
          onStatus(res.status === 401 ? "unauthorized" : "not-found");
          break;
        }
        if (!res.ok || !res.body) throw new Error(`http ${res.status}`);
        onStatus("open");
        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buf = "";
        let verdict = "ok";
        while (verdict === "ok") {
          const { done, value } = await reader.read();
          if (done) break;
          arm();
          buf += decoder.decode(value, { stream: true });
          let end;
          while (verdict === "ok" && (end = buf.indexOf("\n\n")) >= 0) {
            const frame = buf.slice(0, end);
            buf = buf.slice(end + 2);
            verdict = dispatch(frame);
          }
        }
        if (verdict === "revoked") {
          stopped = true;
          onStatus("revoked");
        }
      } catch {
        // network error, abort, idle timeout: all handled by reconnecting
      } finally {
        clearTimeout(idle);
        ctrl.abort();
      }
      if (stopped) break;
      failures++;
      onStatus("reconnecting");
      const backoff = Math.min(retryMaxMs, retryMinMs * 2 ** Math.min(failures - 1, 5));
      await new Promise((resolve) => {
        const t = setTimeout(resolve, backoff * (0.5 + Math.random() / 2));
        wakeRetry = () => {
          clearTimeout(t);
          resolve();
        };
      });
      wakeRetry = null;
    }
  };

  const finished = run();

  return {
    finished,
    close() {
      stopped = true;
      ctrl?.abort();
      wakeRetry?.();
      onStatus("closed");
    },
  };
}
