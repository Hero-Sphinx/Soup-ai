// The page's model of a run. Pure and framework-free so the exact same code
// runs in the browser and in the Node tests.
//
// A point's identity is (key, step). Applying points is an upsert into a map,
// so it is idempotent and order-independent: late or out-of-order points land
// in the right place, and a replayed point changes nothing.

export function createState() {
  return {
    series: new Map(), // key -> Map(step -> value)
    lastSeq: 0, // highest seq applied; the resume cursor
    count: 0,
    gaps: 0, // tripwire: times the stream skipped a seq
    replays: 0, // points delivered again at or below lastSeq (ignored)
  };
}

export function resetState(state) {
  state.series = new Map();
  state.lastSeq = 0;
  state.count = 0;
}

// Returns false when a seq was skipped. The caller must then reconnect from
// state.lastSeq; nothing after the hole has been applied.
export function applyPoints(state, points) {
  for (const p of points) {
    if (p.seq <= state.lastSeq) {
      state.replays++;
      continue;
    }
    if (p.seq !== state.lastSeq + 1) {
      state.gaps++;
      return false;
    }
    let series = state.series.get(p.key);
    if (!series) state.series.set(p.key, (series = new Map()));
    if (!series.has(p.step)) state.count++;
    series.set(p.step, p.value);
    state.lastSeq = p.seq;
  }
  return true;
}

export function toPoints(state) {
  const out = [];
  for (const [key, series] of state.series) {
    for (const [step, value] of series) out.push({ key, step, value });
  }
  return sortPoints(out);
}

export function sortPoints(points) {
  return points.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : a.step - b.step));
}

// Short fingerprint of a set of points (FNV-1a over the sorted set), shown on
// the page so a human or a test can compare it with one computed from the DB.
export function digestPoints(points) {
  let h = 0x811c9dc5;
  for (const p of sortPoints([...points])) {
    const line = `${p.key}|${p.step}|${p.value}\n`;
    for (let i = 0; i < line.length; i++) {
      h ^= line.charCodeAt(i);
      h = Math.imul(h, 0x01000193) >>> 0;
    }
  }
  return h.toString(16).padStart(8, "0");
}
