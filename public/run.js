import { openStream } from "/sse.js";
import { applyPoints, createState, digestPoints, resetState, toPoints } from "/state.js";

const $ = (id) => document.getElementById(id);
const SVG = "http://www.w3.org/2000/svg";
const MAX_DRAWN = 1500; // buckets on the x axis; the line is thinned beyond this, the numbers are not

const apiKey = sessionStorage.getItem("soup_key");
const runId = new URLSearchParams(location.search).get("id") ?? "";
const state = createState();
let link = "connecting"; // transport status from sse.js
let syncedSeq = -1; // last seq the server confirmed we are complete up to

// For the end-to-end test: the page's real model, not a copy.
window.__soup = { state, points: () => toPoints(state) };

const LABELS = {
  connecting: "Connecting…",
  catching: "Catching up…",
  live: "Live",
  reconnecting: "Reconnecting…",
  unauthorized: "Signed out",
  revoked: "Key rotated",
  "not-found": "Not found",
};

function viewStatus() {
  if (link === "open") return syncedSeq === state.lastSeq ? "live" : "catching";
  return link;
}

function setStatus() {
  const s = viewStatus();
  const pill = $("status");
  pill.dataset.status = s;
  pill.textContent = LABELS[s] ?? s;
  const detail = {
    reconnecting: `Connection lost. Showing data up to event #${state.lastSeq}; missed points load when the connection is back.`,
    revoked: "This API key was rotated. Open the project again with the new key.",
    unauthorized: "This API key is no longer valid. Open the project again.",
    "not-found": "This run doesn't exist in your project.",
  }[s];
  $("status-detail").textContent = detail ?? "";
}

function drawSeries(card, key, series) {
  const steps = [...series.keys()].sort((a, b) => a - b);
  const last = steps[steps.length - 1];
  card.querySelector(".value").textContent = String(series.get(last));
  card.querySelector(".meta").textContent = `step ${last} · ${steps.length.toLocaleString()} points`;

  let lo = Infinity;
  let hi = -Infinity;
  for (const v of series.values()) {
    if (v < lo) lo = v;
    if (v > hi) hi = v;
  }
  const x0 = steps[0];
  const xSpan = last - x0 || 1;
  const ySpan = hi - lo || 1;
  // Long series are thinned for drawing, but never by plain sampling: each
  // bucket keeps its lowest and highest point, so a spike cannot disappear.
  const bucket = Math.ceil(steps.length / MAX_DRAWN);
  const coords = [];
  const plot = (step) => {
    const x = ((step - x0) / xSpan) * 300;
    const y = 76 - ((series.get(step) - lo) / ySpan) * 72;
    coords.push(`${x.toFixed(1)},${y.toFixed(1)}`);
  };
  for (let i = 0; i < steps.length; i += bucket) {
    if (bucket === 1) {
      plot(steps[i]);
      continue;
    }
    let min = steps[i];
    let max = steps[i];
    for (let j = i; j < Math.min(i + bucket, steps.length); j++) {
      if (series.get(steps[j]) < series.get(min)) min = steps[j];
      if (series.get(steps[j]) > series.get(max)) max = steps[j];
    }
    plot(Math.min(min, max));
    if (min !== max) plot(Math.max(min, max));
  }
  card.querySelector("polyline").setAttribute("points", coords.join(" "));
}

function cardFor(key) {
  const charts = $("charts");
  for (const c of charts.children) if (c.dataset.key === key) return c;
  const card = document.createElement("article");
  card.className = "card chart";
  card.dataset.key = key;
  const h = document.createElement("h2");
  h.textContent = key;
  const value = document.createElement("p");
  value.className = "value";
  const meta = document.createElement("p");
  meta.className = "meta muted";
  const svg = document.createElementNS(SVG, "svg");
  svg.setAttribute("viewBox", "0 0 300 80");
  svg.setAttribute("preserveAspectRatio", "none");
  svg.setAttribute("role", "img");
  svg.setAttribute("aria-label", `${key} over steps`);
  svg.append(document.createElementNS(SVG, "polyline"));
  card.append(h, value, meta, svg);
  // keep cards in a stable alphabetical order
  const after = [...charts.children].find((c) => c.dataset.key > key);
  charts.insertBefore(card, after ?? null);
  return card;
}

let scheduled = false;
const dirtyKeys = new Set();

function render() {
  scheduled = false;
  for (const key of dirtyKeys) drawSeries(cardFor(key), key, state.series.get(key));
  dirtyKeys.clear();
  // "No metrics yet" is only true for a run we can actually see.
  $("empty").hidden = state.count > 0 || ["not-found", "unauthorized", "revoked"].includes(viewStatus());
  $("count").textContent = String(state.count);
  $("last-seq").textContent = String(state.lastSeq);
  $("gaps").textContent = String(state.gaps);
  // The fingerprint is only meaningful (and only computed) when complete.
  $("digest").textContent = viewStatus() === "live" ? digestPoints(toPoints(state)) : "…";
  setStatus();
}

function schedule() {
  if (scheduled) return;
  scheduled = true;
  setTimeout(render, 150);
}

async function start() {
  if (!apiKey) {
    location.replace("/");
    return;
  }
  fetch(`/runs/${encodeURIComponent(runId)}`, { headers: { authorization: `Bearer ${apiKey}` }, cache: "no-store" })
    .then((res) => (res.ok ? res.json() : null))
    .then((run) => {
      if (run) {
        $("run-name").textContent = run.name;
        document.title = `${run.name} · Soup tracker`;
      }
    })
    .catch(() => {});

  openStream({
    url: `/runs/${encodeURIComponent(runId)}/stream`,
    apiKey,
    getCursor: () => state.lastSeq,
    onPoints(points) {
      const ok = applyPoints(state, points);
      for (const p of points) dirtyKeys.add(p.key);
      schedule();
      return ok;
    },
    onSync(seq) {
      if (seq !== state.lastSeq) return false;
      syncedSeq = seq;
      schedule();
      return true;
    },
    onReset() {
      resetState(state);
      syncedSeq = -1;
      $("charts").replaceChildren();
      schedule();
    },
    onStatus(s) {
      link = s;
      if (s !== "open") syncedSeq = -1;
      schedule();
    },
  });
}

start();
