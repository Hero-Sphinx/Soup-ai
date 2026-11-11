const KEY = "soup_key";
const $ = (id) => document.getElementById(id);

let apiKey = sessionStorage.getItem(KEY);
let timer = null;

async function api(path, init = {}) {
  const res = await fetch(path, {
    ...init,
    cache: "no-store",
    headers: { authorization: `Bearer ${apiKey}`, ...(init.body ? { "content-type": "application/json" } : {}) },
  });
  if (res.status === 401) {
    signOut("That key is not valid (it may have been rotated).");
    throw new Error("unauthorized");
  }
  if (!res.ok) throw new Error(`Request failed (${res.status})`);
  return res.json();
}

function signOut(message = "") {
  sessionStorage.removeItem(KEY);
  apiKey = null;
  clearInterval(timer);
  $("runs-view").hidden = true;
  $("who").hidden = true;
  $("signin").hidden = false;
  $("signin-error").textContent = message;
}

function renderRuns(runs) {
  const list = $("runs");
  list.replaceChildren(
    ...runs.map((run) => {
      const li = document.createElement("li");
      li.dataset.runId = run.id;
      const a = document.createElement("a");
      a.href = `/run.html?id=${encodeURIComponent(run.id)}`;
      const name = document.createElement("strong");
      name.textContent = run.name;
      const meta = document.createElement("span");
      meta.className = "muted";
      meta.dataset.testid = "run-points";
      meta.dataset.points = String(run.last_seq);
      meta.textContent = `${run.last_seq.toLocaleString()} points · updated ${new Date(run.updated_at).toLocaleTimeString()}`;
      a.append(name, meta);
      li.append(a);
      return li;
    }),
  );
  $("runs-empty").hidden = runs.length > 0;
}

async function refresh() {
  try {
    const { runs } = await api("/runs");
    renderRuns(runs);
    $("runs-error").textContent = "";
  } catch (err) {
    if (apiKey) $("runs-error").textContent = "Can't reach the server. Retrying…";
  }
}

async function open() {
  const project = await api("/project");
  $("project-name").textContent = project.name;
  $("signin").hidden = true;
  $("who").hidden = false;
  $("runs-view").hidden = false;
  await refresh();
  clearInterval(timer);
  timer = setInterval(refresh, 3000);
}

$("signin-form").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  apiKey = $("key").value.trim();
  sessionStorage.setItem(KEY, apiKey);
  $("key").value = "";
  try {
    await open();
  } catch (err) {
    if (err.message !== "unauthorized") signOut("Can't reach the server. Try again.");
  }
});

$("signout").addEventListener("click", () => signOut());

$("rotate").addEventListener("click", async () => {
  if (!confirm("Rotate the key? The current key stops working immediately, everywhere.")) return;
  const { api_key } = await api("/keys/rotate", { method: "POST" });
  apiKey = api_key;
  sessionStorage.setItem(KEY, apiKey);
  const note = $("new-key");
  note.textContent = `New key (shown once, copy it now): ${api_key}`;
  note.hidden = false;
});

$("new-run").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const input = $("run-name");
  try {
    await api("/runs", { method: "POST", body: JSON.stringify({ name: input.value }) });
    input.value = "";
    await refresh();
  } catch {
    $("runs-error").textContent = "Couldn't create the run.";
  }
});

if (apiKey) open().catch(() => {});
else signOut();
