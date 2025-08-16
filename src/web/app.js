const $ = (id) => document.getElementById(id);

const chatEl = $("chat");
const statusEl = $("status");
const datasetEl = $("dataset");
const tableEl = $("table");
const fileInput = $("fileInput");

function addMsg(text, who="bot") {
  const div = document.createElement("div");
  div.className = `msg ${who}`;
  // simple code block detection
  if (typeof text === "string" && text.trim().startsWith("SELECT")) {
    const pre = document.createElement("pre");
    pre.textContent = text;
    div.appendChild(pre);
  } else if (typeof text === "object") {
    div.innerHTML = `<pre>${JSON.stringify(text, null, 2)}</pre>`;
  } else {
    div.textContent = String(text);
  }
  chatEl.appendChild(div);
  chatEl.scrollTop = chatEl.scrollHeight;
}

async function api(path, opts={}) {
  const res = await fetch(path, opts);
  const data = await res.json().catch(()=>({ok:false,error:"bad json"}));
  return {ok: res.ok, data};
}

async function refreshHealth() {
  const {data} = await api("/health");
  statusEl.textContent = `Provider: ${data.provider} • Offline: ${data.offline ? "yes" : "no"}`;
}

async function handleUpload() {
  if (!fileInput.files || fileInput.files.length === 0) {
    addMsg("No file selected.", "bot"); return;
  }
  const fd = new FormData();
  fd.append("file", fileInput.files[0]);
  addMsg(`Uploading: ${fileInput.files[0].name}`, "me");
  const {data} = await api("/upload", { method:"POST", body: fd });
  if (data.ok) {
    addMsg("Upload OK. Parsed summary:");
    addMsg(data.contract);
    addMsg("Note: Not auto-activating contract (to avoid overwriting your active one). Use the /contract/activate API if you want.");
  } else {
    addMsg(`Upload failed: ${data.detail || "unknown"}`);
  }
}

async function handleVerify() {
  const dataset = datasetEl.value || "prod";
  const table = tableEl.value || "spans";
  addMsg(`verify ${dataset} ${table}`, "me");
  const {data} = await api(`/verify/compare?dataset=${encodeURIComponent(dataset)}&table=${encodeURIComponent(table)}`);
  if (data && data.status) {
    addMsg({intent:"verify_tables", ...data});
  } else {
    addMsg(data);
  }
}

async function handleSend() {
  const input = $("chatInput");
  const text = input.value.trim();
  if (!text) return;
  const dataset = datasetEl.value || "prod";
  const table = tableEl.value || undefined;

  addMsg(text, "me");
  input.value = "";
  const payload = { text, dataset };
  if (table) payload.table = table;

  const {data} = await api("/chat", {
    method: "POST",
    headers: {"Content-Type":"application/json"},
    body: JSON.stringify(payload)
  });

  if (!data || data.ok === false) {
    addMsg(data?.message || "I didn't understand.");
    return;
  }

  const res = data.result || {};
  if (res.sql) {
    addMsg("Draft SQL:");
    addMsg(res.sql);
  }
  if (res.message) addMsg(res.message);
  if (res.violations && res.violations.length) {
    addMsg({policy_ok: res.policy_ok, violations: res.violations});
  }
}

window.addEventListener("DOMContentLoaded", async () => {
  datasetEl.value = "prod";
  await refreshHealth();

  $("sendBtn").addEventListener("click", handleSend);
  $("verifyBtn").addEventListener("click", handleVerify);
  $("fileInput").addEventListener("change", handleUpload);
  $("chatInput").addEventListener("keydown", (e) => {
    if (e.key === "Enter") handleSend();
  });
});
