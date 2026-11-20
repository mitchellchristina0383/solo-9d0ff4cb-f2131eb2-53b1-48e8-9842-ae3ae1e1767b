// Browser-side SSE client for the failover gateway.
const form = document.getElementById("chat-form");
const promptInput = document.getElementById("prompt");
const maxTokensInput = document.getElementById("max-tokens");
const scenarioSelect = document.getElementById("scenario");
const stopMarkerInput = document.getElementById("stop-marker");
const runBtn = document.getElementById("run-btn");
const statusEl = document.getElementById("status");
const outputEl = document.getElementById("output");
const eventsEl = document.getElementById("events");
const counterEl = document.getElementById("counter");
const sharesEl = document.getElementById("shares");
const verificationEl = document.getElementById("verification");
const markerInfoEl = document.getElementById("marker-info");
const presetsEl = document.getElementById("marker-presets");

let expected = null;

async function loadScenarios() {
  const resp = await fetch("/api/scenarios");
  const data = await resp.json();
  scenarioSelect.innerHTML = "";
  for (const s of data.scenarios) {
    const opt = document.createElement("option");
    opt.value = s.id;
    opt.textContent = s.id;
    scenarioSelect.appendChild(opt);
  }
}

function setStatus(kind, text) {
  statusEl.className = `status ${kind}`;
  statusEl.textContent = text;
}

function addEvent(kind, html) {
  const div = document.createElement("div");
  div.className = `event-line ${kind}`;
  div.innerHTML = html;
  eventsEl.appendChild(div);
}

// Mirror of the gateway's append-only guard: refuse duplicate / reordered
// indices so a late packet could never mutate what the user already sees.
let lastIndex = -1;
function packetOrderGuard(index) {
  if (index !== lastIndex + 1) {
    addEvent("warn", `⚠️ 忽略乱序/迟到词元 index=${index}（下一个应为 ${lastIndex + 1}）`);
    throw new Error("out-of-order token");
  }
  lastIndex = index;
}

function appendToken(obj) {
  const placeholder = outputEl.querySelector(".placeholder");
  if (placeholder) placeholder.remove();
  packetOrderGuard(obj.index);
  // Append-only: never overwrite previously displayed tokens.
  const chip = document.createElement("span");
  chip.className = `token ${obj.node}`;
  chip.dataset.index = obj.index;
  chip.title = `index=${obj.index} id=${obj.id} 来源=${obj.node}`;
  chip.textContent = obj.text;
  outputEl.appendChild(chip);
  counterEl.textContent = `已采纳词元：${obj.index + 1}`;
}

function renderShares(nodes) {
  if (!nodes) return;
  sharesEl.textContent = `各节点词元占比：${Object.entries(nodes)
    .map(([node, n]) => `${node}=${n}`).join("，")}`;
}

async function fetchExpected() {
  const resp = await fetch(
    `/api/expected?prompt=${encodeURIComponent(promptInput.value)}&max_tokens=${maxTokensInput.value}`
  );
  return resp.json();
}

async function verifyAgainstHealthy(tokens, incomplete, endReason, markerIndex) {
  if (!expected) expected = await fetchExpected();
  const got = tokens.map((t) => t.id);
  const n = got.length;
  if (incomplete) {
    const prefixOk = isPrefix(got, expected.ids);
    verificationEl.className = `verification ${prefixOk ? "pass" : "fail"}`;
    verificationEl.innerHTML = prefixOk
      ? `✅ 未完成：保留的 ${n} 个词元与无故障基准的同位置前缀完全一致（已显示内容未被篡改）。`
      : "❌ 保留前缀与基准不一致！";
    return;
  }
  const comparable = isPrefix(got, expected.ids);
  if (endReason === "stop_marker") {
    // Emitted tokens = healthy stream up to (excluding) the matched span.
    const trimOk = comparable && n === markerIndex;
    verificationEl.className = `verification ${trimOk ? "pass" : "fail"}`;
    verificationEl.innerHTML = trimOk
      ? `✅ 命中结束标记：下发的 ${n} 个词元恰为无故障基准在标记之前的前缀，标记及其后内容均未下发。`
      : `❌ 命中标记后的输出与基准前缀不一致（下发 ${n} 个，期望 ${markerIndex} 个）！`;
    return;
  }
  const fullOk = comparable && n === expected.ids.length;
  verificationEl.className = `verification ${fullOk ? "pass" : "fail"}`;
  verificationEl.innerHTML = fullOk
    ? "✅ 接续成功：整条回复（首节点前缀 + 备用节点续写）与无故障 healthy 场景的词元序列完全一致。"
    : "❌ failover 结果与 healthy 不一致！";
}

function isPrefix(got, full) {
  return got.length <= full.length && got.every((id, i) => id === full[i]);
}

// Minimal SSE parser over fetch() POST (EventSource cannot POST).
async function consumeSSE(response, onEvent) {
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let event = null;
  const dataLines = [];

  const dispatch = () => {
    if (event !== null || dataLines.length) {
      onEvent(event || "message", dataLines.join("\n"));
    }
    event = null;
    dataLines.length = 0;
  };

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let sep;
    while ((sep = buffer.indexOf("\n")) !== -1) {
      const line = buffer.slice(0, sep).replace(/\r$/, "");
      buffer = buffer.slice(sep + 1);
      if (line === "") {
        dispatch();
      } else if (line.startsWith("event:")) {
        event = line.slice(6).trim();
      } else if (line.startsWith("data:")) {
        dataLines.push(line.slice(5).replace(/^ /, ""));
      }
    }
  }
  dispatch();
}

function showMarkerInfo(html) {
  markerInfoEl.className = "marker-info";
  markerInfoEl.innerHTML = html;
}

function renderSwallowedMarker(done) {
  const chips = done.stop_marker
    .map((t, i) => {
      const idx = done.marker_index + i;
      return `<span class="swallow">${escapeHtml(t)}<sub>#${idx}</sub></span>`;
    })
    .join(" ");
  showMarkerInfo(
    `⏹ 命中结束标记，以下 ${done.stop_marker.length} 个词元（下标 ${done.marker_index}–${done.marker_end_index - 1}）` +
    `计入上限但<b>未下发</b>：${chips}`
  );
}

function renderPartialMarker(event) {
  const tail = event.partial_marker;
  const tokens = tail.map((t) => `<span class="swallow">${escapeHtml(t)}</span>`).join(" ");
  const why = event.reason === "max_tokens" ? "达到词元上限" : "生成未完成";
  showMarkerInfo(
    `⚠️ ${why}时末尾只有结束标记的一部分（${tokens}），这部分已照常作为正文发出。`
  );
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

async function run() {
  runBtn.disabled = true;
  setStatus("running", "生成中…");
  outputEl.innerHTML = '<span class="placeholder">（回复将在这里逐词元出现）</span>';
  eventsEl.innerHTML = "";
  sharesEl.textContent = "";
  markerInfoEl.className = "marker-info hidden";
  markerInfoEl.innerHTML = "";
  verificationEl.className = "verification hidden";
  verificationEl.innerHTML = "";
  counterEl.textContent = "已采纳词元：0";
  lastIndex = -1;
  expected = null;
  const tokens = [];
  let endReason = null;
  let incomplete = false;
  let markerIndex = null;

  const req = {
    prompt: promptInput.value,
    max_tokens: parseInt(maxTokensInput.value, 10),
    scenario: scenarioSelect.value,
    stop_marker: stopMarkerInput.value.trim(),
  };

  try {
    const resp = await fetch("/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(req),
    });
    if (!resp.ok || !resp.body) {
      throw new Error(`网关返回 HTTP ${resp.status}`);
    }

    await consumeSSE(resp, (event, data) => {
      const obj = JSON.parse(data);
      switch (event) {
        case "meta":
          addEvent("", `请求 ${obj.request_id.slice(0, 8)}… 场景=<b>${obj.scenario}</b>，词元上限=${obj.max_tokens}` +
            (obj.marker_tokens && obj.marker_tokens.length
              ? `，结束标记=<b>${escapeHtml(obj.marker_tokens.join(" "))}</b>`
              : "，无结束标记"));
          break;
        case "token":
          appendToken(obj);
          tokens.push(obj);
          break;
        case "fallback":
          addEvent("warn",
            `⏱ 首节点 <b>${obj.from_node}</b> ${obj.reason === "read_timeout" ? "读超时" : "出错"}（已采纳 ${obj.accepted_count} 个词元），切换到 <b>${obj.to_node}</b> 续写…` +
            (obj.marker_pending_count ? `（其中 ${obj.marker_pending_count} 个词元因可能是标记而暂存）` : ""));
          break;
        case "incomplete":
          incomplete = true;
          endReason = "incomplete";
          addEvent("warn", `⛔ 标记为未完成：${obj.reason}（保留 ${obj.accepted_count} 个已显示词元）`);
          if (obj.partial_marker) renderPartialMarker(obj);
          break;
        case "done":
          endReason = obj.reason;
          markerIndex = obj.marker_index ?? null;
          renderShares(obj.nodes);
          if (obj.reason === "stop_marker") {
            addEvent("ok", `⏹ 命中结束标记：共生成 ${obj.token_count} 个词元，下发 ${obj.emitted_count} 个，结束于下标 ${obj.marker_index}`);
            counterEl.textContent = `已下发词元：${obj.emitted_count}（标记另占 ${obj.token_count - obj.emitted_count} 个上限）`;
            renderSwallowedMarker(obj);
          } else {
            addEvent("ok", `✔ 达到词元上限：共下发 ${obj.emitted_count} 个词元`);
            counterEl.textContent = `已下发词元：${obj.emitted_count}`;
            if (obj.partial_marker) renderPartialMarker(obj);
          }
          break;
        default:
          addEvent("", JSON.stringify(obj));
      }
    });

    if (endReason === "stop_marker") {
      setStatus("marker", "命中结束标记");
    } else if (endReason === "incomplete") {
      setStatus("incomplete", "未完成");
    } else if (endReason === "max_tokens") {
      setStatus("done", "达到上限");
    } else {
      setStatus("incomplete", "未完成");
    }
    await verifyAgainstHealthy(tokens, incomplete, endReason, markerIndex);
  } catch (err) {
    setStatus("incomplete", "请求失败");
    addEvent("warn", `⛔ ${err.message}`);
  } finally {
    runBtn.disabled = false;
  }
}

// Presets computed from the deterministic token stream of the current
// prompt, so the marker demos stay repeatable.
async function refreshPresets() {
  presetsEl.innerHTML = "";
  let exp;
  try {
    exp = await fetchExpected();
  } catch (err) {
    return;
  }
  const t = exp.texts;
  const max = t.length;
  const presets = [];
  const markerAt = (start, len) => t.slice(start, start + len).join(" ");
  const occursBefore = (needle, end) => {
    const parts = needle.split(" ");
    for (let i = 0; i + parts.length <= end; i++) {
      if (parts.every((p, j) => t[i + j] === p)) return true;
    }
    return false;
  };
  if (max >= 6) {
    presets.push({
      label: `命中标记（无故障）：${markerAt(2, 2)}`,
      marker: markerAt(2, 2),
      scenario: "healthy",
      hint: "首节点在第 3 个词元处命中，标记不进入输出流。",
    });
    const cross = markerAt(3, 2);
    if (!occursBefore(cross, 3)) {
      presets.push({
        label: `跨主备切换：${cross}`,
        marker: cross,
        scenario: "failover_success",
        hint: "首节点在第 4 个词元前挂起，标记的第一个词元被暂存，要等备用节点的第一个词元才完整。",
      });
    }
  }
  if (max >= 2) {
    // A following token that never arrives: only the first marker token is
    // ever seen, so it must be emitted as ordinary text at the budget limit.
    presets.push({
      label: `上限截断尾部半截：${t[max - 1]}`,
      marker: `${t[max - 1]} zzz`,
      scenario: "healthy",
      hint: "上限用尽时末尾只有标记的第一个词元，应照常发出。",
    });
  }
  if (max >= 8) {
    const faultTail = markerAt(6, 2);
    if (!occursBefore(faultTail, 7)) {
      presets.push({
        label: `故障终止尾部半截：${faultTail}`,
        marker: faultTail,
        scenario: "failover_backup_error",
        hint: "备用节点在下标 7 前失败，末尾只有标记的第一个词元，应照常发出。",
      });
    }
  }
  for (const p of presets) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.textContent = p.label;
    if (p.hint) btn.title = p.hint;
    btn.addEventListener("click", () => {
      stopMarkerInput.value = p.marker;
      if (scenarioSelect.querySelector(`option[value="${p.scenario}"]`)) {
        scenarioSelect.value = p.scenario;
      }
      addEvent("", `已填充结束标记 <b>${escapeHtml(p.marker)}</b>，场景 <b>${p.scenario}</b>。${p.hint || ""}`);
    });
    presetsEl.appendChild(btn);
  }
}

form.addEventListener("submit", (e) => {
  e.preventDefault();
  run();
});
promptInput.addEventListener("change", refreshPresets);
maxTokensInput.addEventListener("change", refreshPresets);
stopMarkerInput.addEventListener("change", refreshPresets);
refreshPresets();
loadScenarios();
