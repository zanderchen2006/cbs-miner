const $ = (id) => document.getElementById(id);

const filters = ["server", "building", "floor", "room", "device", "metric"];
const berlin = new Intl.DateTimeFormat("de-DE", {
  timeZone: "Europe/Berlin",
  dateStyle: "short",
  timeStyle: "medium",
});

let timer = null;

function optionList(select, values, current) {
  const previous = current ?? select.value;
  select.innerHTML = "";
  const all = document.createElement("option");
  all.value = "";
  all.textContent = "Alle";
  select.appendChild(all);
  const ids = [];
  for (const value of values) {
    const option = document.createElement("option");
    if (value && typeof value === "object") {
      option.value = String(value.id);
      option.textContent = value.name;
      ids.push(String(value.id));
    } else {
      option.value = value;
      option.textContent = value;
      ids.push(value);
    }
    select.appendChild(option);
  }
  select.value = ids.includes(previous) ? previous : "";
}

function displayValue(value) {
  if (value === null || value === undefined || value === "") return "–";
  return String(value);
}

function query() {
  const params = new URLSearchParams();
  for (const key of filters) {
    const value = $(key).value;
    if (value) params.set(key, value);
  }
  const search = $("search").value.trim();
  if (search) params.set("search", search);
  params.set("limit", $("limit").value);
  return params;
}

function renderStats(stats, mqtt, servers) {
  $("stats").innerHTML = `
    <div class="stat"><span>Live-Topics</span><strong>${stats.live_topics ?? 0}</strong></div>
    <div class="stat"><span>Nachrichten</span><strong>${stats.n ?? 0}</strong></div>
    <div class="stat"><span>Erste</span><strong>${formatTime(stats.first_at)}</strong></div>
    <div class="stat"><span>Letzte</span><strong>${formatTime(stats.last_at)}</strong></div>
  `;
  const connected = Boolean(mqtt && mqtt.connected);
  const flag = $("live-flag");
  flag.classList.toggle("off", !connected);
  flag.textContent = connected ? "Live" : "Offline";
  const list = servers || mqtt?.servers || [];
  if (!list.length) {
    $("mqtt-status").textContent = "Kein MQTT-Server konfiguriert";
    return;
  }
  $("mqtt-status").textContent = list
    .map((server) => {
      const state = server.connected ? "verbunden" : "getrennt";
      const topics = (server.topics || []).map((item) => item.topic).join(", ") || "keine Topics";
      return `${server.name}: ${state} · ${topics}`;
    })
    .join("\n");
}

function formatTime(iso) {
  if (!iso) return "–";
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return iso;
  return berlin.format(date);
}

function renderLatest(rows, multiServer) {
  $("latest").replaceChildren(
    ...rows.map((row) => {
      const card = document.createElement("div");
      card.className = "card";
      const label = document.createElement("span");
      const place = `${displayValue(row.device)} · ${displayValue(row.metric)}`;
      label.textContent = multiServer && row.server_name ? `${row.server_name} · ${place}` : place;
      const value = document.createElement("strong");
      value.textContent = displayValue(row.payload);
      card.append(label, value);
      return card;
    }),
  );
}

function buildForest(items) {
  const roots = new Map();
  for (const item of items) {
    const sid = item.server_id ?? "none";
    const sname = item.server_name || "Unbekannt";
    if (!roots.has(sid)) {
      roots.set(sid, { name: sname, children: new Map(), item: null });
    }
    let node = roots.get(sid);
    const parts = (item.topic || "").split("/").filter(Boolean);
    for (const name of parts) {
      if (!node.children.has(name)) {
        node.children.set(name, { name, children: new Map(), item: null });
      }
      node = node.children.get(name);
    }
    node.item = item;
  }
  return [...roots.values()];
}

function renderNode(node) {
  if (node.children.size === 0) {
    const row = document.createElement("div");
    row.className = "leaf";
    const name = document.createElement("span");
    name.className = "leaf-name";
    name.textContent = node.name;
    const value = document.createElement("strong");
    value.textContent = displayValue(node.item && node.item.payload);
    row.append(name, value);
    return row;
  }
  const details = document.createElement("details");
  details.open = true;
  const summary = document.createElement("summary");
  summary.textContent = node.name;
  if (node.item) {
    const value = document.createElement("strong");
    value.textContent = ` ${displayValue(node.item.payload)}`;
    summary.appendChild(value);
  }
  const kids = document.createElement("div");
  kids.className = "tree-children";
  for (const child of node.children.values()) {
    kids.appendChild(renderNode(child));
  }
  details.append(summary, kids);
  return details;
}

function renderTree(items) {
  const forest = buildForest(items);
  if (!forest.length) {
    $("tree").replaceChildren();
    return;
  }
  $("tree").replaceChildren(...forest.map(renderNode));
}

function renderTable(rows) {
  $("table-title").textContent = `Letzte ${rows.length} Nachrichten`;
  $("rows").innerHTML = rows
    .map(
      (row) => `
      <tr>
        <td>${formatTime(row.received_at)}</td>
        <td>${displayValue(row.server_name)}</td>
        <td>${displayValue(row.building)}</td>
        <td>${displayValue(row.floor)}</td>
        <td>${displayValue(row.room)}</td>
        <td>${displayValue(row.device)}</td>
        <td>${displayValue(row.metric)}</td>
        <td>${displayValue(row.payload)}</td>
        <td class="topic" title="${displayValue(row.topic)}">${displayValue(row.topic)}</td>
      </tr>
    `,
    )
    .join("");
}

function drawChart(messages) {
  const svg = $("chart");
  const empty = $("chart-empty");
  const numeric = messages.filter((row) => typeof row.value_num === "number");
  svg.replaceChildren();
  if (numeric.length < 2) {
    empty.classList.remove("hidden");
    return;
  }
  empty.classList.add("hidden");

  const grouped = new Map();
  for (const row of [...numeric].reverse()) {
    const key = row.metric || "wert";
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push({
      t: new Date(row.received_at).getTime(),
      v: row.value_num,
    });
  }

  const width = 800;
  const height = 220;
  const pad = { l: 44, r: 16, t: 16, b: 28 };
  const all = [...grouped.values()].flat();
  const tMin = Math.min(...all.map((p) => p.t));
  const tMax = Math.max(...all.map((p) => p.t));
  const vMin = Math.min(...all.map((p) => p.v));
  const vMax = Math.max(...all.map((p) => p.v));
  const tSpan = Math.max(tMax - tMin, 1);
  const vSpan = Math.max(vMax - vMin, 1);
  const x = (t) => pad.l + ((t - tMin) / tSpan) * (width - pad.l - pad.r);
  const y = (v) => pad.t + (1 - (v - vMin) / vSpan) * (height - pad.t - pad.b);
  const colors = ["#0081c6", "#94bc0c", "#585858", "#0069b0", "#88bbc8"];

  const axis = document.createElementNS("http://www.w3.org/2000/svg", "polyline");
  axis.setAttribute(
    "points",
    `${pad.l},${pad.t} ${pad.l},${height - pad.b} ${width - pad.r},${height - pad.b}`,
  );
  axis.setAttribute("fill", "none");
  axis.setAttribute("stroke", "#d6dee4");
  svg.appendChild(axis);

  let colorIndex = 0;
  for (const [name, points] of grouped) {
    const color = colors[colorIndex % colors.length];
    colorIndex += 1;
    const line = document.createElementNS("http://www.w3.org/2000/svg", "polyline");
    line.setAttribute(
      "points",
      points.map((p) => `${x(p.t).toFixed(1)},${y(p.v).toFixed(1)}`).join(" "),
    );
    line.setAttribute("fill", "none");
    line.setAttribute("stroke", color);
    line.setAttribute("stroke-width", "2");
    line.setAttribute("data-metric", name);
    svg.appendChild(line);
  }
}

function renderServers(servers) {
  const list = $("server-list");
  const items = servers || [];
  list.replaceChildren();
  if (!items.length) {
    const empty = document.createElement("p");
    empty.className = "muted";
    empty.textContent = "Noch kein Server.";
    list.appendChild(empty);
    return;
  }
  for (const server of items) {
    const card = document.createElement("div");
    card.className = "server-card";
    const head = document.createElement("div");
    head.className = "server-head";
    const dot = document.createElement("span");
    dot.className = `status-dot${server.connected ? " on" : ""}${server.enabled ? "" : " off"}`;
    const title = document.createElement("strong");
    title.textContent = server.name;
    head.append(dot, title);
    const meta = document.createElement("p");
    meta.className = "muted";
    meta.textContent = `${server.host}:${server.port}${server.tls ? " · TLS" : ""}${server.enabled ? "" : " · inaktiv"}`;
    const topics = document.createElement("ul");
    topics.className = "topic-list";
    for (const item of server.topics || []) {
      const li = document.createElement("li");
      li.textContent = item.topic;
      topics.appendChild(li);
    }
    if (!server.topics || !server.topics.length) {
      const li = document.createElement("li");
      li.className = "muted";
      li.textContent = "keine Topics";
      topics.appendChild(li);
    }
    const actions = document.createElement("div");
    actions.className = "form-actions";
    const edit = document.createElement("button");
    edit.type = "button";
    edit.className = "btn btn-light";
    edit.textContent = "Bearbeiten";
    edit.addEventListener("click", () => openServerForm(server));
    const remove = document.createElement("button");
    remove.type = "button";
    remove.className = "btn btn-light";
    remove.textContent = "Löschen";
    remove.addEventListener("click", () => deleteServer(server));
    actions.append(edit, remove);
    card.append(head, meta);
    if (server.error && !server.connected) {
      const err = document.createElement("p");
      err.className = "form-error";
      err.textContent = server.error;
      card.append(err);
    }
    card.append(topics, actions);
    list.appendChild(card);
  }
}

function addTopicRow(value) {
  const row = document.createElement("div");
  row.className = "topic-row";
  const input = document.createElement("input");
  input.type = "text";
  input.className = "topic-input";
  input.placeholder = "cbs_koblenz/#";
  input.value = value || "";
  const remove = document.createElement("button");
  remove.type = "button";
  remove.className = "btn btn-light";
  remove.textContent = "×";
  remove.setAttribute("aria-label", "Topic entfernen");
  remove.addEventListener("click", () => row.remove());
  row.append(input, remove);
  $("server-topics-fields").appendChild(row);
}

function topicValues() {
  return [...$("server-topics-fields").querySelectorAll(".topic-input")]
    .map((input) => input.value.trim())
    .filter(Boolean);
}

function setFormError(message) {
  const node = $("server-form-error");
  node.hidden = !message;
  node.textContent = message || "";
}

function openServerForm(server) {
  $("server-form").hidden = false;
  $("server-add").hidden = true;
  setFormError("");
  $("server-id").value = server ? String(server.id) : "";
  $("server-name").value = server ? server.name : "";
  $("server-host").value = server ? server.host : "";
  $("server-port").value = server ? server.port : 8883;
  $("server-tls").checked = server ? Boolean(server.tls) : true;
  $("server-user").value = server ? server.username || "" : "";
  $("server-pass").value = "";
  $("server-pass").placeholder = server && server.has_password ? "unverändert" : "";
  $("server-client-id").value = server ? server.client_id || "" : "";
  $("server-enabled").checked = server ? Boolean(server.enabled) : true;
  $("server-topics-fields").replaceChildren();
  const topics = server && server.topics && server.topics.length ? server.topics : [{ topic: "" }];
  for (const item of topics) {
    addTopicRow(item.topic || "");
  }
  $("server-host").focus();
}

function closeServerForm() {
  $("server-form").hidden = true;
  $("server-add").hidden = false;
  setFormError("");
  $("server-form").reset();
  $("server-id").value = "";
  $("server-topics-fields").replaceChildren();
}

function formPayload() {
  const port = Number($("server-port").value || 8883);
  const payload = {
    name: $("server-name").value.trim(),
    host: $("server-host").value.trim(),
    port,
    tls: $("server-tls").checked,
    username: $("server-user").value.trim(),
    client_id: $("server-client-id").value.trim(),
    enabled: $("server-enabled").checked,
    topics: topicValues(),
  };
  const password = $("server-pass").value;
  if (password) payload.password = password;
  return payload;
}

async function saveServer(event) {
  event.preventDefault();
  const payload = formPayload();
  if (!payload.host) {
    setFormError("Host ist erforderlich");
    return;
  }
  const id = $("server-id").value;
  const response = await fetch(id ? `/api/servers/${id}` : "/api/servers", {
    method: id ? "PUT" : "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    setFormError(data.error || "Speichern fehlgeschlagen");
    return;
  }
  closeServerForm();
  await load();
}

async function deleteServer(server) {
  if (!window.confirm(`Server „${server.name}“ wirklich löschen?`)) return;
  const response = await fetch(`/api/servers/${server.id}`, { method: "DELETE" });
  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    window.alert(data.error || "Löschen fehlgeschlagen");
    return;
  }
  if ($("server-id").value === String(server.id)) closeServerForm();
  await load();
}

async function load() {
  const response = await fetch(`/api/state?${query()}`);
  const data = await response.json();
  $("db-path").textContent = data.db_path || "";
  const servers = data.servers || data.mqtt?.servers || [];
  renderServers(servers);
  optionList($("server"), data.filters?.server || servers.map((s) => ({ id: s.id, name: s.name })));
  const live = data.live || [];
  const hasData = Boolean(
    live.length || (data.stats && data.stats.n) || (data.mqtt && data.mqtt.connected),
  );
  if (!hasData) {
    $("empty").hidden = false;
    $("content").hidden = true;
    $("empty-text").textContent = servers.length
      ? "Warten auf MQTT-Nachrichten …"
      : "Lege in der Seitenleiste einen MQTT-Server an.";
    $("mqtt-status").textContent = servers.length
      ? servers
          .map((server) => `${server.name}: ${server.connected ? "verbunden" : "getrennt"}`)
          .join("\n")
      : "Kein MQTT-Server konfiguriert";
    return;
  }

  $("empty").hidden = true;
  $("content").hidden = false;
  optionList($("building"), data.filters.building);
  optionList($("floor"), data.filters.floor);
  optionList($("room"), data.filters.room);
  optionList($("device"), data.filters.device);
  optionList($("metric"), data.filters.metric);
  renderStats(data.stats || {}, data.mqtt, servers);
  renderTree(live);
  renderLatest(
    live.filter((row) => row.device && row.metric),
    servers.length > 1,
  );
  drawChart(data.messages || []);
  renderTable(data.messages || []);
}

function schedule() {
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
  if ($("autorefresh").checked) {
    timer = setInterval(load, 1000);
  }
}

$("sidebar-toggle").addEventListener("click", () => {
  const closed = document.body.classList.toggle("sidebar-closed");
  $("sidebar-toggle").setAttribute("aria-expanded", String(!closed));
});

for (const id of [...filters, "limit"]) {
  $(id).addEventListener("change", load);
}
$("search").addEventListener("input", () => {
  clearTimeout($("search")._t);
  $("search")._t = setTimeout(load, 250);
});
$("autorefresh").addEventListener("change", schedule);
$("server-add").addEventListener("click", () => openServerForm(null));
$("server-form-cancel").addEventListener("click", closeServerForm);
$("topic-add-row").addEventListener("click", () => addTopicRow(""));
$("server-form").addEventListener("submit", (event) => {
  saveServer(event).catch((err) => setFormError(err.message || "Speichern fehlgeschlagen"));
});
$("server-port").addEventListener("change", () => {
  $("server-tls").checked = Number($("server-port").value) !== 1883;
});

load().then(schedule).catch((err) => {
  console.error(err);
  $("empty").hidden = false;
  $("content").hidden = true;
});
