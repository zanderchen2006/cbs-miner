const $ = (id) => document.getElementById(id);

const filters = ["building", "floor", "room", "device", "metric"];
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
  for (const value of values) {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = value;
    select.appendChild(option);
  }
  select.value = values.includes(previous) ? previous : "";
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

function renderStats(stats, mqtt) {
  $("stats").innerHTML = `
    <div class="stat"><span>Live-Topics</span><strong>${stats.live_topics ?? 0}</strong></div>
    <div class="stat"><span>Nachrichten</span><strong>${stats.n ?? 0}</strong></div>
    <div class="stat"><span>Erste</span><strong>${formatTime(stats.first_at)}</strong></div>
    <div class="stat"><span>Letzte</span><strong>${formatTime(stats.last_at)}</strong></div>
  `;
  const flag = $("live-flag");
  flag.classList.toggle("off", !mqtt || !mqtt.connected);
  flag.textContent = mqtt && mqtt.connected ? "Live" : "Offline";
  $("mqtt-status").textContent = mqtt
    ? `${mqtt.connected ? "MQTT verbunden" : "MQTT getrennt"} · ${mqtt.topic}`
    : "";
}

function formatTime(iso) {
  if (!iso) return "–";
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return iso;
  return berlin.format(date);
}

function renderLatest(rows) {
  $("latest").replaceChildren(
    ...rows.map((row) => {
      const card = document.createElement("div");
      card.className = "card";
      const label = document.createElement("span");
      label.textContent = `${displayValue(row.device)} · ${displayValue(row.metric)}`;
      const value = document.createElement("strong");
      value.textContent = displayValue(row.payload);
      card.append(label, value);
      return card;
    }),
  );
}

function buildTree(items) {
  const root = { name: "cbs_koblenz", children: new Map(), item: null };
  for (const item of items) {
    const parts = (item.topic || "").split("/").filter(Boolean);
    let node = root;
    const start = parts[0] === "cbs_koblenz" ? 1 : 0;
    for (let i = start; i < parts.length; i++) {
      const name = parts[i];
      if (!node.children.has(name)) {
        node.children.set(name, { name, children: new Map(), item: null });
      }
      node = node.children.get(name);
    }
    node.item = item;
  }
  return root;
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
  const root = buildTree(items);
  $("tree").replaceChildren(renderNode(root));
}

function renderTable(rows) {
  $("table-title").textContent = `Letzte ${rows.length} Nachrichten`;
  $("rows").innerHTML = rows
    .map(
      (row) => `
      <tr>
        <td>${formatTime(row.received_at)}</td>
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

async function load() {
  const response = await fetch(`/api/state?${query()}`);
  const data = await response.json();
  $("db-path").textContent = data.db_path || "";
  const live = data.live || [];
  const hasData = Boolean(
    live.length || (data.stats && data.stats.n) || (data.mqtt && data.mqtt.connected),
  );
  $("empty").hidden = hasData;
  $("content").hidden = !hasData;
  if (!hasData) return;

  optionList($("building"), data.filters.building);
  optionList($("floor"), data.filters.floor);
  optionList($("room"), data.filters.room);
  optionList($("device"), data.filters.device);
  optionList($("metric"), data.filters.metric);
  renderStats(data.stats || {}, data.mqtt);
  renderTree(live);
  renderLatest(live.filter((row) => row.device && row.metric));
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

load().then(schedule).catch(() => {
  $("empty").hidden = false;
  $("content").hidden = true;
});
