async function fetchJson(path) {
  const r = await fetch(path, { cache: "no-store" });
  if (!r.ok) throw new Error("加载失败: " + path);
  return await r.json();
}

function deriveSummary(actions) {
  const s = { BUY: 0, HOLD: 0, REDUCE: 0 };
  const d = { FOUND: 0, SKIP: 0 };
  for (const it of actions) {
    const sig = String(it.signal || "");
    if (sig in s) s[sig] += 1;
    const label = String(it.label || "");
    if (label.startsWith("[DISCOVERY]") && label.includes("FOUND")) d.FOUND += 1;
    if (label.startsWith("[DISCOVERY]") && label.includes("SKIP")) d.SKIP += 1;
  }
  return { total_rows: actions.length, signal_summary: s, discovery_summary: d };
}

function resolveSnapshotPath(file) {
  const raw = String(file || "").replace(/^\.?\//, "");
  if (!raw) return "";
  return "../reports/" + raw;
}

const dateSel = document.getElementById("dateSel");
const runSel = document.getElementById("runSel");
const qEl = document.getElementById("q");
const pageSizeEl = document.getElementById("pageSize");
const prevBtn = document.getElementById("prevBtn");
const nextBtn = document.getElementById("nextBtn");
const pageInfo = document.getElementById("pageInfo");
const rowsInfo = document.getElementById("rowsInfo");
const tbody = document.getElementById("tbody");
const meta = document.getElementById("meta");
const summaryEl = document.getElementById("summary");
const diagTbody = document.getElementById("diagTbody");
const diagMeta = document.getElementById("diagMeta");

let index = [];
let currentActions = [];
let filteredRows = [];
let diagnosticsRows = [];
let currentPage = 1;

function shortId(v, left = 8, right = 6) {
  const s = String(v || "");
  if (!s || s === "—") return "—";
  if (s.length <= left + right + 3) return s;
  return `${s.slice(0, left)}...${s.slice(-right)}`;
}

function pct(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return `${(n * 100).toFixed(2)}%`;
}

function humanReason(it) {
  const raw = String(it.hold_reason || "").trim();
  if (!raw) return "—";
  if (raw.startsWith("dynamic_buy_usdc<")) return "资金不足：本次可下单金额低于最小下单额";
  if (raw === "token_position_limit") return "单一 token 持仓上限";
  if (raw === "condition_position_limit") return "同市场总持仓上限";
  if (raw === "total_exposure_limit") return "触发全局总暴露上限";
  if (raw === "exited_this_round") return "本轮已触发卖出，不重复买入";

  if (raw.includes("event_found_but_no_tradable_outcomes")) {
    return "已找到事件，但暂无可交易区间";
  }
  if (raw.includes("event_not_found_or_score_too_low")) {
    return "未找到匹配事件或匹配评分过低";
  }

  const cand = raw.match(/candidates=(\d+)/);
  const score = raw.match(/best_score=([-\d.]+)/);
  if (raw.includes("discovered condition=")) {
    return `发现成功（候选 ${cand?.[1] || "?"}，评分 ${score?.[1] || "?"}）`;
  }
  return raw;
}

function renderSummaryCards(rs) {
  const ss = rs.signal_summary || {};
  const ds = rs.discovery_summary || {};
  const tradable = Number(ss.BUY || 0) + Number(ss.REDUCE || 0);
  summaryEl.innerHTML = `
    <div class="metric"><div class="k">总记录</div><div class="v">${rs.total_rows ?? 0}</div></div>
    <div class="metric"><div class="k">BUY</div><div class="v">${ss.BUY ?? 0}</div></div>
    <div class="metric"><div class="k">HOLD</div><div class="v">${ss.HOLD ?? 0}</div></div>
    <div class="metric"><div class="k">REDUCE</div><div class="v">${ss.REDUCE ?? 0}</div></div>
    <div class="metric"><div class="k">发现成功</div><div class="v">${ds.FOUND ?? 0}</div></div>
    <div class="metric"><div class="k">发现跳过</div><div class="v">${ds.SKIP ?? 0}</div></div>
    <div class="metric"><div class="k">可交易信号</div><div class="v">${tradable}</div></div>
  `;
}

function fmtNum(v, digits = 3) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(digits);
}

function renderDiagnosticsTable() {
  if (!Array.isArray(diagnosticsRows) || diagnosticsRows.length === 0) {
    diagMeta.textContent = "暂无 diagnostics.json 数据（先运行机器人一轮）。";
    diagTbody.innerHTML =
      '<tr><td colspan="7"><div class="empty">没有诊断数据可展示。</div></td></tr>';
    return;
  }

  const selectedDate = String(dateSel.value || "").trim();
  const keyword = String(qEl.value || "").trim().toLowerCase();
  const currentCities = new Set(
    currentActions.map((it) => String(it.city || "").trim()).filter(Boolean),
  );

  let rows = diagnosticsRows.filter((it) => {
    const okDate = !selectedDate || String(it.date || "") === selectedDate;
    const okCity = currentCities.size === 0 || currentCities.has(String(it.city || "").trim());
    const hay = [
      it.city,
      it.date,
      it.date_label,
      it.base_label,
      it.condition_id,
      it?.yes?.token_id,
      it?.no?.token_id,
    ]
      .map((v) => String(v || "").toLowerCase())
      .join(" ");
    const okQuery = !keyword || hay.includes(keyword);
    return okDate && okCity && okQuery;
  });

  rows.sort((a, b) => {
    const ae = Math.max(Number(a?.yes?.edge || -999), Number(a?.no?.edge || -999));
    const be = Math.max(Number(b?.yes?.edge || -999), Number(b?.no?.edge || -999));
    return be - ae;
  });

  rows = rows.slice(0, 120);
  diagMeta.textContent = `诊断条目 ${rows.length} 条（按优势值从高到低，已和当前筛选联动）`;

  if (rows.length === 0) {
    diagTbody.innerHTML =
      '<tr><td colspan="7"><div class="empty">当前筛选条件下没有诊断数据。</div></td></tr>';
    return;
  }

  diagTbody.innerHTML = rows
    .map((it) => {
      const yesEdge = Number(it?.yes?.edge || 0);
      const noEdge = Number(it?.no?.edge || 0);
      const bestSide = yesEdge >= noEdge ? "YES" : "NO";
      const bestEdge = yesEdge >= noEdge ? yesEdge : noEdge;
      const sideClass = bestEdge > 0 ? "side-win" : "side-neutral";
      return `
        <tr>
          <td>${it.city || "—"}</td>
          <td>${it.date_label || "—"}<div class="small">${it.date || ""}</div></td>
          <td>${it.base_label || "—"}</td>
          <td>
            价=${fmtNum(it?.yes?.market_price)} | 概=${pct(it?.yes?.fair_prob)}<br/>
            Edge=${fmtNum(it?.yes?.edge)}
          </td>
          <td>
            价=${fmtNum(it?.no?.market_price)} | 概=${pct(it?.no?.fair_prob)}<br/>
            Edge=${fmtNum(it?.no?.edge)}
          </td>
          <td><span class="side-tag ${sideClass}">${bestSide} (${fmtNum(bestEdge)})</span></td>
          <td>${fmtNum(it.forecast_max, 2)} ${it.forecast_unit || ""}</td>
        </tr>
      `;
    })
    .join("");
}

function rowHtml(it) {
  const isDiscovery = String(it.label || "").startsWith("[DISCOVERY]");
  const p = (it.market_price ?? "") === "" ? "—" : it.market_price;
  const fair = (it.fair_prob ?? "") === "" ? "—" : it.fair_prob;
  const edge = (it.edge ?? "") === "" ? "—" : it.edge;
  const tokenPos = (it.current_position_shares ?? "") === "" ? "—" : it.current_position_shares;
  const conditionPos = (it.total_condition_position_shares ?? "") === "" ? "—" : it.total_condition_position_shares;
  const buyAmt = (it.dynamic_buy_usdc ?? "") === "" ? "—" : it.dynamic_buy_usdc;
  const reduceAmt = (it.reduce_size_shares ?? "") === "" ? "—" : it.reduce_size_shares;
  const tokenId = shortId(it.token_id || "—");
  const conditionId = shortId(it.condition_id || "—");
  const entry = (it.entry_price ?? "") === "" ? "—" : it.entry_price;
  const pnl = (it.unrealized_pnl ?? "") === "" ? "—" : pct(it.unrealized_pnl);
  const exitReason = it.exit_reason ? String(it.exit_reason) : "—";
  const kelly = (it.kelly_fraction ?? "") === "" ? "—" : pct(it.kelly_fraction);
  const expo = (it.total_exposure_usdc ?? "") === "" ? "—" : `$${Number(it.total_exposure_usdc).toFixed(2)}`;

  if (isDiscovery) {
    return `
      <tr>
        <td>${it.city || "—"}</td>
        <td>${it.date_label || ""}<div class="small">${it.date || ""}</div></td>
        <td><span class="pill HOLD">DISCOVERY</span></td>
        <td>${it.label || ""}<div class="small">市场探测步骤</div></td>
        <td class="small">—</td>
        <td class="small">—</td>
        <td class="small">token=—<br/>condition=${conditionId}</td>
        <td class="small">Kelly=—<br/>暴露=—</td>
        <td>${humanReason(it)}</td>
      </tr>
    `;
  }

  return `
    <tr>
      <td>${it.city || ""}</td>
      <td>${it.date_label || ""}<div class="small">${it.date || ""}</div></td>
      <td><span class="pill ${it.signal || "HOLD"}">${it.signal || ""}</span></td>
      <td>${it.label || ""}<div class="small mono">token: ${tokenId}</div></td>
      <td>市场价=${p}<br/>公允概率=${fair}</td>
      <td>Edge=${edge}<br/>浮盈亏=${pnl}<br/>退出原因=${exitReason}</td>
      <td>token仓=${tokenPos}<br/>市场总仓=${conditionPos}<br/>入场价=${entry}</td>
      <td>买入预算=$${buyAmt}<br/>减仓份额=${reduceAmt}<br/>Kelly=${kelly}<br/>总暴露=${expo}</td>
      <td>${it.question || "—"}<div class="small">${humanReason(it)}</div><div class="small mono">condition: ${conditionId}</div></td>
    </tr>
  `;
}

function applyFilter() {
  const q = qEl.value.trim().toLowerCase();
  filteredRows = currentActions.filter((it) => {
    if (!q) return true;
    const hay = [
      it.date,
      it.city,
      it.date_label,
      it.label,
      it.signal,
      it.token_id,
      it.condition_id,
      it.question,
      it.hold_reason,
    ]
      .map((v) => String(v || "").toLowerCase())
      .join(" ");
    return hay.includes(q);
  });
}

function renderTable() {
  applyFilter();
  const pageSize = Number(pageSizeEl.value || 20);
  const total = filteredRows.length;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  currentPage = Math.min(Math.max(1, currentPage), totalPages);

  const from = (currentPage - 1) * pageSize;
  const to = from + pageSize;
  const rows = filteredRows.slice(from, to);

  rowsInfo.textContent = `总计 ${currentActions.length} 条，筛选后 ${total} 条`;
  pageInfo.textContent = `${currentPage} / ${totalPages}`;
  prevBtn.disabled = currentPage <= 1;
  nextBtn.disabled = currentPage >= totalPages;

  if (rows.length === 0) {
    tbody.innerHTML =
      '<tr><td colspan="9"><div class="empty">本批次没有可展示记录。若 FOUND=0，表示该时段未发现匹配市场。</div></td></tr>';
    return;
  }
  tbody.innerHTML = rows.map(rowHtml).join("");
  renderDiagnosticsTable();
}

async function loadRun() {
  const file = runSel.value;
  currentPage = 1;
  if (!file) {
    currentActions = [];
    renderSummaryCards({
      total_rows: 0,
      signal_summary: { BUY: 0, HOLD: 0, REDUCE: 0 },
      discovery_summary: { FOUND: 0, SKIP: 0 },
    });
    renderTable();
    return;
  }

  const snap = await fetchJson(resolveSnapshotPath(file));
  currentActions = Array.isArray(snap.actions) ? snap.actions : [];
  const rs = snap.run_summary || deriveSummary(currentActions);
  meta.textContent = `批次: ${snap.generated_at || ""} | 条数: ${currentActions.length}`;
  renderSummaryCards(rs);
  renderTable();
}

function rebuildRunSelector() {
  const date = dateSel.value;
  runSel.innerHTML = "";
  const items = index.filter((x) => !date || x.date_key === date);
  items.forEach((x) => {
    const ss = x.signal_summary || {};
    const ds = x.discovery_summary || {};
    const op = document.createElement("option");
    op.value = x.file;
    op.textContent = `${x.generated_at} | BUY:${ss.BUY ?? 0} HOLD:${ss.HOLD ?? 0} REDUCE:${ss.REDUCE ?? 0} | FOUND:${ds.FOUND ?? 0} SKIP:${ds.SKIP ?? 0}`;
    runSel.appendChild(op);
  });
}

async function boot() {
  const idx = await fetchJson("../reports/history_index.json");
  try {
    const diag = await fetchJson("../reports/diagnostics.json");
    diagnosticsRows = Array.isArray(diag.rows) ? diag.rows : [];
    if (diag.generated_at) {
      diagMeta.textContent = `诊断快照时间：${diag.generated_at}，共 ${diagnosticsRows.length} 条。`;
    }
  } catch (err) {
    diagnosticsRows = [];
  }
  index = Array.isArray(idx.history) ? idx.history : [];
  const days = [...new Set(index.map((x) => x.date_key).filter(Boolean))];
  dateSel.innerHTML =
    '<option value="">全部日期</option>' + days.map((d) => `<option value="${d}">${d}</option>`).join("");
  rebuildRunSelector();
  await loadRun();
}

dateSel.addEventListener("change", async () => {
  rebuildRunSelector();
  await loadRun();
});
runSel.addEventListener("change", loadRun);
qEl.addEventListener("input", () => {
  currentPage = 1;
  renderTable();
});
pageSizeEl.addEventListener("change", () => {
  currentPage = 1;
  renderTable();
});
prevBtn.addEventListener("click", () => {
  currentPage -= 1;
  renderTable();
});
nextBtn.addEventListener("click", () => {
  currentPage += 1;
  renderTable();
});

boot().catch((err) => {
  meta.textContent = err.message + "（请先运行机器人并确认 reports/history_index.json 已生成）";
  renderSummaryCards({
    total_rows: 0,
    signal_summary: { BUY: 0, HOLD: 0, REDUCE: 0 },
    discovery_summary: { FOUND: 0, SKIP: 0 },
  });
  renderTable();
});
