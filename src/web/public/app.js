// =====================
// Tab 切换
// =====================
document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('tab-' + btn.dataset.tab).classList.add('active');
  });
});

// =====================
// 通用辅助
// =====================
const escape = s => String(s ?? '').replace(/[&<>"']/g, c => ({
  '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
}[c]));

// 浏览器本地时间（用户视角，用于 "seen" / "first_seen_at" / "last_check_at" 等"我们什么时候看到的"）
const fmtTs = (ms) => {
  if (!ms) return '—';
  const d = new Date(Number(ms));
  if (isNaN(d.getTime())) return '—';
  return d.toLocaleString();
};
const fmtTsSec = (sec) => fmtTs(sec ? sec * 1000 : null);

// 城市本地时间（数据视角，用于 "obs_ts" / detail 表里的"这条观测在哪个时刻"）
// tz 形如 'Asia/Shanghai' / 'Europe/London' / 'America/New_York'
const CITY_TIME_FMT_CACHE = new Map();
function getCityFmt(tz) {
  if (!CITY_TIME_FMT_CACHE.has(tz)) {
    try {
      CITY_TIME_FMT_CACHE.set(tz, new Intl.DateTimeFormat('zh-CN', {
        timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit',
        hour: '2-digit', minute: '2-digit', hour12: false
      }));
    } catch {
      CITY_TIME_FMT_CACHE.set(tz, null);
    }
  }
  return CITY_TIME_FMT_CACHE.get(tz);
}
function fmtCityTimeSec(unixSec, tz) {
  if (!unixSec || !tz) return fmtTsSec(unixSec);
  const fmt = getCityFmt(tz);
  if (!fmt) return fmtTsSec(unixSec);
  try { return fmt.format(new Date(Number(unixSec) * 1000)); }
  catch { return fmtTsSec(unixSec); }
}

// 用户本地时区名（用于 legend 显示，让用户清楚 "seen" 是哪个时区）
const USER_TZ = (() => {
  try { return Intl.DateTimeFormat().resolvedOptions().timeZone || 'local'; }
  catch { return 'local'; }
})();

const fmtIv = (n) => {
  const x = Number(n);
  if (!isFinite(x) || x === 0) return '—';
  if (x >= 1e12) return `$${(x / 1e12).toFixed(2)}T`;
  if (x >= 1e9)  return `$${(x / 1e9).toFixed(2)}B`;
  if (x >= 1e6)  return `$${(x / 1e6).toFixed(2)}M`;
  return `$${x.toFixed(0)}`;
};

function fmtHL(s, unit) {
  if (!s || s.high === undefined || s.high === null) return '<span class="dash">—</span>';
  const lo = s.low !== null && s.low !== undefined ? s.low : '—';
  return `${s.high}/${lo}`;
}

function setUpdated(tab) {
  const el = document.getElementById(tab + '-updated');
  if (el) el.textContent = `updated ${new Date().toLocaleTimeString()}`;
}

function flash(el) {
  if (!el) return;
  el.classList.remove('flash');
  void el.offsetWidth;
  el.classList.add('flash');
}

// =====================
// Tab 1: Forecast
// =====================
function renderForecast(root, data) {
  if (!data.cities || data.cities.length === 0) {
    root.innerHTML = '<div class="empty">no cities configured (set poly:config:cities)</div>';
    return;
  }
  root.innerHTML = data.cities.map(({ city, days }) => `
    <div class="card" data-station="${escape(city.station)}" data-name="${escape((city.name || '').toLowerCase())}">
      <div class="card-header">
        <div class="card-title">
          ${escape(city.name)} <small>${escape(city.station)} · ${escape(city.unit)} · ${escape(city.tz)}</small>
        </div>
      </div>
      <table>
        <thead>
          <tr>
            <th>Date</th>
            <th>Wunder</th>
            <th>OpenM</th>
            <th>ECMWF</th>
            <th>GFS</th>
            <th>ICON</th>
            <th>NOAA</th>
            <th>MET.NO</th>
          </tr>
        </thead>
        <tbody>
          ${days.map(d => {
            const s = d.sources || {};
            return `<tr>
              <td>${d.date}</td>
              <td class="value-num">${fmtHL(s.wunder)}</td>
              <td class="value-num">${fmtHL(s.open_meteo)}</td>
              <td class="value-num">${fmtHL(s.open_meteo_ecmwf)}</td>
              <td class="value-num">${fmtHL(s.open_meteo_gfs)}</td>
              <td class="value-num">${fmtHL(s.open_meteo_icon)}</td>
              <td class="value-num">${fmtHL(s.noaa)}</td>
              <td class="value-num">${fmtHL(s.met_no)}</td>
            </tr>`;
          }).join('')}
        </tbody>
      </table>
    </div>
  `).join('');
}

// =====================
// Tab 2: Observations
// =====================
// 哪些日期块当前是展开的——key 形如 `KLGA|2026-06-04`，跨 SSE 重渲染保持状态
const expandedSections = new Set();

function renderObservations(root, data) {
  if (!data.cities || data.cities.length === 0) {
    root.innerHTML = '<div class="empty">no cities configured</div>';
    return;
  }
  // 城市卡片 → 日期块 → settlement/metar/wethr 并排 + 可展开明细
  root.innerHTML = data.cities.map(({ city, local_now, settlement, metar, wethr }) => {
    const w = wethr || { today: null, yesterday: null };
    const todayDate = (settlement.today?.date) || (metar.today?.date) || (w.today?.date) || '';
    const yesterdayDate = (settlement.yesterday?.date) || (metar.yesterday?.date) || (w.yesterday?.date) || '';
    return `
    <div class="card" data-station="${escape(city.station)}" data-name="${escape((city.name || '').toLowerCase())}">
      <div class="card-header">
        <div class="card-title">
          ${escape(city.name)} <small>${escape(city.station)} · ${escape(city.unit)} · local ${escape(local_now)}</small>
        </div>
      </div>
      ${dateBlock(city.station, 'today', todayDate, city.unit, city.tz, settlement.today, metar.today, w.today)}
      ${dateBlock(city.station, 'yesterday', yesterdayDate, city.unit, city.tz, settlement.yesterday, metar.yesterday, w.yesterday)}
    </div>`;
  }).join('');
}

/**
 * 一个日期块：可点击 header + 摘要表（settlement / metar 各一行）+ 隐藏的明细面板
 * yesterday 默认整段折叠（仅显示标题），用户点击展开 summary + detail；today 默认 summary 可见，
 * detail 仍在 pane 里通过同一个点击切换
 */
function dateBlock(station, when, dateStr, unit, tz, settlementData, metarData, wethrData) {
  const label = dateStr ? `${dateStr} · ${when}` : when;
  const stateKey = `${station}|${when}|${dateStr || ''}`;
  const userExpanded = expandedSections.has(stateKey);
  const yesterdayCollapsed = when === 'yesterday' && !userExpanded;
  const caret = userExpanded ? '▼' : '▶';
  const paneHidden = !userExpanded;
  const sectionCls = [
    'date-section',
    when === 'yesterday' ? 'is-yesterday' : 'is-today',
    yesterdayCollapsed ? 'is-yesterday-collapsed' : ''
  ].filter(Boolean).join(' ');
  const hintText = yesterdayCollapsed
    ? '点击展开 yesterday'
    : (userExpanded ? '点击折叠' : '点击查看 detail');

  // 只在 wethr 真有数据时渲染那一行，国际站直接跳过
  const hasWethr = wethrData && (wethrData.high !== undefined && wethrData.high !== '' && wethrData.high !== null);
  const detailHasWethr = wethrData && Array.isArray(wethrData.detail) && wethrData.detail.length > 0;
  const detailGridCls = (hasWethr || detailHasWethr) ? 'obs-detail-pane has-wethr' : 'obs-detail-pane';

  return `
    <div class="${sectionCls}">
      <div class="date-section-header" onclick="toggleSection(this, '${escape(stateKey)}', '${escape(when)}')">
        <span class="caret">${caret}</span>
        <span class="date-label">${escape(label)}</span>
        <small class="hint">${hintText}</small>
      </div>
      <table class="obs-summary">
        <thead>
          <tr>
            <th>Source</th>
            <th>High <small>@ city local · seen 你的本地</small></th>
            <th>Low <small>@ city local · seen 你的本地</small></th>
            <th>Latest <small>@ city local · seen 你的本地</small></th>
            <th>Obs#</th>
          </tr>
        </thead>
        <tbody>
          ${obsRow('settlement', unit, tz, settlementData)}
          ${obsRow('metar', unit, tz, metarData)}
          ${hasWethr ? obsRow('wethr', unit, tz, wethrData) : ''}
        </tbody>
      </table>
      <div class="${detailGridCls}" ${paneHidden ? 'hidden' : ''}>
        ${detailSection('settlement', unit, tz, settlementData)}
        ${detailSection('metar', unit, tz, metarData)}
        ${(hasWethr || detailHasWethr) ? detailSection('wethr', unit, tz, wethrData) : ''}
      </div>
    </div>
  `;
}

/** 一个 high/low/latest 单元格：温度 + 观测时间(城市本地) + 我们首次看到的时间(你本地) */
function obsCell(value, unit, obsTs, firstSeenAt, tz) {
  if (value === null || value === undefined || value === '') return '<span class="dash">—</span>';
  const obsTime = obsTs ? fmtCityTimeSec(Number(obsTs), tz) : '—';
  const seen = firstSeenAt ? fmtTs(firstSeenAt) : '—';
  return `<div class="obs-cell">
    <span class="obs-temp">${escape(value)}°${escape(unit)}</span>
    <small class="obs-meta">@ ${escape(obsTime)} <span class="tz-tag">city</span></small>
    <small class="obs-meta">seen ${escape(seen)} <span class="tz-tag">you</span></small>
  </div>`;
}

function obsRow(type, unit, tz, d) {
  const tag = `<span class="tag tag-${type}">${type}</span>`;
  if (!d || d.high === undefined || d.high === '' || d.high === null) {
    return `<tr><td>${tag}</td><td colspan="4" class="dash">no data</td></tr>`;
  }
  return `<tr>
    <td>${tag}</td>
    <td>${obsCell(d.high, unit, d.high_obs_ts, d.high_first_seen_at, tz)}</td>
    <td>${obsCell(d.low, unit, d.low_obs_ts, d.low_first_seen_at, tz)}</td>
    <td>${obsCell(d.latest_temp, unit, d.latest_obs_ts, d.latest_first_seen_at, tz)}</td>
    <td class="value-num">${escape(d.obs_count || 0)}</td>
  </tr>`;
}

/** 展开后的单个 source 的全量观测列表 */
function detailSection(type, unit, tz, d) {
  if (!d || !d.detail || d.detail.length === 0) {
    return `<div class="detail-block">
      <div class="detail-title"><span class="tag tag-${type}">${type}</span></div>
      <div class="dash" style="padding:8px">no observation detail</div>
    </div>`;
  }
  const high = d.high !== undefined && d.high !== '' ? Number(d.high) : null;
  const low  = d.low  !== undefined && d.low  !== '' ? Number(d.low)  : null;
  const rows = d.detail.slice().reverse().map(o => {
    const isHi = high !== null && o.temp === high;
    const isLo = low  !== null && o.temp === low;
    const cls = isHi ? 'detail-row-high' : (isLo ? 'detail-row-low' : '');
    return `<tr class="${cls}">
      <td>${fmtCityTimeSec(o.ts, tz)}</td>
      <td class="value-num">${escape(o.temp)}°${escape(unit)}</td>
      <td>${isHi ? '<span class="mini-tag mini-tag-high">HIGH</span>' : (isLo ? '<span class="mini-tag mini-tag-low">LOW</span>' : '')}</td>
    </tr>`;
  }).join('');

  return `<div class="detail-block">
    <div class="detail-title">
      <span class="tag tag-${type}">${type}</span>
      <small>${d.detail.length} observations · city local time · newest first</small>
    </div>
    <table class="detail-table">
      <thead><tr><th>Obs Time</th><th>Temp</th><th></th></tr></thead>
      <tbody>${rows}</tbody>
    </table>
  </div>`;
}

function toggleSection(headerEl, stateKey, when) {
  const section = headerEl.parentElement;
  const pane = section.querySelector('.obs-detail-pane');
  const caret = headerEl.querySelector('.caret');
  const hint = headerEl.querySelector('.hint');

  // Yesterday 折叠状态 → 整体展开（summary + detail 一起亮）
  if (section.classList.contains('is-yesterday-collapsed')) {
    section.classList.remove('is-yesterday-collapsed');
    if (pane) pane.hidden = false;
    expandedSections.add(stateKey);
    if (caret) caret.textContent = '▼';
    if (hint) hint.textContent = '点击折叠';
    return;
  }

  // 今天 或 已展开的 yesterday：切换 detail
  if (pane.hidden) {
    pane.hidden = false;
    caret.textContent = '▼';
    if (hint) hint.textContent = '点击折叠';
    expandedSections.add(stateKey);
  } else {
    pane.hidden = true;
    caret.textContent = '▶';
    if (hint) hint.textContent = '点击查看 detail';
    expandedSections.delete(stateKey);
    // yesterday 收起 detail 时同时把整段重新折回
    if (when === 'yesterday') {
      section.classList.add('is-yesterday-collapsed');
      if (hint) hint.textContent = '点击展开 yesterday';
    }
  }
}
window.toggleSection = toggleSection;

// =====================
// Tab 3: NPM Pricing
// =====================
function renderNPM(root, data) {
  if (!data.companies || data.companies.length === 0) {
    root.innerHTML = '<div class="empty">no companies configured (set poly:config:companies)</div>';
    return;
  }
  root.innerHTML = data.companies.map(({ config, state }) => {
    const s = state || {};
    const iv = s.current_iv ? Number(s.current_iv) : null;
    const prevIv = s.prev_iv ? Number(s.prev_iv) : null;
    let deltaHtml = '<span class="dash">—</span>';
    if (iv !== null && prevIv !== null) {
      const d = iv - prevIv;
      const pct = prevIv !== 0 ? (d / prevIv * 100) : 0;
      const cls = d > 0 ? 'delta-up' : (d < 0 ? 'delta-down' : 'delta-flat');
      const sign = d > 0 ? '+' : '';
      deltaHtml = `<span class="${cls}">${sign}${(d / 1e9).toFixed(2)}B (${sign}${pct.toFixed(2)}%)</span>`;
    }
    const sourceTag = s.current_source
      ? `<span class="tag" style="background:rgba(139,148,158,0.2);color:#8b949e">${escape(s.current_source)}</span>`
      : '';
    return `
    <div class="card" data-company="${escape(config.id)}">
      <div class="card-header">
        <div class="card-title">
          ${escape(s.name || config.name || config.id)}
          <small>${escape(config.id)}</small>
        </div>
        ${sourceTag}
      </div>
      <div class="npm-grid">
        <div>
          <div class="npm-iv-big">${fmtIv(iv)}</div>
          <div class="npm-sub">current · ${escape(s.current_date || '—')}</div>
        </div>
        <div>
          <div class="npm-cell-label">Price / Share</div>
          <div class="npm-cell-value">${s.current_price !== undefined && s.current_price !== '' ? '$' + Number(s.current_price).toFixed(2) : '—'}</div>
        </div>
        <div>
          <div class="npm-cell-label">Δ vs Prev</div>
          <div class="npm-cell-value">${deltaHtml}</div>
          <div class="npm-sub">prev ${fmtIv(prevIv)} on ${escape(s.prev_date || '—')}</div>
        </div>
        <div>
          <div class="npm-cell-label">Last Check</div>
          <div class="npm-cell-value" style="font-size:12px">${fmtTs(s.last_check_at)}</div>
        </div>
      </div>
    </div>`;
  }).join('');
}

// =====================
// 数据加载
// =====================
const RENDERERS = {
  forecast: renderForecast,
  observations: renderObservations,
  npm: renderNPM,
};

async function loadTab(name) {
  try {
    const resp = await fetch('/api/' + name);
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();
    const root = document.getElementById(name + '-content');
    RENDERERS[name](root, data);
    setUpdated(name);
    // 渲染后重新应用当前 filter
    applyFilter(name);
    // 闪一下当前激活的 tab，表示刚刷新
    const tab = document.getElementById('tab-' + name);
    if (tab && tab.classList.contains('active')) flash(tab);
  } catch (e) {
    const root = document.getElementById(name + '-content');
    if (root) root.innerHTML = `<div class="empty" style="color:#f85149">error: ${escape(e.message)}</div>`;
  }
}

// =====================
// 城市过滤
// =====================
function applyFilter(tabName) {
  const input = document.querySelector(`.city-filter[data-tab="${tabName}"]`);
  if (!input) return;
  const q = (input.value || '').trim().toLowerCase();
  const root = document.getElementById(tabName + '-content');
  if (!root) return;
  const cards = root.querySelectorAll('.card');
  let shown = 0;
  for (const c of cards) {
    if (!q) {
      c.classList.remove('hidden-by-filter');
      shown++;
      continue;
    }
    const station = (c.dataset.station || '').toLowerCase();
    const name = (c.dataset.name || '').toLowerCase();
    if (station.includes(q) || name.includes(q)) {
      c.classList.remove('hidden-by-filter');
      shown++;
    } else {
      c.classList.add('hidden-by-filter');
    }
  }
  // 清掉之前的 no-match 提示
  const oldEmpty = root.querySelector('.no-match');
  if (oldEmpty) oldEmpty.remove();
  if (q && shown === 0) {
    const div = document.createElement('div');
    div.className = 'no-match';
    div.textContent = `no city matches "${q}"`;
    root.appendChild(div);
  }
}

document.querySelectorAll('.city-filter').forEach(input => {
  input.addEventListener('input', () => applyFilter(input.dataset.tab));
});

// 在 legend 里显示用户的时区
const userTzEl = document.getElementById('user-tz');
if (userTzEl) userTzEl.textContent = `(${USER_TZ})`;

// 首次加载所有 Tab（即使隐藏也加载，切到时直接显示）
loadTab('forecast');
loadTab('observations');
loadTab('npm');

// =====================
// 通知：底部走马灯 + 右上角醒目 toast
// =====================
const tickerBar = document.getElementById('ticker-bar');
const toastStack = document.getElementById('toast-stack');

// 走马灯节流：用元素实际宽度算出"上一条完全清出右边后再放下一条"的最小间隔
// 公式：item 的右边缘移出 viewport 需要 (animDur × itemW / (vw + itemW)) ms
// 加 200ms 安全缓冲；外加 TICKER_MIN_GAP_MS 保底（窄 item 也不至于一闪而过）
const TICKER_ANIM_DURATION_MS = 18000;  // 必须与 CSS keyframe ticker-slide 一致
const TICKER_MIN_GAP_MS = 1500;          // 最小间隔保底
const TICKER_BUFFER_MS = 200;            // 视觉缓冲
const TICKER_MAX_DELAY_MS = 30000;       // 排队 >30s 丢弃
let tickerNextStartAt = 0;

function addTickerItem(cls, html) {
  if (!tickerBar) return;
  const now = Date.now();
  const desiredStart = Math.max(now, tickerNextStartAt);
  const delayMs = desiredStart - now;
  if (delayMs > TICKER_MAX_DELAY_MS) return;

  const el = document.createElement('div');
  el.className = 'ticker-item ' + (cls || '');
  el.innerHTML = html;
  if (delayMs > 0) el.style.animationDelay = `${delayMs}ms`;
  tickerBar.appendChild(el);
  el.addEventListener('animationend', () => el.remove(), { once: true });

  // 测量实际宽度，按"上一条 item 右边缘完全离开右边界"算下一条最早出场时间
  const w = el.offsetWidth;
  const vw = window.innerWidth || 1400;
  const clearMs = TICKER_ANIM_DURATION_MS * w / (vw + w);
  const gap = Math.max(TICKER_MIN_GAP_MS, clearMs + TICKER_BUFFER_MS);
  tickerNextStartAt = desiredStart + gap;
}

function addToast(opts) {
  if (!toastStack) return;
  const el = document.createElement('div');
  el.className = 'toast' + (opts.cls ? ' ' + opts.cls : '');
  el.innerHTML = `
    <div class="toast-row1">${escape(opts.row1)}</div>
    <div class="toast-row2">${opts.row2 /* trusted html, already escaped upstream */}</div>
    ${opts.row3 ? `<div class="toast-row3">${escape(opts.row3)}</div>` : ''}
  `;
  toastStack.appendChild(el);
  const ttl = opts.ttl || 9000;
  let t = setTimeout(removeToast, ttl);
  function removeToast() {
    el.classList.add('removing');
    el.addEventListener('animationend', () => el.remove(), { once: true });
  }
  el.addEventListener('click', () => { clearTimeout(t); removeToast(); });
}

// wethr 走马灯三重 gate：
// 1) isNewHigh/isNewLow 判定（必备）
// 2) per-station 30s debounce（吸收传感器 90→91→92 连跳，只让第一条进 ticker）
// 3) 幅度阈值 Δ ≥ 1°F（rounding-flap 直接吃掉）
const WETHR_DEBOUNCE_MS = 30_000;
const WETHR_THRESHOLD = 1;  // 1°F 或 1°C，按 unit 量纲一致
const wethrTickerLast = new Map();  // station|kind -> ts

function handleWeatherObsUpdate(u) {
  const high = u.high === null || u.high === undefined ? null : Number(u.high);
  const prevHigh = u.prev_high === null || u.prev_high === undefined ? null : Number(u.prev_high);
  const low = u.low === null || u.low === undefined ? null : Number(u.low);
  const prevLow = u.prev_low === null || u.prev_low === undefined ? null : Number(u.prev_low);

  const isNewHigh = prevHigh !== null && high !== null && high > prevHigh;
  const isNewLow  = prevLow  !== null && low  !== null && low  < prevLow;

  if (!isNewHigh && !isNewLow) return;

  // wethr 三重 gate：高低**分别**判定（避免极端情况同时新高新低时低温被吃）
  const checkWethrGate = (kind, delta) => {
    if (u.type !== 'wethr') return true;
    const dKey = `${u.station}|${kind}`;
    const lastTs = wethrTickerLast.get(dKey) || 0;
    if (Date.now() - lastTs < WETHR_DEBOUNCE_MS) return false;
    if (delta < WETHR_THRESHOLD) return false;
    wethrTickerLast.set(dKey, Date.now());
    return true;
  };
  const showHigh = isNewHigh && checkWethrGate('high', high - prevHigh);
  const showLow  = isNewLow  && checkWethrGate('low',  prevLow - low);
  if (!showHigh && !showLow) return;

  const city = `${escape(u.name || u.station)} <small>(${escape(u.station)} · ${escape(u.type)})</small>`;
  const u_ = escape(u.unit || '');

  // 高温和低温独立展示（同一 update 可能两个都触发）
  if (showHigh) {
    addTickerItem(
      'ticker-item-high',
      `<span class="ticker-icon">🔥 NEW HIGH</span><span class="ticker-city">${city}</span> · <span class="ticker-info">high <strong>${escape(prevHigh)}° → ${escape(high)}°${u_}</strong></span>`
    );
    addToast({
      cls: '',
      row1: `🔥 NEW HIGH · ${u.type}`,
      row2: `${escape(u.name || u.station)} <span style="opacity:0.7">(${escape(u.station)})</span>`,
      row3: `${prevHigh}°${u.unit} → ${high}°${u.unit} · ${u.date}`,
      ttl: 12000,
    });
  }
  if (showLow) {
    addTickerItem(
      'ticker-item-low',
      `<span class="ticker-icon">❄️ NEW LOW</span><span class="ticker-city">${city}</span> · <span class="ticker-info">low <strong>${escape(prevLow)}° → ${escape(low)}°${u_}</strong></span>`
    );
    addToast({
      cls: 'toast-low',
      row1: `❄️ NEW LOW · ${u.type}`,
      row2: `${escape(u.name || u.station)} <span style="opacity:0.7">(${escape(u.station)})</span>`,
      row3: `${prevLow}°${u.unit} → ${low}°${u.unit} · ${u.date}`,
      ttl: 8000,
    });
  }
}

// NPM / Forecast 更新当前不进 ticker（仅触发 Tab 刷新）
// 想要回来时，把对应 ticker 调用加回这里即可
function handleNpmUpdate(_u) { /* noop */ }
function handleForecastUpdate() { /* noop */ }

function parseSseData(raw) {
  try { return JSON.parse(raw); } catch { return null; }
}

// observations Tab reload 节流：多条 SSE（weather_obs / wethr_obs）合并一次拉
// 2s debounce 兼顾响应感和不刷屏
const OBS_RELOAD_DEBOUNCE_MS = 2_000;
let obsReloadTimer = null;
function scheduleObservationsReload() {
  if (obsReloadTimer) return;
  obsReloadTimer = setTimeout(() => {
    obsReloadTimer = null;
    loadTab('observations');
  }, OBS_RELOAD_DEBOUNCE_MS);
}

// =====================
// SSE：服务端推变化，触发刷新 + 通知
// =====================
const sseEl = document.getElementById('sse-status');
function connectSSE() {
  const evt = new EventSource('/events');
  evt.addEventListener('ready', () => {
    sseEl.textContent = 'live';
    sseEl.className = 'connected';
  });
  evt.onerror = () => {
    sseEl.textContent = 'disconnected, retrying…';
    sseEl.className = 'disconnected';
  };
  // 两个频道都走同一个 reload 节流——否则 wethr 节流形同虚设：
  // wethr 5s 内若 metar 也推一条，metar handler 会立即 loadTab，把 wethr 状态顺带刷出来
  evt.addEventListener('weather_obs', (e) => {
    const msg = parseSseData(e.data);
    if (msg?.data?.updates) for (const u of msg.data.updates) handleWeatherObsUpdate(u);
    scheduleObservationsReload();
  });
  evt.addEventListener('wethr_obs', (e) => {
    const msg = parseSseData(e.data);
    if (msg?.data?.updates) for (const u of msg.data.updates) handleWeatherObsUpdate(u);
    scheduleObservationsReload();
  });
  evt.addEventListener('npm_pricing', (e) => {
    const msg = parseSseData(e.data);
    if (msg?.data?.updates) for (const u of msg.data.updates) handleNpmUpdate(u);
    loadTab('npm');
  });
  evt.addEventListener('weather_forecast_4days', () => {
    handleForecastUpdate();
    loadTab('forecast');
  });
  return evt;
}
connectSSE();
