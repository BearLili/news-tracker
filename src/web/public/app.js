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
const escape = s => String(s ?? '').replace(/[&<>"]/g, c => ({
  '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;'
}[c]));

const fmtTs = (ms) => {
  if (!ms) return '—';
  const d = new Date(Number(ms));
  if (isNaN(d.getTime())) return '—';
  return d.toLocaleString();
};

const fmtTsSec = (sec) => fmtTs(sec ? sec * 1000 : null);

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
function renderObservations(root, data) {
  if (!data.cities || data.cities.length === 0) {
    root.innerHTML = '<div class="empty">no cities configured</div>';
    return;
  }
  // 按"城市 → 日期 → 源"分组：每个城市卡片内部，今天和昨天各一个块，
  // 块内 settlement 和 metar 上下并排，方便对比同日两路数据
  root.innerHTML = data.cities.map(({ city, local_now, settlement, metar }) => {
    // 从两路数据里捞出实际日期字符串（settlement / metar 都用同一对 today/yesterday）
    const today = (settlement.today?.date) || (metar.today?.date) || '';
    const yesterday = (settlement.yesterday?.date) || (metar.yesterday?.date) || '';

    return `
    <div class="card" data-station="${escape(city.station)}" data-name="${escape((city.name || '').toLowerCase())}">
      <div class="card-header">
        <div class="card-title">
          ${escape(city.name)} <small>${escape(city.station)} · ${escape(city.unit)} · local ${escape(local_now)}</small>
        </div>
      </div>
      <table>
        <thead>
          <tr>
            <th>Date</th>
            <th>Source</th>
            <th>High</th>
            <th>Low</th>
            <th>Obs#</th>
            <th>First Obs</th>
            <th>Last Obs</th>
            <th>First Seen</th>
          </tr>
        </thead>
        <tbody>
          ${dateBlock('today', today, city.unit, settlement.today, metar.today)}
          ${dateBlock('yesterday', yesterday, city.unit, settlement.yesterday, metar.yesterday)}
        </tbody>
      </table>
    </div>`;
  }).join('');
}

/**
 * 一个日期块：表头 divider + 同日 settlement / metar 两行
 */
function dateBlock(when, dateStr, unit, settlementData, metarData) {
  const label = dateStr ? `${dateStr} · ${when}` : when;
  return `
    <tr class="date-divider"><td colspan="8">${escape(label)}</td></tr>
    ${obsRow('settlement', unit, settlementData)}
    ${obsRow('metar', unit, metarData)}
  `;
}

function obsRow(type, unit, d) {
  const tag = `<span class="tag tag-${type}">${type}</span>`;
  if (!d || Object.keys(d).length === 0) {
    return `<tr><td class="dash">—</td><td>${tag}</td><td colspan="6" class="dash">no data</td></tr>`;
  }
  const lastObs = d.last_obs_ts ? fmtTsSec(Number(d.last_obs_ts)) : '—';
  const firstObs = d.first_obs_ts ? fmtTsSec(Number(d.first_obs_ts)) : '—';
  const firstSeen = d.first_seen_at ? fmtTs(d.first_seen_at) : '—';
  return `<tr>
    <td></td>
    <td>${tag}</td>
    <td class="value-num">${escape(d.high)}°${escape(unit)}</td>
    <td class="value-num">${escape(d.low)}°${escape(unit)}</td>
    <td class="value-num">${escape(d.obs_count)}</td>
    <td>${firstObs}</td>
    <td>${lastObs}</td>
    <td>${firstSeen}</td>
  </tr>`;
}

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

// 首次加载所有 Tab（即使隐藏也加载，切到时直接显示）
loadTab('forecast');
loadTab('observations');
loadTab('npm');

// =====================
// SSE：服务端推变化，前端按 channel 刷新对应 Tab
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
  // 收到 collector 推送时，重新拉对应 API（保持渲染逻辑统一）
  evt.addEventListener('weather_obs', () => loadTab('observations'));
  evt.addEventListener('npm_pricing', () => loadTab('npm'));
  evt.addEventListener('weather_forecast_4days', () => loadTab('forecast'));
  return evt;
}
connectSSE();
