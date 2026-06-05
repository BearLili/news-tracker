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
    <div class="card" data-station="${escape(city.station)}">
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
  root.innerHTML = data.cities.map(({ city, local_now, settlement, metar }) => `
    <div class="card" data-station="${escape(city.station)}">
      <div class="card-header">
        <div class="card-title">
          ${escape(city.name)} <small>${escape(city.station)} · ${escape(city.unit)} · local ${escape(local_now)}</small>
        </div>
      </div>
      <table>
        <thead>
          <tr>
            <th>Source</th>
            <th>Date</th>
            <th>High</th>
            <th>Low</th>
            <th>Obs#</th>
            <th>Last Obs</th>
          </tr>
        </thead>
        <tbody>
          ${obsRow('settlement', city.unit, settlement.yesterday)}
          ${obsRow('settlement', city.unit, settlement.today)}
          ${obsRow('metar', city.unit, metar.yesterday)}
          ${obsRow('metar', city.unit, metar.today)}
        </tbody>
      </table>
    </div>
  `).join('');
}

function obsRow(type, unit, d) {
  const tag = `<span class="tag tag-${type}">${type}</span>`;
  if (!d || Object.keys(d).length === 0) {
    return `<tr><td>${tag}</td><td colspan="5" class="dash">no data</td></tr>`;
  }
  const lastObs = d.last_obs_ts ? fmtTsSec(Number(d.last_obs_ts)) : '—';
  return `<tr>
    <td>${tag}</td>
    <td>${escape(d.date)}</td>
    <td class="value-num">${escape(d.high)}°${escape(unit)}</td>
    <td class="value-num">${escape(d.low)}°${escape(unit)}</td>
    <td class="value-num">${escape(d.obs_count)}</td>
    <td>${lastObs}</td>
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
    // 闪一下当前激活的 tab，表示刚刷新
    const tab = document.getElementById('tab-' + name);
    if (tab && tab.classList.contains('active')) flash(tab);
  } catch (e) {
    const root = document.getElementById(name + '-content');
    if (root) root.innerHTML = `<div class="empty" style="color:#f85149">error: ${escape(e.message)}</div>`;
  }
}

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
