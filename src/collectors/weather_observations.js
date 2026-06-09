import axios from 'axios';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import timezone from 'dayjs/plugin/timezone.js';
import pLimit from 'p-limit';
import BaseCollector from '../core/BaseCollector.js';
import redis from '../core/redis.js';
import logger from '../utils/logger.js';
import WethrPushClient from './wethrPushClient.js';

dayjs.extend(utc);
dayjs.extend(timezone);

// IBM/TWC API key（与 Wunderground 网站客户端公开使用的相同）—— 用于 settlement 拉取历史 METAR
const TWC_API_KEY = 'e1f10a1e78da46f5b10a1e78da96f525';

// aviationweather.gov METAR 接口（限 100 req/min，1 req/min/thread/endpoint）
const METAR_BASE = 'https://aviationweather.gov/api/data/metar';

/**
 * Weather Observations 采集器（结算 + 预记录二合一）
 *
 * 服务两类下游观测数据：
 *
 * 1) Settlement（结算数据，"最稳"）
 *    源 : api.weather.com historical METAR（TWC，与 Polymarket 裁决同源）
 *    频率: 每 10 分钟全城市跑一遍
 *    用途: Polymarket 裁决基准
 *
 * 2) METAR 预记录（pre-record，"近实时但可能漏"）
 *    源 : aviationweather.gov /api/data/metar (NOAA 官方 METAR REST API)
 *    频率: 每 60 秒一轮，一次性 batch 拉所有站点（ids=KLGA,KSFO,...）
 *    用途: 高频检测新 METAR 发布、为策略提供"未结算但已观测"的最新值
 *    注意: METAR 接口 100 req/min，batch 后等于 1 req/分钟，留出很大余量
 *
 * Redis schema:
 *   poly:settlement:{station}:{date}  (hash) - 结算（与老 weather_settlement 兼容）
 *   poly:metar:{station}:{date}       (hash) - 预记录
 *     字段都包含: station, name, date, unit, source, high, low, obs_count,
 *                last_obs_ts, updated_at  (METAR 多一个 last_raw)
 *   TTL 14 天
 *
 * Pub/Sub:
 *   poly:feed:weather_obs
 *     payload = { source, ts, data:{ updates:[{ type, station, date, high, low,
 *               prev_high, prev_low, obs_count, prev_count, last_obs_ts, unit, ... }]} }
 *   只在 (high|low|obs_count) 真的变化时发布。
 */
class WeatherObservationsCollector extends BaseCollector {
    constructor() {
        // 基础 tick = 3s。METAR 按"出生窗口"分热/冷：
        //   - 城市的 sliceMinute ~ sliceMinute+8min 期间 + 本小时还没抓到 → hot 每 tick (3s)
        //   - 已抓到本 hot 窗口对应 obs，或者不在窗口 → cold 每 5 tick (15s)
        super('weather_observations', 3 * 1000);
        this.targets = [];
        this.tick = 0;
        // Settlement 每 40 tick 跑一次 = 40 × 3s = 120s = 2 分钟
        this.settlementEveryTicks = 40;
        this.settlementOffset = 1;
        this.metarConcurrency = 4;
        this.settlementConcurrency = 8;
        // METAR 热/冷配置
        this.metarHotWindowMin = 8;        // 切片后 8 分钟内
        this.metarColdEveryTicks = 5;      // cold = 5 × 3s = 15s
        this.metarLastPolled = new Map();  // chunkIdx -> last tick number
        // 站点 sliceMinute 自动探测：观察 METAR obsTime 的分钟分布
        this.detectedSlices = new Map();   // station -> [m1, m2?]（半小时报有两个）
        this.lastFreshObs = new Map();     // station -> 最近一次见到的 obsTime (unix sec)
        this.proxyPool = [];
        this.wethrClient = null;
        this.wethrTargets = [];
        this.wethrBuffer = new Map();
        this.wethrConfigReady = false;
    }

    sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
    randInt(a, b) { return Math.floor(Math.random() * (b - a + 1)) + a; }
    cToF(c) { return Math.round(c * 9 / 5 + 32); }

    toLocalDate(unixSec, tz) {
        return dayjs.unix(unixSec).tz(tz).format('YYYY-MM-DD');
    }

    async loadCitiesConfig() {
        try {
            const raw = await redis.hgetall('poly:config:cities');
            const all = Object.values(raw).map(s => {
                try { return JSON.parse(s); } catch { return null; }
            }).filter(c => c && c.fetchData === true && c.station);
            // tz 缺失会让 dayjs().tz() 回退到 process tz 导致静默日期错；
            // unit 缺失会让温度单位无从转换。两者必备。
            const valid = [];
            for (const c of all) {
                if (!c.tz || !c.unit) {
                    logger.warn(`⚠️ [Obs] city ${c.station} skipped: missing tz/unit`);
                    continue;
                }
                valid.push(c);
            }
            this.targets = valid;
            logger.debug(`📋 [Obs] loaded ${this.targets.length} cities`);
        } catch (e) {
            logger.error(`❌ [Obs] config error: ${e.message}`);
            this.targets = [];
        }
    }

    async getAxiosProxy() {
        try {
            const s = await redis.srandmember('poly:proxylist');
            if (!s) return null;
            return this._parseProxy(s);
        } catch { return null; }
    }

    _parseProxy(s) {
        const p = s.split(':');
        if (p.length < 2) return null;
        return {
            protocol: 'http',
            host: p[0],
            port: parseInt(p[1]),
            auth: (p[2] && p[3]) ? { username: p[2], password: p[3] } : undefined
        };
    }

    /** 拉全部 proxy 列表（不是随机一个），用于轮转分配给并发请求 */
    async loadProxyPool() {
        try {
            const list = await redis.smembers('poly:proxylist');
            this.proxyPool = list.map(s => this._parseProxy(s)).filter(Boolean);
        } catch (e) {
            logger.warn(`[Obs] loadProxyPool failed: ${e.message}`);
            this.proxyPool = [];
        }
        return this.proxyPool;
    }

    // =====================
    // METAR (pre-record)
    // =====================

    /**
     * Batch 拉取所有站点的 METAR，最近 48 小时
     *
     * 48h 是数学下限：任何时区"昨天 00:00 local"距当前 UTC 最多 24h(今天已过) + 24h(昨天) = 48h。
     * 所以 hours=48 能完整覆盖任意时区的"今天+昨天"，再多就是浪费。
     *
     * 关键细节：aviationweather.gov 单次响应硬上限是 400 条 entries。
     * 一个城市 48h 平均 ~48 条（每小时一条），坏天气 + SPECI 异常报可飙到 ~95
     * （实测 RKSI/EGLC/ZBAA 在 72h 拿 ~143，按比例 48h 约 95）。
     * 取 chunk size = 3：3 × ~95 max ≈ 285 条，给 SPECI 突发留 >100 条余量。
     *
     * 把站点切成多个 chunk 串行发请求；与 ratelimit 的关系：
     *   假设 20 城市 → 7 chunks per cycle，10s 一轮 → 42 req/min « 100/min 上限。
     *
     * 返回 Map<icaoId, [{obsTime, temp, raw}, ...]>
     */
    async fetchMetarBatch(stations) {
        const CHUNK_SIZE = 3;  // 3 站 × 48h max~95/站 ≈ 285 条，远低于 400 上限

        // 把 stations 切成 chunks 列表
        const chunkList = [];
        for (let i = 0; i < stations.length; i += CHUNK_SIZE) {
            chunkList.push(stations.slice(i, i + CHUNK_SIZE));
        }

        // 每个 batch 随机洗牌 proxy 池：避免某个 chunk 永远命中同一个慢 proxy
        const proxies = this.proxyPool.slice();
        for (let i = proxies.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [proxies[i], proxies[j]] = [proxies[j], proxies[i]];
        }

        // 热/冷判定：站点 sliceMinute (UTC) ~ sliceMinute+8min 为"出生窗口"
        // 三个数据源排序：cities 配置 > 自动探测 > 未知（视为 hot 给探测机会）
        // 额外优化：若已抓到本窗口对应 obs（lastFreshObs.minute ≈ slice 且时间足够近），降回 cold
        const nowMinute = new Date().getUTCMinutes();
        const nowSec = Math.floor(Date.now() / 1000);
        // 抓到判定的"足够近"= 热窗口长度 + 2min 容差
        const HOT_GRACE_SEC = (this.metarHotWindowMin + 2) * 60;
        const targetByStation = new Map(this.targets.map(t => [t.station, t]));

        // 获取站点的 slice minute 列表（优先 config，回退到自动探测）
        const getSlices = (station) => {
            const t = targetByStation.get(station);
            if (!t) return null;
            const rawCfg = t.sliceMinute;
            if (rawCfg !== undefined && rawCfg !== null && rawCfg !== '') {
                const n = Number(rawCfg);
                if (!isNaN(n) && n >= 0 && n <= 59) return [n];
            }
            const detected = this.detectedSlices.get(station);
            if (detected && detected.length > 0) return detected;
            return null;
        };

        const isStationHot = (station) => {
            const slices = getSlices(station);
            // 完全未知 sliceMinute 的站点：暂时视为 hot，让本 tick 拉到数据 → 后续 detect
            if (!slices) return true;
            // 检查当前是否处于任何一个 slice 的"出生窗口"
            let activeSlice = null;
            for (const s of slices) {
                const diff = (nowMinute - s + 60) % 60;
                if (diff < this.metarHotWindowMin) { activeSlice = s; break; }
            }
            if (activeSlice === null) return false;
            // 已经在窗口内：检查是否已抓到对应这个 slice 的 obs
            const lastObs = this.lastFreshObs.get(station) || 0;
            if (lastObs > 0) {
                const lastMin = Math.floor(lastObs / 60) % 60;
                const lastAgo = nowSec - lastObs;
                // 最新 obs 分钟跟 activeSlice 对得上（±1 容差），且时间足够近 → 抓到了
                const minDiff = Math.min(
                    Math.abs(lastMin - activeSlice),
                    60 - Math.abs(lastMin - activeSlice)
                );
                if (minDiff <= 1 && lastAgo <= HOT_GRACE_SEC) {
                    return false;  // 抓到了，降 cold
                }
            }
            return true;
        };

        const limit = pLimit(this.metarConcurrency);
        const byStation = new Map();
        let failed = 0;
        let retried = 0;
        let skipped = 0;
        let hotChunks = 0;

        await Promise.all(chunkList.map((chunk, idx) => limit(async () => {
            const isHot = chunk.some(s => isStationHot(s));
            if (isHot) hotChunks++;
            const lastTick = this.metarLastPolled.get(idx) || 0;
            const ticksSinceLast = this.tick - lastTick;
            // 冷 chunk + 还没到拉取间隔 → 跳过本 tick
            if (!isHot && ticksSinceLast < this.metarColdEveryTicks) {
                skipped++;
                return;
            }
            this.metarLastPolled.set(idx, this.tick);
            // 最多尝试 2 次：第 1 次用洗牌后的 proxy[idx]，失败后换 proxy[idx+1]
            let lastErr = null;
            for (let attempt = 0; attempt < 2; attempt++) {
                if (attempt > 0) retried++;
                const proxyConfig = proxies.length > 0
                    ? proxies[(idx + attempt) % proxies.length]
                    : null;
                const proxyTag = proxyConfig ? proxyConfig.host : 'direct';
                const url = `${METAR_BASE}?ids=${chunk.join(',')}&format=json&hours=48&_t=${Date.now()}`;
                try {
                    const resp = await axios.get(url, {
                        timeout: 6000,
                        headers: {
                            'User-Agent': 'news-tracker/1.0 (contact: openclaw-agent)',
                            'Accept': 'application/json'
                        },
                        proxy: proxyConfig || false
                    });
                    if (!Array.isArray(resp.data)) return;
                    for (const e of resp.data) {
                        const id = e?.icaoId;
                        const t = e?.temp;
                        const ts = e?.obsTime;
                        if (!id || typeof t !== 'number' || typeof ts !== 'number') continue;
                        if (!byStation.has(id)) byStation.set(id, []);
                        byStation.get(id).push({ obsTime: ts, temp: t, raw: e.rawOb || '' });
                        // 跟踪每站最新 obsTime，供"抓到即降频"判定用
                        const prev = this.lastFreshObs.get(id) || 0;
                        if (ts > prev) this.lastFreshObs.set(id, ts);
                    }
                    return; // 成功，跳出 retry 循环
                } catch (e) {
                    lastErr = e;
                    if (attempt === 0) {
                        // 第一次失败：debug log 不刷屏，下一次换 proxy 重试
                        logger.debug(`[METAR] chunk ${chunk.join(',')} via ${proxyTag} timeout, retrying`);
                    }
                }
            }
            failed++;
            const status = lastErr?.response?.status;
            logger.warn(`❌ [METAR] chunk ${chunk.join(',')} failed both attempts: ${status || ''} ${lastErr?.message}`);
        })));

        if (failed > 0 || retried > 0 || skipped > 0) {
            logger.debug(`[METAR] tick: ${failed}/${chunkList.length} fail, ${retried} retry, ${skipped} cold-skip, ${hotChunks} hot`);
        }

        // 自动探测 sliceMinute：对没有 config 且尚未 detected 的站点，从本次响应里挖
        for (const target of this.targets) {
            const station = target.station;
            if (this.detectedSlices.has(station)) continue;
            const cfg = target.sliceMinute;
            if (cfg !== undefined && cfg !== null && cfg !== '') continue;  // 已 config
            const obs = byStation.get(station);
            if (obs && obs.length >= 3) this._detectSliceFromObs(station, obs);
        }

        return byStation;
    }

    /**
     * 从一组 obs 的 obsTime 分钟分布探测站点的 sliceMinute
     * 半小时报的站点能识别出两个 slice（top 2 peaks 都 ≥ top 1 的 50%）
     */
    _detectSliceFromObs(station, obsList) {
        const hist = new Map();
        for (const o of obsList) {
            const m = Math.floor(o.obsTime / 60) % 60;
            hist.set(m, (hist.get(m) || 0) + 1);
        }
        if (hist.size === 0) return;
        const sorted = [...hist.entries()].sort((a, b) => b[1] - a[1]);
        const slices = [sorted[0][0]];
        // 半小时报：第二个 peak 至少占第一个的 50%
        if (sorted.length > 1 && sorted[1][1] >= sorted[0][1] * 0.5) {
            slices.push(sorted[1][0]);
        }
        slices.sort((a, b) => a - b);
        this.detectedSlices.set(station, slices);
        const pretty = slices.map(s => ':' + String(s).padStart(2, '0')).join(', ');
        logger.info(`[METAR] auto-detected slice for ${station}: ${pretty}`);
    }

    /**
     * METAR 数据是 Celsius，按 target.unit 转换 + 按 target.tz 聚合到本地日 high/low
     * 仅保留 dates 内的日期（避免远日观测污染）
     */
    // 注意：firstObsTs 语义是"当前 48h 窗口里能见到的最早 obsTime"，
    // 一旦最早那条观测滑出 48h 窗口，firstObsTs 会向后跳——不是"该日历日的首条观测时间"。
    aggregateMetarByDate(obsList, target, validDates) {
        const byDate = {};
        for (const o of obsList) {
            const d = this.toLocalDate(o.obsTime, target.tz);
            if (!validDates.includes(d)) continue;
            if (!byDate[d]) byDate[d] = {
                count: 0,
                highC: -Infinity, highObsTs: 0,
                lowC: +Infinity,  lowObsTs: 0,
                latestObsTs: 0, latestC: null,
                firstObsTs: 0,
                lastRaw: '',
                detail: []
            };
            const slot = byDate[d];
            slot.count++;
            // 严格 > 或同温取最早（避免依赖 TWC/NOAA 返回顺序导致 highObsTs flapping）
            if (o.temp > slot.highC || (o.temp === slot.highC && o.obsTime < slot.highObsTs)) {
                slot.highC = o.temp; slot.highObsTs = o.obsTime;
            }
            if (o.temp < slot.lowC || (o.temp === slot.lowC && o.obsTime < slot.lowObsTs)) {
                slot.lowC = o.temp; slot.lowObsTs = o.obsTime;
            }
            if (o.obsTime > slot.latestObsTs) {
                slot.latestObsTs = o.obsTime;
                slot.latestC = o.temp;
                slot.lastRaw = o.raw || slot.lastRaw;
            }
            if (slot.firstObsTs === 0 || o.obsTime < slot.firstObsTs) {
                slot.firstObsTs = o.obsTime;
            }
            slot.detail.push({ ts: o.obsTime, tempC: o.temp, raw: o.raw || '' });
        }
        const conv = (c) => (c === null || c === -Infinity || c === +Infinity) ? null
            : (target.unit === 'F' ? this.cToF(c) : Math.round(c));
        const result = {};
        for (const [d, info] of Object.entries(byDate)) {
            info.detail.sort((a, b) => a.ts - b.ts);
            result[d] = {
                high: conv(info.highC),
                low:  conv(info.lowC),
                count: info.count,
                highObsTs: info.highObsTs,
                lowObsTs: info.lowObsTs,
                latestTemp: conv(info.latestC),
                latestObsTs: info.latestObsTs,
                firstObsTs: info.firstObsTs,
                lastObsTs: info.latestObsTs,
                lastRaw: info.lastRaw,
                detail: info.detail.map(x => ({ ts: x.ts, temp: conv(x.tempC) }))
            };
        }
        return result;
    }

    async runMetarTick() {
        if (this.targets.length === 0) return [];
        const stations = this.targets.map(t => t.station);

        let byStation;
        try {
            byStation = await this.fetchMetarBatch(stations);
        } catch (e) {
            const status = e?.response?.status;
            logger.warn(`❌ [METAR] batch fetch failed: ${status || ''} ${e.message}`);
            return [];
        }

        const allUpdates = [];
        let activeStations = 0, totalObs = 0;
        for (const target of this.targets) {
            const obs = byStation.get(target.station) || [];
            if (obs.length === 0) continue;
            activeStations++;
            totalObs += obs.length;
            const now = dayjs().tz(target.tz);
            const valid = [now.format('YYYY-MM-DD'), now.subtract(1, 'day').format('YYYY-MM-DD')];
            const byDate = this.aggregateMetarByDate(obs, target, valid);
            const updates = await this.commitObservations(target, byDate, 'metar');
            allUpdates.push(...updates);
        }

        logger.debug(`[METAR] tick: ${activeStations}/${this.targets.length} stations, ${totalObs} obs, ${allUpdates.length} changed`);

        // 简洁打印变化（避免刷屏）
        for (const u of allUpdates) {
            const cntDelta = u.obs_count - (u.prev_count || 0);
            logger.info(
                `📡 [METAR|${u.station}|${u.name}] ${u.date} ` +
                `H=${u.high}°${u.unit}/L=${u.low}° obs=${u.obs_count}(+${cntDelta}) ` +
                `prev=H${u.prev_high ?? '-'}/L${u.prev_low ?? '-'}`
            );
        }
        return allUpdates;
    }

    // =====================
    // Settlement (api.weather.com historical)
    // =====================

    async fetchSettlementOne(target) {
        const country = (target.country || '').toUpperCase();
        if (!country || !target.station) throw new Error('missing country/station');

        const now = dayjs().tz(target.tz);
        const startUtc = now.subtract(2, 'day').utc().format('YYYYMMDD');
        const endUtc = now.add(1, 'day').utc().format('YYYYMMDD');
        const units = target.unit === 'C' ? 'm' : 'e';
        const url = `https://api.weather.com/v1/location/${target.station}:9:${country}/observations/historical.json` +
                    `?apiKey=${TWC_API_KEY}&units=${units}&startDate=${startUtc}&endDate=${endUtc}`;

        let response;
        const maxRetries = 3;
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            const proxyConfig = await this.getAxiosProxy();
            try {
                response = await axios.get(url, {
                    timeout: 15000,
                    headers: {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                    },
                    proxy: proxyConfig || false
                });
                break;
            } catch (e) {
                const status = e?.response?.status;
                const code = e?.code;
                const isProxyErr = status === 407 ||
                    ['ECONNRESET', 'ETIMEDOUT', 'ECONNREFUSED', 'ECONNABORTED', 'EHOSTUNREACH'].includes(code);
                if (!isProxyErr || attempt === maxRetries) throw e;
                logger.debug(`[Settlement|${target.station}] retry ${attempt}/${maxRetries} due to ${status || code}`);
            }
        }

        const obs = response.data?.observations || [];
        const today = now.format('YYYY-MM-DD');
        const yesterday = now.subtract(1, 'day').format('YYYY-MM-DD');
        const valid = [today, yesterday];

        // 聚合逻辑与 METAR 完全对称（拉 → 按本地日过滤 → 找 max/min/latest + 收集 detail）
        // 唯一差异：TWC 返回的 temp 已是目标单位，不需要 cToF 转换
        const byDate = {};
        for (const o of obs) {
            const t = o?.temp;
            const ts = o?.valid_time_gmt;
            if (typeof t !== 'number' || typeof ts !== 'number') continue;
            const d = this.toLocalDate(ts, target.tz);
            if (!valid.includes(d)) continue;
            if (!byDate[d]) byDate[d] = {
                count: 0,
                high: -Infinity, highObsTs: 0,
                low: +Infinity,  lowObsTs: 0,
                latestObsTs: 0, latestTemp: null,
                firstObsTs: 0,
                detail: []
            };
            const slot = byDate[d];
            slot.count++;
            // 严格 > 或同温取最早，避免依赖 TWC 返回顺序
            if (t > slot.high || (t === slot.high && ts < slot.highObsTs)) {
                slot.high = t; slot.highObsTs = ts;
            }
            if (t < slot.low || (t === slot.low && ts < slot.lowObsTs)) {
                slot.low = t; slot.lowObsTs = ts;
            }
            if (ts > slot.latestObsTs) { slot.latestObsTs = ts; slot.latestTemp = t; }
            if (slot.firstObsTs === 0 || ts < slot.firstObsTs) slot.firstObsTs = ts;
            slot.detail.push({ ts, temp: t });
        }
        const result = {};
        for (const [d, info] of Object.entries(byDate)) {
            info.detail.sort((a, b) => a.ts - b.ts);
            result[d] = {
                high: Math.round(info.high),
                low: Math.round(info.low),
                count: info.count,
                highObsTs: info.highObsTs,
                lowObsTs: info.lowObsTs,
                latestTemp: info.latestTemp !== null ? Math.round(info.latestTemp) : null,
                latestObsTs: info.latestObsTs,
                firstObsTs: info.firstObsTs,
                lastObsTs: info.latestObsTs,
                // detail 里 temp 也四舍五入保持一致（settlement 默认整数）
                detail: info.detail.map(x => ({ ts: x.ts, temp: Math.round(x.temp) }))
            };
        }
        return result;
    }

    async runSettlementTick() {
        if (this.targets.length === 0) return [];
        // 并发 settlement，避免 46 城串行跑 4 分钟（实际上根本无法 2min 跑完一轮）。
        // fetchSettlementOne 内部每次 retry 都随机抽 proxy，多并发也会自然摊匀到 proxy 池。
        const limit = pLimit(this.settlementConcurrency);
        let success = 0, failed = 0;
        const allUpdates = [];
        const t0 = Date.now();

        await Promise.all(this.targets.map(target => limit(async () => {
            try {
                const byDate = await this.fetchSettlementOne(target);
                const updates = await this.commitObservations(target, byDate, 'settlement');
                allUpdates.push(...updates);
                const summary = Object.entries(byDate).sort(([a], [b]) => a.localeCompare(b))
                    .map(([d, info]) => `${d}:H=${info.high}°${target.unit}/L=${info.low}°(${info.count})`)
                    .join(' | ');
                logger.info(`✅ [Settlement|${target.station}|${target.name}] ${summary || 'no data'}`);
                success++;
            } catch (e) {
                logger.warn(`❌ [Settlement|${target.station}|${target.name}] ${e.message}`);
                failed++;
            }
        })));

        const elapsed = Date.now() - t0;
        logger.info(`🧾 [Settlement] cycle done in ${(elapsed / 1000).toFixed(1)}s. success=${success}, failed=${failed}, updates=${allUpdates.length}`);
        return allUpdates;
    }

    // =====================
    // 通用：与 Redis 已有状态对比 + 落地
    // =====================

    /**
     * type: 'metar' | 'settlement'
     * byDate: { date: { high, low, count, highObsTs, lowObsTs, latestTemp, latestObsTs,
     *                   firstObsTs, lastObsTs, lastRaw?, detail } } (已是 target.unit)
     * 只有 high/low/count 发生变化才计入 updates（推送给策略）
     *
     * 写入 Redis：
     *   poly:{type}:{station}:{date}        hash - 主状态
     *   poly:{type}:obs:{station}:{date}    string(JSON) - obs detail（供 UI 点击展开看明细）
     *
     * 跟踪三类"首次见到"语义：
     *   high_obs_ts        max 对应那条观测的发生时间
     *   high_first_seen_at 我们首次记录到这个 high 的本地 wall clock
     *   low_obs_ts / low_first_seen_at 同理
     *   latest_obs_ts      最新一条观测的发生时间
     *   latest_temp        最新一条观测的温度（不是 max！METAR 高敏感场景需要）
     *   latest_first_seen_at 我们首次记录到这条 latest 的本地 wall clock
     *   first_seen_at      整个 hash key 首次创建的本地 wall clock（任何字段写入即记录）
     */
    async commitObservations(target, byDate, type) {
        const updates = [];
        const nowMs = Date.now();
        for (const [date, info] of Object.entries(byDate)) {
            const key = `poly:${type}:${target.station}:${date}`;
            const detailKey = `poly:${type}:obs:${target.station}:${date}`;
            const existing = await redis.hgetall(key);
            const prevHigh = existing.high !== undefined && existing.high !== '' ? Number(existing.high) : null;
            const prevLow  = existing.low  !== undefined && existing.low  !== '' ? Number(existing.low)  : null;
            const prevCount = existing.obs_count !== undefined && existing.obs_count !== ''
                ? Number(existing.obs_count) : 0;
            const prevLatestObsTs = existing.latest_obs_ts !== undefined && existing.latest_obs_ts !== ''
                ? Number(existing.latest_obs_ts) : 0;
            const prevLatestTemp = existing.latest_temp !== undefined && existing.latest_temp !== ''
                ? Number(existing.latest_temp) : null;

            const latestTempChanged = info.latestTemp !== null && info.latestTemp !== undefined
                && (prevLatestTemp === null || Number(info.latestTemp) !== prevLatestTemp);

            const changed = (prevHigh !== info.high)
                         || (prevLow !== info.low)
                         || (info.count > prevCount)
                         || latestTempChanged;

            const payload = {
                station: target.station,
                name: target.name || '',
                date,
                unit: target.unit,
                source: type,
                high: info.high,
                low: info.low,
                obs_count: info.count,
                first_obs_ts: info.firstObsTs || '',
                last_obs_ts: info.lastObsTs,
                updated_at: nowMs,
                // 始终同步 obs_ts 字段：消除 stale（老数据迁移、TWC 数据回退、窗口滑动等场景）
                // 这样 summary 显示的"高温对应的观测时间"会跟 detail 高亮的那条永远一致
                high_obs_ts: info.highObsTs || '',
                low_obs_ts: info.lowObsTs || '',
                latest_obs_ts: info.latestObsTs || '',
            };
            if (info.lastRaw) payload.last_raw = info.lastRaw;
            if (info.latestTemp !== null && info.latestTemp !== undefined) {
                payload.latest_temp = info.latestTemp;
            }

            // 首次创建 hash 时记录 first_seen_at
            if (!existing.first_seen_at) payload.first_seen_at = nowMs;

            // first_seen_at 三连：分两种情况
            //   1) 严格升降 (high > prev / low < prev / latest 推进)：刷新成现在
            //   2) 老数据没有这个 first_seen 字段（PR #16 留下的 stale 数据）：用现在 backfill 一次
            //      之后稳态再不动，所以"我们首次见到这个值的时间"语义就建立起来了
            if (prevHigh === null || info.high > prevHigh) {
                payload.high_first_seen_at = nowMs;
            } else if (!existing.high_first_seen_at) {
                payload.high_first_seen_at = nowMs;
            }
            if (prevLow === null || info.low < prevLow) {
                payload.low_first_seen_at = nowMs;
            } else if (!existing.low_first_seen_at) {
                payload.low_first_seen_at = nowMs;
            }
            if (info.latestObsTs > prevLatestObsTs) {
                payload.latest_first_seen_at = nowMs;
            } else if (!existing.latest_first_seen_at && info.latestObsTs > 0) {
                payload.latest_first_seen_at = nowMs;
            }

            await redis.hset(key, payload);
            await redis.expire(key, 14 * 24 * 60 * 60);

            // 写明细列表（每次全量重写：仅反映当前 72h 窗口里看到的观测）
            // 注意：byDate[date] 为空（窗口里这天没观测）时，commitObservations 根本不会进入这条循环，
            // 所以老的 detailKey 会自然保留——这是有意的，避免短暂的网络抖动清掉昨天的明细。
            if (info.detail && info.detail.length > 0) {
                await redis.set(detailKey, JSON.stringify(info.detail), 'EX', 14 * 24 * 60 * 60);
            }

            if (changed) {
                updates.push({
                    type,
                    station: target.station,
                    name: target.name || '',
                    date,
                    unit: target.unit,
                    high: info.high,
                    low: info.low,
                    prev_high: prevHigh,
                    prev_low: prevLow,
                    latest_temp: info.latestTemp,
                    prev_latest_temp: prevLatestTemp,
                    latest_obs_ts: info.latestObsTs,
                    prev_latest_obs_ts: prevLatestObsTs,
                    high_obs_ts: info.highObsTs,
                    low_obs_ts: info.lowObsTs,
                    obs_count: info.count,
                    prev_count: prevCount,
                    first_obs_ts: info.firstObsTs || null,
                    last_obs_ts: info.lastObsTs,
                    first_seen_at: existing.first_seen_at ? Number(existing.first_seen_at) : nowMs,
                });
            }
        }
        return updates;
    }

    async publishUpdates(updates) {
        if (updates.length === 0) return;
        const msg = JSON.stringify({
            source: this.sourceId,
            ts: Date.now(),
            data: { updates }
        });
        await redis.publish('poly:feed:weather_obs', msg);
        await redis.set(`poly:latest:${this.sourceId}`, msg);
    }

    /** wethr 的 updates 走独立频道，避免冲淡 METAR/Settlement 的 SSE 节奏 */
    async publishWethrUpdates(updates) {
        if (updates.length === 0) return;
        const msg = JSON.stringify({
            source: 'wethr_push',
            ts: Date.now(),
            data: { updates }
        });
        await redis.publish('poly:feed:wethr_obs', msg);
        await redis.set('poly:latest:wethr_obs', msg);
    }

    // =====================
    // wethr Push（SSE 子任务）
    // =====================

    /**
     * 首次 fetch() 时初始化 wethr SSE 客户端（lazy init）：
     * - 从 cities 配置筛 wethrEnabled=true 的站点
     * - 启动 SSE 长连接 + observation 事件入 buffer
     * - 从 Redis 已有的 detail 恢复 buffer（pm2 重启不丢历史）
     */
    async _initWethrIfNeeded() {
        if (this.wethrConfigReady) return;

        // 三个 early-exit 是"配置不需要 wethr"，可标 ready；
        // 但 throw 类（_restoreWethrBuffers / WethrPushClient ctor）要保证失败后能重试
        const enabled = this.targets.filter(t => {
            const v = t.wethrEnabled;
            return v === true || v === 'true' || v === '1' || v === 1;
        });
        if (enabled.length === 0) {
            this.wethrConfigReady = true;
            logger.info('[wethr] no cities have wethrEnabled=1, skipping');
            return;
        }
        if (enabled.length > 5) {
            this.wethrConfigReady = true;  // 配置错误，重试也没用
            logger.error(`[wethr] ${enabled.length} cities have wethrEnabled=1 but Professional tier 限 5 站，请减少；skipping wethr`);
            return;
        }
        const apiKey = process.env.WETHR_API_KEY;
        if (!apiKey) {
            this.wethrConfigReady = true;  // 启动校验已 fail-fast，理论上到不了这里
            logger.error('[wethr] WETHR_API_KEY env var missing — wethr disabled');
            return;
        }

        // 真正可能抛错的初始化用 try / catch；失败时 wethrConfigReady 保持 false，下个 tick 重试
        try {
            this.wethrTargets = enabled;
            for (const t of enabled) this.wethrBuffer.set(t.station, []);
            await this._restoreWethrBuffers();
            this.wethrClient = new WethrPushClient({
                stations: enabled.map(t => t.station),
                apiKey,
                onEvent: (e) => this._handleWethrEvent(e),
            });
            this.wethrClient.start();
            this.wethrConfigReady = true;
            logger.info(`[wethr] enabled for ${enabled.length} stations: ${enabled.map(t => t.station).join(',')}`);
        } catch (e) {
            logger.warn(`[wethr] init failed, will retry next tick: ${e.message}`);
            this.wethrTargets = [];
            this.wethrClient = null;
            // wethrConfigReady 仍是 false，下个 tick 还会调用 _initWethrIfNeeded
        }
    }

    async _restoreWethrBuffers() {
        for (const target of this.wethrTargets) {
            const tz = target.tz;
            const now = dayjs().tz(tz);
            const restored = [];
            for (const offset of [1, 0]) {
                const d = now.subtract(offset, 'day').format('YYYY-MM-DD');
                const raw = await redis.get(`poly:wethr:obs:${target.station}:${d}`);
                if (!raw) continue;
                try {
                    const list = JSON.parse(raw);
                    if (!Array.isArray(list)) continue;
                    for (const o of list) {
                        if (typeof o?.ts === 'number' && typeof o?.temp === 'number') {
                            // detail 里 temp 是 target.unit；buffer 要 C（aggregateMetarByDate 假设输入 C）
                            const tempC = target.unit === 'F' ? (o.temp - 32) * 5 / 9 : o.temp;
                            restored.push({ obsTime: o.ts, temp: tempC, raw: '' });
                        }
                    }
                } catch {}
            }
            restored.sort((a, b) => a.obsTime - b.obsTime);
            this.wethrBuffer.set(target.station, restored);
            if (restored.length > 0) {
                logger.info(`[wethr] restored ${restored.length} obs for ${target.station} from Redis`);
            }
        }
    }

    /**
     * 解析 wethr SSE 事件，提取 observation 入 buffer
     * 只处理 ASOS-HFM/ASOS-HR/SPECI 的温度观测；忽略 connected/heartbeat/new_high/new_low
     * （new_high/new_low 是衍生事件，原始 observation 进 buffer 就够了，避免重复计入）
     */
    _handleWethrEvent(e) {
        if (!e || e.type !== 'observation') return;
        const d = e.data;
        if (!d || typeof d !== 'object') return;

        const station = d.station_code;
        // 关键：用 temperature_f 而不是 temperature（后者是摄氏）
        const tempF = d.temperature_f;
        const obsIso = d.observation_time_utc;
        if (!station || typeof tempF !== 'number' || !obsIso) return;

        const obsTime = Math.floor(new Date(obsIso).getTime() / 1000);
        if (!isFinite(obsTime) || obsTime <= 0) return;

        const buf = this.wethrBuffer.get(station);
        if (!buf) return; // 收到不在订阅列表里的站，忽略

        // 数据质量降级：anomaly / suspect_temperature → 不进 buffer
        if (d.anomaly === true || d.suspect_temperature === true) {
            logger.debug(`[wethr] dropped ${station} obs at ${obsIso}: anomaly/suspect flagged`);
            return;
        }

        // F → C 让 aggregateMetarByDate 一视同仁
        const tempC = (tempF - 32) * 5 / 9;
        buf.push({
            obsTime,
            temp: tempC,
            raw: `${d.product || 'OBS'} ${tempF}°F`,
        });
    }

    /**
     * 每个 tick 把 buffer flush 进 Redis：
     * - 先按 48h 窗口 trim buffer（避免无限增长）
     * - aggregateMetarByDate 算 high/low/latest + detail
     * - commitObservations 走 'wethr' source（与 metar/settlement 完全对称）
     */
    async runWethrFlush() {
        if (!this.wethrClient || this.wethrTargets.length === 0) return [];
        const cutoffSec = Math.floor((Date.now() - 48 * 3600 * 1000) / 1000);

        const allUpdates = [];
        for (const target of this.wethrTargets) {
            let buf = this.wethrBuffer.get(target.station);
            if (!buf || buf.length === 0) continue;

            // trim 48h（按 obsTime 升序）
            buf = buf.filter(o => o.obsTime >= cutoffSec);
            this.wethrBuffer.set(target.station, buf);
            if (buf.length === 0) continue;

            const tz = target.tz;
            const now = dayjs().tz(tz);
            const valid = [now.format('YYYY-MM-DD'), now.subtract(1, 'day').format('YYYY-MM-DD')];
            // 直接复用 metar 的聚合：输入 C，按 target.unit 输出
            const byDate = this.aggregateMetarByDate(buf, target, valid);
            const updates = await this.commitObservations(target, byDate, 'wethr');
            allUpdates.push(...updates);
        }
        return allUpdates;
    }

    /** BaseCollector.stop() 的 hook：必须 await 释放 wethr slot，否则 pm2 重启被自己 displaced */
    async stop() {
        super.stop();
        if (this.wethrClient) {
            const client = this.wethrClient;
            this.wethrClient = null;
            try { await client.stop(); } catch (e) {
                logger.warn(`[wethr] stop error: ${e.message}`);
            }
        }
    }

    // =====================
    // 主循环
    // =====================

    async fetch() {
        this.tick++;
        await this.loadCitiesConfig();
        if (this.targets.length === 0) {
            logger.warn('⚠️ [Obs] no cities configured, skipping tick');
            return null;
        }

        // 每个 tick 入口刷一次 proxy 池快照（用户可能动态增减 proxy）
        await this.loadProxyPool();

        // 第一次 fetch 时初始化 wethr SSE（lazy，因为依赖 loadCitiesConfig 的结果）
        await this._initWethrIfNeeded();

        if (this.tick === 1 || this.tick % 60 === 1) {
            // 启动时 + 每 60 tick (5 min) 打一次状态日志
            const wstats = this.wethrClient ? this.wethrClient.getStats() : null;
            logger.info(`[Obs] cities=${this.targets.length}, proxies=${this.proxyPool.length}, `
                + `tick=${this.intervalMs/1000}s, metar conc=${this.metarConcurrency}, `
                + `settlement conc=${this.settlementConcurrency}`
                + (wstats ? `, wethr=${wstats.stations.length}st/conn=${wstats.connects}/evt=${wstats.events}/disp=${wstats.displaced}` : '')
            );
        }

        // 每个 tick 都跑 METAR
        const metarUpdates = await this.runMetarTick();

        // Settlement: 每 settlementEveryTicks 个 tick 跑一次
        let settlementUpdates = [];
        if (this.tick % this.settlementEveryTicks === this.settlementOffset % this.settlementEveryTicks) {
            settlementUpdates = await this.runSettlementTick();
        }

        // METAR + Settlement 发到 weather_obs 频道
        const weatherObsUpdates = [...metarUpdates, ...settlementUpdates];
        if (weatherObsUpdates.length > 0) await this.publishUpdates(weatherObsUpdates);

        // wethr buffer flush 发到独立 wethr_obs 频道
        const wethrUpdates = await this.runWethrFlush();
        if (wethrUpdates.length > 0) await this.publishWethrUpdates(wethrUpdates);

        return null;
    }
}

export default WeatherObservationsCollector;
