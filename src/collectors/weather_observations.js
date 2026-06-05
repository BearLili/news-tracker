import axios from 'axios';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import timezone from 'dayjs/plugin/timezone.js';
import BaseCollector from '../core/BaseCollector.js';
import redis from '../core/redis.js';
import logger from '../utils/logger.js';

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
        // 基础 tick = 10s（= METAR 节奏）
        // 注：METAR 每个站点真实更新约每小时一次（SPECI 异常报除外），
        // 10s 远超 aviationweather.gov "1 req/min per thread" 建议，
        // 但因为 batch 一次拉所有站，全局 6 req/min «« 100 req/min 上限，所以仍然安全。
        super('weather_observations', 10 * 1000);
        this.targets = [];
        this.tick = 0;
        // Settlement 每 N 个 tick 跑一次（12 × 10s = 120s = 2 分钟）
        this.settlementEveryTicks = 12;
        // 启动时立刻跑一遍 settlement（不等 2 分钟）
        this.settlementOffset = 1;
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
            const p = s.split(':');
            if (p.length < 2) return null;
            return {
                protocol: 'http',
                host: p[0],
                port: parseInt(p[1]),
                auth: (p[2] && p[3]) ? { username: p[2], password: p[3] } : undefined
            };
        } catch { return null; }
    }

    // =====================
    // METAR (pre-record)
    // =====================

    /**
     * Batch 拉取所有站点的 METAR，最近 72 小时
     * 72 小时是为了完整覆盖任意时区（含极端 UTC-12 ~ UTC+14）的"昨天"全部观测：
     * 在 UTC-12 时区，"昨天 00:00" 距当前 UTC 最多可达 24h(昨天) + 24h(今天) + 12h(偏移) = 60h
     * 用 72h 留余量。响应仅多~50% 数据量（仍是 KB 级），对带宽和限流都没影响。
     * 返回 Map<icaoId, [{obsTime, temp, raw}, ...]>
     */
    async fetchMetarBatch(stations) {
        const url = `${METAR_BASE}?ids=${stations.join(',')}&format=json&hours=72`;
        const resp = await axios.get(url, {
            timeout: 15000,
            headers: {
                'User-Agent': 'news-tracker/1.0 (contact: openclaw-agent)',
                'Accept': 'application/json'
            },
            proxy: false   // 跟 NPM 同坑：绕开本地 HTTPS_PROXY
        });
        const byStation = new Map();
        if (!Array.isArray(resp.data)) return byStation;
        for (const e of resp.data) {
            const id = e?.icaoId;
            const t = e?.temp;            // 始终 Celsius
            const ts = e?.obsTime;        // unix seconds
            if (!id || typeof t !== 'number' || typeof ts !== 'number') continue;
            if (!byStation.has(id)) byStation.set(id, []);
            byStation.get(id).push({ obsTime: ts, temp: t, raw: e.rawOb || '' });
        }
        return byStation;
    }

    /**
     * METAR 数据是 Celsius，按 target.unit 转换 + 按 target.tz 聚合到本地日 high/low
     * 仅保留 dates 内的日期（避免远日观测污染）
     */
    // 注意：firstObsTs 语义是"当前 72h 窗口里能见到的最早 obsTime"，
    // 一旦最早那条观测滑出 72h 窗口，firstObsTs 会向后跳——不是"该日历日的首条观测时间"。
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
            // 严格 >，让"最早达到该极值"的观测胜出（同温取最早）
            if (o.temp > slot.highC) { slot.highC = o.temp; slot.highObsTs = o.obsTime; }
            if (o.temp < slot.lowC)  { slot.lowC  = o.temp; slot.lowObsTs  = o.obsTime; }
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
            if (t > slot.high) { slot.high = t; slot.highObsTs = ts; }
            if (t < slot.low)  { slot.low  = t; slot.lowObsTs  = ts; }
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
        let success = 0, failed = 0;
        const allUpdates = [];
        for (let i = 0; i < this.targets.length; i++) {
            const target = this.targets[i];
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
            if (i < this.targets.length - 1) {
                await this.sleep(this.randInt(1500, 3500));
            }
        }
        logger.info(`🧾 [Settlement] cycle done. success=${success}, failed=${failed}, updates=${allUpdates.length}`);
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

            const changed = (prevHigh !== info.high)
                         || (prevLow !== info.low)
                         || (info.count > prevCount);

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
                updated_at: nowMs
            };
            if (info.lastRaw) payload.last_raw = info.lastRaw;

            // 首次创建 hash 时记录 first_seen_at
            if (!existing.first_seen_at) payload.first_seen_at = nowMs;

            // high 变化（首次 OR 严格更高）→ 重新记录 high_obs_ts + first_seen
            if (prevHigh === null || info.high > prevHigh) {
                payload.high_obs_ts = info.highObsTs || '';
                payload.high_first_seen_at = nowMs;
            }
            // low 变化（首次 OR 严格更低）→ 同理
            if (prevLow === null || info.low < prevLow) {
                payload.low_obs_ts = info.lowObsTs || '';
                payload.low_first_seen_at = nowMs;
            }
            // 有新观测进来（latest_obs_ts 推进）→ 更新 latest_*
            if (info.latestObsTs > prevLatestObsTs) {
                if (info.latestTemp !== null && info.latestTemp !== undefined) {
                    payload.latest_temp = info.latestTemp;
                }
                payload.latest_obs_ts = info.latestObsTs;
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

        // 每个 tick 都跑 METAR
        const metarUpdates = await this.runMetarTick();

        // Settlement: 每 settlementEveryTicks 个 tick 跑一次（默认 10 → 10 分钟）；
        // 启动后第一个 tick 也跑（settlementOffset=1）
        let settlementUpdates = [];
        if (this.tick % this.settlementEveryTicks === this.settlementOffset % this.settlementEveryTicks) {
            settlementUpdates = await this.runSettlementTick();
        }

        const all = [...metarUpdates, ...settlementUpdates];
        if (all.length > 0) await this.publishUpdates(all);

        // 自己已经处理了 publish，不依赖 BaseCollector.save 兜底
        return null;
    }
}

export default WeatherObservationsCollector;
