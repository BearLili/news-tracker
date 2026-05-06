import axios from 'axios';
import * as cheerio from 'cheerio';
import BaseCollector from '../core/BaseCollector.js';
import logger from '../utils/logger.js';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import timezone from 'dayjs/plugin/timezone.js';
import Redis from 'ioredis';

dayjs.extend(utc);
dayjs.extend(timezone);

const redis = new Redis({ host: '127.0.0.1', port: 6379 });

class WeatherForecastCollector extends BaseCollector {
    constructor() {
        super('weather_forecast_4days', 15 * 60 * 1000);
        this.targets = [];
        this.noaaGridCache = new Map(); // key: "lat,lon" -> { gridId, gridX, gridY }
    }

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
        
    randInt(min, max) {
        return Math.floor(Math.random() * (max - min + 1)) + min;
    }

    async loadCitiesConfig() {
        try {
            const rawData = await redis.hgetall('poly:config:cities');
            this.targets = Object.values(rawData).map(s => {
                try { return JSON.parse(s); } catch (e) { return null; }
            }).filter(c => c && c.fetchData === true);
            logger.info(`📋 Loaded ${this.targets.length} cities from Redis config.`);
        } catch (e) {
            logger.error(`❌ Load Config Error: ${e.message}`);
        }
    }

    async getAxiosProxy() {
        try {
            const proxyStr = await redis.srandmember('poly:proxylist');
            if (!proxyStr) return null;
            const parts = proxyStr.split(':');
            if (parts.length < 2) return null;
            return {
                protocol: 'http',
                host: parts[0],
                port: parseInt(parts[1]),
                auth: (parts[2] && parts[3]) ? { username: parts[2], password: parts[3] } : undefined
            };
        } catch (e) { return null; }
    }

    // 辅助：华氏转摄氏
    fToC(f) { return Math.round((f - 32) * 5 / 9); }

    // 辅助：摄氏转华氏
    cToF(c) { return Math.round((c * 9 / 5) + 32); }

    median(values) {
        if (!Array.isArray(values) || values.length === 0) return null;
        const arr = [...values].sort((a, b) => a - b);
        const mid = Math.floor(arr.length / 2);
        if (arr.length % 2 === 0) return (arr[mid - 1] + arr[mid]) / 2;
        return arr[mid];
    }

    toMapByDate(rows) {
        const m = new Map();
        (rows || []).forEach(r => {
            if (r?.date) m.set(r.date, r);
        });
        return m;
    }

    async fetch() {
        await this.loadCitiesConfig();

        if (this.targets.length === 0) {
            logger.warn('⚠️ No targets loaded. Skipping cycle.');
            return;
        }

        logger.info('🔮 Starting 4-Day Forecast Collection (Respecting Config Units)...');

        const batchSize = 5;
        const total = this.targets.length;

        for (let i = 0; i < total; i += batchSize) {
            const batch = this.targets.slice(i, i + batchSize);
            const batchNo = Math.floor(i / batchSize) + 1;
            const batchTotal = Math.ceil(total / batchSize);
            
            logger.info(`🚚 Batch ${batchNo}/${batchTotal}, cities=${batch.length}`);

            const tasks = batch.map(async (target, idx) => {
                if (idx > 0) await this.sleep(this.randInt(1500, 3000));
                // 1) 并行采集
                const results = await Promise.allSettled([
                    this.fetchWunderground(target),
                    this.fetchOpenMeteo(target),
                    target.type === 'us' ? this.fetchNOAA(target) : Promise.resolve([]),
                    target.type === 'intl' ? this.fetchMetNo(target) : Promise.resolve([]),
                    this.fetchOpenMeteoModels(target)
                ]);

                const wunderData = results[0].status === 'fulfilled' ? results[0].value : [];
                const openData = results[1].status === 'fulfilled' ? results[1].value : [];
                const noaaData = results[2].status === 'fulfilled' ? results[2].value : [];
                const metNoData = results[3].status === 'fulfilled' ? results[3].value : [];
                const openMModels = results[4].status === 'fulfilled' ? results[4].value : {};

                const wunderMap = this.toMapByDate(wunderData);
                const openMap = this.toMapByDate(openData);
                const noaaMap = this.toMapByDate(noaaData);
                const metNoMap = this.toMapByDate(metNoData);
                const modelMapByName = Object.fromEntries(
                    Object.entries(openMModels || {}).map(([name, rows]) => [name, this.toMapByDate(rows)])
                );

                const today = dayjs().tz(target.tz);

                const logBuffer = [];
                logBuffer.push(`\n📊 [${target.name} | ${target.station}] Forecast Summary (Next 4 Days) [Unit: ${target.unit}]:`);
                logBuffer.push(`    Date    |  Wunder H/L |  OpenM H/L |  NOAA H/L  |  MET.NO H/L`);
                logBuffer.push(` -----------|-------------|------------|------------|------------`);

                for (let i = 0; i < 4; i++) {
                    const targetDate = today.add(i, 'day').format('YYYY-MM-DD');
                    const key = `poly:forecast:${target.station}:${targetDate}`;

                    const wVal = wunderMap.get(targetDate);
                    const oVal = openMap.get(targetDate);
                    const nVal = noaaMap.get(targetDate);
                    const mVal = metNoMap.get(targetDate);

                    const fmt = (v) => v ? `${v.high}/${v.low ?? '-'}` : '--';
                    const wStr = fmt(wVal);
                    const oStr = fmt(oVal);
                    const nStr = fmt(nVal);
                    const mStr = fmt(mVal);

                    logBuffer.push(` ${targetDate} | ${wStr.padEnd(11)} | ${oStr.padEnd(10)} | ${nStr.padEnd(10)} | ${mStr.padEnd(10)}`);

                    const nowTs = Date.now();
                    const payload = {};
                    if (wVal) payload['wunder'] = JSON.stringify({ high: wVal.high, low: wVal.low, unit: target.unit, ts: nowTs, source: 'wunder' });
                    if (oVal) payload['open_meteo'] = JSON.stringify({ high: oVal.high, low: oVal.low, unit: target.unit, ts: nowTs, source: 'open_meteo' });
                    if (nVal) payload['noaa'] = JSON.stringify({ high: nVal.high, low: nVal.low, unit: target.unit, ts: nowTs, source: 'noaa' });
                    if (mVal) payload['met_no'] = JSON.stringify({ high: mVal.high, low: mVal.low, unit: target.unit, ts: nowTs, source: 'met_no' });

                    // Open-Meteo 多模型明细，每个模型独立存为一个 field
                    for (const [name, map] of Object.entries(modelMapByName)) {
                        const row = map.get(targetDate);
                        if (row && typeof row.high === 'number') {
                            const shortName = name.replace('_ifs025', '').replace('_seamless', '');
                            payload[`open_meteo_${shortName}`] = JSON.stringify({
                                high: row.high,
                                low: row.low,
                                unit: target.unit,
                                ts: nowTs,
                                source: `open_meteo_${shortName}`
                            });
                        }
                    }

                    if (Object.keys(payload).length > 0) {
                        payload['updated_at'] = nowTs;
                        await redis.hmset(key, payload);
                        await redis.expire(key, 7 * 24 * 60 * 60);
                    }
                }

                logBuffer.push(`✅ [${target.station}] Processed.`);
                logger.info(logBuffer.join('\n'));
            });

            await Promise.all(tasks);

            // 非最后一批才等待
            if (i + batchSize < total) {
                const gap = this.randInt(5000, 8000);
                logger.info(`⏳ Wait ${gap}ms before next batch...`);
                await this.sleep(gap);
            }
        }

        return { timestamp: Date.now() };
    }

    // ==========================================
    // 源 1: Wunderground
    // 特性: Raw JSON 几乎总是 F，需要根据 target.unit 转换
    // ==========================================
    async fetchWunderground(target) {
        if (!target.wunderUrl) return [];
        try {
            const proxyConfig = await this.getAxiosProxy();
            // 用 hourly 页面而非 forecast 页面，拿逐小时预报，按本地日聚合得到 high/low
            // Wunder 是 Polymarket 的裁决数据源，需要最新数据
            const hourlyUrl = target.wunderUrl.replace('/weather/', '/hourly/');
            const response = await axios.get(hourlyUrl, {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Cache-Control': 'no-cache'
                },
                timeout: 15000,
                proxy: proxyConfig || false
            });

            const $ = cheerio.load(response.data);
            const scriptContent = $('#app-root-state').html();
            if (!scriptContent) return [];

            const rawData = JSON.parse(scriptContent);

            // 找逐小时温度数组：长度匹配的 temperature + validTimeLocal，长度通常 100+
            let hourlyTemps = null;
            let hourlyTimes = null;
            for (const key in rawData) {
                const b = rawData[key]?.b;
                if (!b) continue;
                if (Array.isArray(b.temperature) && Array.isArray(b.validTimeLocal)
                    && b.temperature.length === b.validTimeLocal.length
                    && b.temperature.length > 100) {
                    hourlyTemps = b.temperature;
                    hourlyTimes = b.validTimeLocal;
                    break;
                }
            }
            if (!hourlyTemps) return [];

            const highByDate = new Map();
            const lowByDate = new Map();
            for (let i = 0; i < hourlyTemps.length; i++) {
                const t = hourlyTemps[i]; // F
                const ts = hourlyTimes[i];
                if (typeof t !== 'number' || !ts) continue;
                const dateStr = dayjs(ts).format('YYYY-MM-DD');
                const prevH = highByDate.get(dateStr);
                if (prevH === undefined || t > prevH) highByDate.set(dateStr, t);
                const prevL = lowByDate.get(dateStr);
                if (prevL === undefined || t < prevL) lowByDate.set(dateStr, t);
            }

            const result = [];
            for (const [dateStr, h] of highByDate) {
                let high = h;
                let low = lowByDate.get(dateStr);
                if (target.unit === 'C') {
                    high = this.fToC(high);
                    if (typeof low === 'number') low = this.fToC(low);
                } else {
                    high = Math.round(high);
                    if (typeof low === 'number') low = Math.round(low);
                }
                result.push({ date: dateStr, high, low: typeof low === 'number' ? low : null });
            }
            return result;
        } catch (e) {
            // logger.warn(`[Wunder] Failed ${target.station}: ${e.message}`);
            return [];
        }
    }

    // ==========================================
    // 源 2: Open-Meteo
    // 特性: 支持参数控制，直接请求需要的单位
    // ==========================================
    async fetchOpenMeteo(target) {
        try {
            const unitParam = target.unit === 'F' ? 'fahrenheit' : 'celsius';
            const url = `https://api.open-meteo.com/v1/forecast?latitude=${target.lat}&longitude=${target.lon}&daily=temperature_2m_max,temperature_2m_min&timezone=auto&forecast_days=5&temperature_unit=${unitParam}`;

            const response = await axios.get(url, { timeout: 8000 });
            const daily = response.data?.daily;
            if (!daily || !daily.time || !daily.temperature_2m_max) return [];

            const result = [];

            for (let i = 0; i < daily.time.length; i++) {
                const dateStr = daily.time[i];
                const high = daily.temperature_2m_max[i];
                const low = daily.temperature_2m_min?.[i];
                if (dateStr && typeof high === 'number') {
                    result.push({ date: dateStr, high: Math.round(high), low: typeof low === 'number' ? Math.round(low) : null });
                }
            }
            return result;
        } catch (e) {
            logger.warn(`[OpenMeteo] Failed ${target.station}: ${e.message}`);
            return [];
        }
    }

    // ==========================================
    // 源 2.1: Open-Meteo 多模型（用于观测，us + intl 都采）
    // ==========================================
    async fetchOpenMeteoModels(target) {
        const unitParam = target.unit === 'F' ? 'fahrenheit' : 'celsius';
        const modelNames = ['ecmwf_ifs025', 'gfs_seamless', 'icon_seamless'];
        const out = {};

        try {
            const url = `https://api.open-meteo.com/v1/forecast?latitude=${target.lat}&longitude=${target.lon}&daily=temperature_2m_max,temperature_2m_min&timezone=auto&forecast_days=5&temperature_unit=${unitParam}&models=${modelNames.join(',')}`;
            const response = await axios.get(url, { timeout: 8000 });
            const daily = response.data?.daily;
            if (!daily?.time) return out;

            for (const model of modelNames) {
                const maxKey = `temperature_2m_max_${model}`;
                const minKey = `temperature_2m_min_${model}`;
                const highs = daily[maxKey];
                const lows = daily[minKey];
                if (!Array.isArray(highs)) { out[model] = []; continue; }

                out[model] = daily.time.map((dateStr, idx) => {
                    const high = highs[idx];
                    const low = lows?.[idx];
                    if (!dateStr || typeof high !== 'number') return null;
                    return { date: dateStr, high: Math.round(high), low: typeof low === 'number' ? Math.round(low) : null };
                }).filter(Boolean);
            }
        } catch (e) {
            modelNames.forEach(m => { out[m] = []; });
        }

        return out;
    }

    // ==========================================
    // 源 2.2: MET Norway（met.no）
    // 特性: 逐小时温度（C），这里聚合为本地日最高；仅 intl 城市采集
    // ==========================================
    async fetchMetNo(target) {
        try {
            const url = `https://api.met.no/weatherapi/locationforecast/2.0/compact?lat=${target.lat}&lon=${target.lon}`;
            const response = await axios.get(url, {
                timeout: 8000,
                headers: {
                    'User-Agent': 'news-tracker/1.0 (contact: openclaw-agent)'
                }
            });

            const series = response.data?.properties?.timeseries;
            if (!Array.isArray(series) || series.length === 0) return [];

            const maxByDate = new Map();
            const minByDate = new Map();
            for (const point of series) {
                const iso = point?.time;
                const tempC = point?.data?.instant?.details?.air_temperature;
                if (!iso || typeof tempC !== 'number') continue;

                const dateStr = dayjs(iso).tz(target.tz).format('YYYY-MM-DD');
                const prevMax = maxByDate.get(dateStr);
                if (prevMax === undefined || tempC > prevMax) maxByDate.set(dateStr, tempC);
                const prevMin = minByDate.get(dateStr);
                if (prevMin === undefined || tempC < prevMin) minByDate.set(dateStr, tempC);
            }

            const today = dayjs().tz(target.tz).startOf('day');
            const result = [];
            for (let i = 0; i < 5; i++) {
                const d = today.add(i, 'day').format('YYYY-MM-DD');
                if (!maxByDate.has(d)) continue;
                let high = maxByDate.get(d);
                let low = minByDate.get(d);
                if (target.unit === 'F') { high = this.cToF(high); low = low !== undefined ? this.cToF(low) : null; }
                else { high = Math.round(high); low = low !== undefined ? Math.round(low) : null; }
                result.push({ date: d, high, low });
            }

            return result;
        } catch (e) {
            logger.warn(`[MET.NO] Failed ${target.station}: ${e.message}`);
            return [];
        }
    }

    // ==========================================
    // 源 3: NOAA
    // 特性: 默认 F，如果配置要 C 则转换
    // ==========================================
    async fetchNOAA(target) {
        try {
            const proxyConfig = await this.getAxiosProxy();
            const cacheKey = `${target.lat},${target.lon}`;
            let gridId, gridX, gridY;

            try {
                const pointUrl = `https://api.weather.gov/points/${target.lat},${target.lon}`;
                const pointRes = await axios.get(pointUrl, {
                    headers: { 'User-Agent': 'PolyBot_Forecast/1.0' },
                    timeout: 8000,
                    proxy: proxyConfig || false
                });
                ({ gridId, gridX, gridY } = pointRes.data.properties);
                this.noaaGridCache.set(cacheKey, { gridId, gridX, gridY });
            } catch (e) {
                const cached = this.noaaGridCache.get(cacheKey);
                if (!cached) throw e;
                ({ gridId, gridX, gridY } = cached);
                logger.debug(`[NOAA] /points failed for ${target.station}, using cached grid`);
            }

            // 并行拉取：实测观测（今天的真实数据）+ hourly 预报（未来）
            const hourlyUrl = `https://api.weather.gov/gridpoints/${gridId}/${gridX},${gridY}/forecast/hourly`;
            const obsUrl = `https://api.weather.gov/stations/${target.station}/observations?limit=500`;

            const [foreRes, obsRes] = await Promise.allSettled([
                axios.get(hourlyUrl, {
                    headers: { 'User-Agent': 'PolyBot_Forecast/1.0' },
                    timeout: 8000,
                    proxy: proxyConfig || false
                }),
                axios.get(obsUrl, {
                    headers: { 'User-Agent': 'PolyBot_Forecast/1.0' },
                    timeout: 8000,
                    proxy: proxyConfig || false
                })
            ]);

            const highByDate = new Map();
            const lowByDate = new Map();

            // 1) 实测观测点（覆盖最近 ~2 天，每 5 分钟一条）—— Polymarket 裁决基准
            if (obsRes.status === 'fulfilled') {
                const features = obsRes.value.data?.features || [];
                for (const f of features) {
                    const ts = f?.properties?.timestamp;
                    const tempC = f?.properties?.temperature?.value;
                    if (!ts || typeof tempC !== 'number') continue;
                    const dateStr = dayjs(ts).tz(target.tz).format('YYYY-MM-DD');
                    const tempF = (tempC * 9 / 5) + 32; // 转 F 与其他源对齐
                    const prevHigh = highByDate.get(dateStr);
                    if (prevHigh === undefined || tempF > prevHigh) highByDate.set(dateStr, tempF);
                    const prevLow = lowByDate.get(dateStr);
                    if (prevLow === undefined || tempF < prevLow) lowByDate.set(dateStr, tempF);
                }
            } else {
                logger.warn(`[NOAA] observations failed ${target.station}: ${obsRes.reason?.message}`);
            }

            // 2) hourly 预报（未来几天）—— 跟实测合并取 max/min，今天会同时受两边影响
            if (foreRes.status === 'fulfilled') {
                const periods = foreRes.value.data?.properties?.periods || [];
                for (const p of periods) {
                    if (typeof p.temperature !== 'number') continue;
                    const dateStr = dayjs(p.startTime).tz(target.tz).format('YYYY-MM-DD');
                    // 统一转 F，与 observations 对齐
                    const t = p.temperatureUnit === 'C' ? (p.temperature * 9 / 5) + 32 : p.temperature;
                    const prevHigh = highByDate.get(dateStr);
                    if (prevHigh === undefined || t > prevHigh) highByDate.set(dateStr, t);
                    const prevLow = lowByDate.get(dateStr);
                    if (prevLow === undefined || t < prevLow) lowByDate.set(dateStr, t);
                }
            } else {
                logger.warn(`[NOAA] hourly forecast failed ${target.station}: ${foreRes.reason?.message}`);
            }

            const result = [];
            for (const [dateStr, high] of highByDate) {
                let h = high;
                let l = lowByDate.get(dateStr);
                if (target.unit === 'C') {
                    h = this.fToC(h);
                    if (typeof l === 'number') l = this.fToC(l);
                } else {
                    h = Math.round(h);
                    if (typeof l === 'number') l = Math.round(l);
                }
                result.push({ date: dateStr, high: h, low: typeof l === 'number' ? l : null });
            }
            return result;
        } catch (e) {
            //
            logger.warn(`[NOAA] Failed ${target.station}: ${e.message}`);
            return [];
        }
    }

}

export default WeatherForecastCollector;