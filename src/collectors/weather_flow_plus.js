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
            }).filter(Boolean);
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

            const tasks = batch.map(async (target) => {
                // 1) 并行采集
                const results = await Promise.allSettled([
                    this.fetchWunderground(target),
                    this.fetchOpenMeteo(target),
                    target.type === 'us' ? this.fetchNOAA(target) : Promise.resolve([]),
                    target.type === 'intl' ? this.fetchMetNo(target) : Promise.resolve([]),
                    // this.fetchOpenMeteoModels(target)
                ]);


                const wunderData = results[0].status === 'fulfilled' ? results[0].value : [];
                const openData = results[1].status === 'fulfilled' ? results[1].value : [];
                const noaaData = results[2].status === 'fulfilled' ? results[2].value : [];
                const metNoData = results[3].status === 'fulfilled' ? results[3].value : [];

                const wunderMap = this.toMapByDate(wunderData);
                const openMap = this.toMapByDate(openData);
                const noaaMap = this.toMapByDate(noaaData);
                const metNoMap = this.toMapByDate(metNoData);

                // const modelMapByName = Object.fromEntries(
                //     Object.entries(openMModels || {}).map(([name, rows]) => [name, this.toMapByDate(rows)])
                // );

                const today = dayjs().tz(target.tz);

                const logBuffer = [];
                logBuffer.push(`\n📊 [${target.name} | ${target.station}] Forecast Summary (Next 4 Days) [Unit: ${target.unit}]:`);
                logBuffer.push(`    Date    |  Wunder |  OpenM  |  NOAA   |  MET.NO `);
                logBuffer.push(` -----------|---------|---------|---------|---------`);

                for (let i = 0; i < 4; i++) {
                    const targetDate = today.add(i, 'day').format('YYYY-MM-DD');
                    const key = `poly:forecast:${target.station}:${targetDate}`;

                    const wVal = wunderMap.get(targetDate);
                    const oVal = openMap.get(targetDate);
                    const nVal = noaaMap.get(targetDate);
                    const mVal = metNoMap.get(targetDate);

                    // const modelVals = Object.entries(modelMapByName).map(([name, map]) => {
                    //     const row = map.get(targetDate);
                    //     return { name, high: row?.high };
                    // }).filter(x => typeof x.high === 'number');

                    // const modelHighs = modelVals.map(v => v.high);
                    // const modelMedian = modelHighs.length ? this.median(modelHighs) : null;
                    // const modelSpread = modelHighs.length ? (Math.max(...modelHighs) - Math.min(...modelHighs)) : null;

                    const wTemp = wVal ? `${wVal.high}°` : '--';
                    const oTemp = oVal ? `${oVal.high}°` : '--';
                    const nTemp = nVal ? `${nVal.high}°` : '--';
                    const mTemp = mVal ? `${mVal.high}°` : '--';

                    const valArr = [wVal?.high, oVal?.high, nVal?.high, mVal?.high].filter(v => typeof v === 'number');
                    const avg = valArr.length > 0 ? (valArr.reduce((a, b) => a + b, 0) / valArr.length).toFixed(1) + '°' : '--';

                    logBuffer.push(` ${targetDate} | ${wTemp.padEnd(7)} | ${oTemp.padEnd(7)} | ${nTemp.padEnd(7)} | ${mTemp.padEnd(7)} | Avg: ${avg}`);

                    const nowTs = Date.now();
                    const payload = {};
                    if (wVal) payload['wunder'] = JSON.stringify({ high: wVal.high, unit: target.unit, ts: nowTs, source: 'wunder' });
                    if (oVal) payload['open_meteo'] = JSON.stringify({ high: oVal.high, unit: target.unit, ts: nowTs, source: 'open_meteo' });
                    if (nVal) payload['noaa'] = JSON.stringify({ high: nVal.high, unit: target.unit, ts: nowTs, source: 'noaa' });
                    if (mVal) payload['met_no'] = JSON.stringify({ high: mVal.high, unit: target.unit, ts: nowTs, source: 'met_no' });

                    // // Open-Meteo 多模型明细（适用于 us + intl）
                    // const modelPayload = {};
                    // const modelNameList = Object.keys(modelMapByName);
                    // modelNameList.forEach((name, idx) => {
                    //     const row = modelMapByName[name].get(targetDate);
                    //     if (row && typeof row.high === 'number') {
                    //         modelPayload[`open_meteo_m${idx + 1}`] = JSON.stringify({
                    //             model: name,
                    //             high: row.high,
                    //             unit: target.unit,
                    //             ts: nowTs,
                    //             source: 'open_meteo_model'
                    //         });
                    //     }
                    // });

                    // if (modelMedian !== null) {
                    //     modelPayload['open_meteo_multi'] = JSON.stringify({
                    //         high: Number(modelMedian.toFixed(1)),
                    //         spread: modelSpread !== null ? Number(modelSpread.toFixed(1)) : null,
                    //         count: modelHighs.length,
                    //         unit: target.unit,
                    //         ts: nowTs,
                    //         source: 'open_meteo_multi'
                    //     });
                    // }

                    // Object.assign(payload, modelPayload);

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
                const gap = this.randInt(3000, 5000);
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
            const response = await axios.get(target.wunderUrl, {
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
            const result = [];

            for (const key in rawData) {
                const item = rawData[key];
                if (item?.b && item.b.calendarDayTemperatureMax && item.b.validTimeLocal) {
                    const dates = item.b.validTimeLocal;
                    const highs = item.b.calendarDayTemperatureMax; // 这是 F

                    if (Array.isArray(dates) && Array.isArray(highs)) {
                        for (let i = 0; i < Math.min(dates.length, highs.length); i++) {
                            const timeStr = dates[i];
                            let temp = highs[i]; // Raw is F

                            if (timeStr && typeof temp === 'number') {
                                const dateStr = dayjs(timeStr).format('YYYY-MM-DD');

                                // 如果配置要求 C，则转换
                                if (target.unit === 'C') {
                                    temp = this.fToC(temp);
                                }

                                result.push({ date: dateStr, high: temp });
                            }
                        }
                        if (result.length > 0) break;
                    }
                }
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
            const url = `https://api.open-meteo.com/v1/forecast?latitude=${target.lat}&longitude=${target.lon}&daily=temperature_2m_max&timezone=auto&forecast_days=5&temperature_unit=${unitParam}`;

            const response = await axios.get(url, { timeout: 8000 });
            const daily = response.data?.daily;
            if (!daily || !daily.time || !daily.temperature_2m_max) return [];

            const result = [];

            for (let i = 0; i < daily.time.length; i++) {
                const dateStr = daily.time[i];
                const temp = daily.temperature_2m_max[i];
                if (dateStr && typeof temp === 'number') {
                    result.push({ date: dateStr, high: Math.round(temp) });
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

        await Promise.all(modelNames.map(async (model) => {
            try {
                const url = `https://api.open-meteo.com/v1/forecast?latitude=${target.lat}&longitude=${target.lon}&daily=temperature_2m_max&timezone=auto&forecast_days=5&temperature_unit=${unitParam}&models=${model}`;
                const response = await axios.get(url, { timeout: 8000 });
                const daily = response.data?.daily;
                if (!daily?.time || !daily?.temperature_2m_max) {
                    out[model] = [];
                    return;
                }

                out[model] = daily.time.map((dateStr, idx) => {
                    const temp = daily.temperature_2m_max[idx];
                    if (!dateStr || typeof temp !== 'number') return null;
                    return { date: dateStr, high: Math.round(temp) };
                }).filter(Boolean);
            } catch (e) {
                out[model] = [];
            }
        }));

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
            for (const point of series) {
                const iso = point?.time;
                const tempC = point?.data?.instant?.details?.air_temperature;
                if (!iso || typeof tempC !== 'number') continue;

                const dateStr = dayjs(iso).tz(target.tz).format('YYYY-MM-DD');
                const prev = maxByDate.get(dateStr);
                if (prev === undefined || tempC > prev) {
                    maxByDate.set(dateStr, tempC);
                }
            }

            const today = dayjs().tz(target.tz).startOf('day');
            const result = [];
            for (let i = 0; i < 5; i++) {
                const d = today.add(i, 'day').format('YYYY-MM-DD');
                if (!maxByDate.has(d)) continue;
                let temp = maxByDate.get(d);
                if (target.unit === 'F') temp = this.cToF(temp);
                else temp = Math.round(temp);
                result.push({ date: d, high: temp });
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

            const pointUrl = `https://api.weather.gov/points/${target.lat},${target.lon}`;
            const pointRes = await axios.get(pointUrl, {
                headers: { 'User-Agent': 'PolyBot_Forecast/1.0' },
                timeout: 8000,
                proxy: proxyConfig || false
            });
            const { gridId, gridX, gridY } = pointRes.data.properties;

            const forecastUrl = `https://api.weather.gov/gridpoints/${gridId}/${gridX},${gridY}/forecast`;
            const foreRes = await axios.get(forecastUrl, {
                headers: { 'User-Agent': 'PolyBot_Forecast/1.0' },
                timeout: 8000,
                proxy: proxyConfig || false
            });

            const periods = foreRes.data.properties.periods;
            const result = [];
            const seenDates = new Set();

            for (const p of periods) {
                if (p.isDaytime) {
                    const dateStr = dayjs(p.startTime).tz(target.tz).format('YYYY-MM-DD');
                    if (!seenDates.has(dateStr)) {
                        let temp = p.temperature; // Raw is F

                        // 如果配置要求 C，则转换
                        if (target.unit === 'C') {
                            temp = this.fToC(temp);
                        }

                        result.push({ date: dateStr, high: temp });
                        seenDates.add(dateStr);
                    }
                }
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