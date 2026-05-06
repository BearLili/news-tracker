import axios from 'axios';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import timezone from 'dayjs/plugin/timezone.js';
import BaseCollector from '../core/BaseCollector.js';
import redis from '../core/redis.js';
import logger from '../utils/logger.js';

dayjs.extend(utc);
dayjs.extend(timezone);

// IBM/TWC API key（与 Wunderground 网站客户端公开使用的相同）
const TWC_API_KEY = 'e1f10a1e78da46f5b10a1e78da96f525';

/**
 * 结算数据采集器
 *
 * 用途：抓 Wunderground 历史 METAR 实测数据（与 Polymarket 裁决数据同源），
 *      用于策略落地后的对账，不参与预测。
 *
 * 接口：https://api.weather.com/v1/location/{ICAO}:9:{COUNTRY}/observations/historical.json
 * 频率：30 分钟
 * 范围：本地日期"今天 + 昨天"两天
 *      昨天数据用于覆盖跨日时段（API 数据延迟，午夜后昨天数据可能还没拉全）
 *
 * 存储：poly:settlement:{station}:{date} (Hash)
 *  - station, date, unit
 *  - high, low（本地日聚合的小时观测最大/最小温度）
 *  - obs_count（观测点数量，>20 可认为完整）
 *  - last_obs_ts（最后一条观测的 UTC 时间戳，用于判断数据完整性）
 *  - updated_at
 */
class WeatherSettlementCollector extends BaseCollector {
    constructor() {
        super('weather_settlement', 30 * 60 * 1000); // 30 分钟
        this.targets = [];
    }

    sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
    randInt(min, max) { return Math.floor(Math.random() * (max - min + 1)) + min; }

    async loadCitiesConfig() {
        try {
            const rawData = await redis.hgetall('poly:config:cities');
            this.targets = Object.values(rawData).map(s => {
                try { return JSON.parse(s); } catch (e) { return null; }
            }).filter(c => c && c.fetchData === true);
            logger.info(`📋 [Settlement] Loaded ${this.targets.length} cities`);
        } catch (e) {
            logger.error(`❌ [Settlement] Load Config Error: ${e.message}`);
        }
    }

    /**
     * 采集单个城市的 historical METAR 数据，按本地日聚合
     * @returns {Object} byDate: { 'YYYY-MM-DD': { high, low, count, lastObsTs } }
     */
    async fetchSettlement(target) {
        const country = (target.country || '').toUpperCase();
        if (!country || !target.station) {
            throw new Error('missing country/station');
        }

        // 本地日: 今天 + 昨天
        const now = dayjs().tz(target.tz);
        const todayLocal = now.format('YYYY-MM-DD');
        const yesterdayLocal = now.subtract(1, 'day').format('YYYY-MM-DD');

        // API 用 UTC 日期。为了拿到当地"昨天 + 今天"完整观测，
        // 需要查询 [本地昨天 00:00 - 1天, 本地今天 23:59 + 1天] 的 UTC 范围
        // 简化做法：UTC 起止日期向外各扩 1 天
        const startUtc = now.subtract(2, 'day').utc().format('YYYYMMDD');
        const endUtc = now.add(1, 'day').utc().format('YYYYMMDD');

        const units = target.unit === 'C' ? 'm' : 'e';
        const url = `https://api.weather.com/v1/location/${target.station}:9:${country}/observations/historical.json` +
                    `?apiKey=${TWC_API_KEY}&units=${units}&startDate=${startUtc}&endDate=${endUtc}`;

        const response = await axios.get(url, {
            timeout: 15000,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
        });

        const obs = response.data?.observations || [];
        const byDate = {};

        for (const o of obs) {
            const t = o?.temp;
            const ts = o?.valid_time_gmt;
            if (typeof t !== 'number' || typeof ts !== 'number') continue;

            const localDate = dayjs.unix(ts).tz(target.tz).format('YYYY-MM-DD');
            // 只保留今天和昨天
            if (localDate !== todayLocal && localDate !== yesterdayLocal) continue;

            if (!byDate[localDate]) {
                byDate[localDate] = { temps: [], lastObsTs: 0, count: 0 };
            }
            const slot = byDate[localDate];
            slot.temps.push(t);
            if (ts > slot.lastObsTs) slot.lastObsTs = ts;
            slot.count++;
        }

        // 聚合 high/low
        const result = {};
        for (const [date, slot] of Object.entries(byDate)) {
            if (slot.temps.length === 0) continue;
            result[date] = {
                high: Math.max(...slot.temps),
                low: Math.min(...slot.temps),
                count: slot.count,
                lastObsTs: slot.lastObsTs
            };
        }
        return result;
    }

    async saveSettlement(target, byDate) {
        const entries = Object.entries(byDate);
        if (entries.length === 0) return;

        const pipe = redis.pipeline();
        const nowMs = Date.now();
        for (const [date, info] of entries) {
            const key = `poly:settlement:${target.station}:${date}`;
            pipe.hmset(key, {
                station: target.station,
                name: target.name || '',
                date,
                unit: target.unit,
                high: info.high,
                low: info.low,
                obs_count: info.count,
                last_obs_ts: info.lastObsTs,
                updated_at: nowMs
            });
            pipe.expire(key, 14 * 24 * 60 * 60); // 14 天 TTL
        }
        await pipe.exec();
    }

    async fetch() {
        await this.loadCitiesConfig();
        if (this.targets.length === 0) {
            logger.warn('⚠️ [Settlement] No targets, skipping cycle');
            return;
        }

        logger.info(`🧾 [Settlement] Start cycle, ${this.targets.length} cities`);
        let success = 0, failed = 0;

        for (let i = 0; i < this.targets.length; i++) {
            const target = this.targets[i];
            try {
                const byDate = await this.fetchSettlement(target);
                await this.saveSettlement(target, byDate);

                const summary = Object.entries(byDate)
                    .sort(([a], [b]) => a.localeCompare(b))
                    .map(([d, info]) => `${d}:H=${info.high}°${target.unit}/L=${info.low}°(${info.count})`)
                    .join(' | ');
                logger.info(`✅ [Settlement|${target.station}|${target.name}] ${summary || 'no data'}`);
                success++;
            } catch (e) {
                logger.warn(`❌ [Settlement|${target.station}|${target.name}] ${e.message}`);
                failed++;
            }

            // 城市间 1.5-3.5 秒随机延时，避免 API 限流
            if (i < this.targets.length - 1) {
                await this.sleep(this.randInt(1500, 3500));
            }
        }

        logger.info(`🧾 [Settlement] Done. success=${success}, failed=${failed}`);
        return { timestamp: Date.now(), success, failed };
    }
}

export default WeatherSettlementCollector;
