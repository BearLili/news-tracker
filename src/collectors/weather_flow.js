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

  async fetch() {
    await this.loadCitiesConfig();
    
    if (this.targets.length === 0) {
        logger.warn('⚠️ No targets loaded. Skipping cycle.');
        return;
    }

    logger.info('🔮 Starting 4-Day Forecast Collection (Respecting Config Units)...');
    
    const tasks = this.targets.map(async (target) => {
      // 1. 并行采集
      const results = await Promise.allSettled([
        this.fetchWunderground(target), 
        this.fetchOpenMeteo(target),    
        target.type === 'us' ? this.fetchNOAA(target) : Promise.resolve([]) 
      ]);

      const wunderData = results[0].status === 'fulfilled' ? results[0].value : [];
      const openData = results[1].status === 'fulfilled' ? results[1].value : [];
      const noaaData = results[2].status === 'fulfilled' ? results[2].value : [];

      const today = dayjs().tz(target.tz);
      
      const logBuffer = [];
      logBuffer.push(`\n📊 [${target.name} | ${target.station}] Forecast Summary (Next 4 Days) [Unit: ${target.unit}]:`);
      logBuffer.push(`   Date       | Wunder  | OpenM   | NOAA    | Consensus`);
      logBuffer.push(`   -----------|---------|---------|---------|-----------`);

      for (let i = 0; i < 4; i++) {
        const targetDate = today.add(i, 'day').format('YYYY-MM-DD');
        const key = `poly:forecast:${target.station}:${targetDate}`;
        
        // 这里的 value 已经是经过 fetch 方法处理过、符合 target.unit 的数值了
        const wVal = wunderData.find(d => d.date === targetDate);
        const oVal = openData.find(d => d.date === targetDate);
        const nVal = noaaData.find(d => d.date === targetDate);

        const wTemp = wVal ? `${wVal.high}°` : '--';
        const oTemp = oVal ? `${oVal.high}°` : '--';
        const nTemp = nVal ? `${nVal.high}°` : '--';

        // 计算均值
        const valArr = [wVal?.high, oVal?.high, nVal?.high].filter(v => typeof v === 'number');
        const avg = valArr.length > 0 ? (valArr.reduce((a,b)=>a+b,0)/valArr.length).toFixed(1) + '°' : '--';

        logBuffer.push(`   ${targetDate} | ${wTemp.padEnd(7)} | ${oTemp.padEnd(7)} | ${nTemp.padEnd(7)} | Avg: ${avg}`);

        // 存入 Redis，带上 unit 字段，方便回溯
        const payload = {};
        if (wVal) payload['wunder'] = JSON.stringify({ high: wVal.high, unit: target.unit, ts: Date.now() });
        if (oVal) payload['open_meteo'] = JSON.stringify({ high: oVal.high, unit: target.unit, ts: Date.now() });
        if (nVal) payload['noaa'] = JSON.stringify({ high: nVal.high, unit: target.unit, ts: Date.now() });

        if (Object.keys(payload).length > 0) {
          payload['updated_at'] = Date.now();
          await redis.hmset(key, payload);
          await redis.expire(key, 7 * 24 * 60 * 60);
        }
      }
      
      logBuffer.push(`✅ [${target.station}] Processed.`);
      logger.info(logBuffer.join('\n'));
    });

    await Promise.all(tasks);
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
                         // validTimeLocal 形如 "2026-05-14T07:00:00+0900"，
                         // 必须按 target.tz format，否则跑在 UTC 服务器上日期会漂一天
                         const dateStr = dayjs(timeStr).tz(target.tz).format('YYYY-MM-DD');
                         
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
      // 动态设置 unit 参数
      const unitParam = target.unit === 'F' ? 'fahrenheit' : 'celsius';
      const url = `https://api.open-meteo.com/v1/forecast?latitude=${target.lat}&longitude=${target.lon}&daily=temperature_2m_max&timezone=auto&forecast_days=5&temperature_unit=${unitParam}`;
      
      const response = await axios.get(url, { timeout: 8000 });
      const daily = response.data?.daily;

      if (!daily || !daily.time || !daily.temperature_2m_max) return [];

      const result = [];
      for (let i = 0; i < daily.time.length; i++) {
        const dateStr = daily.time[i]; 
        const temp = daily.temperature_2m_max[i]; // 已经是目标单位
        
        if (dateStr && typeof temp === 'number') {
          // OpenMeteo 有时会给小数，取整保持一致
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
      // logger.warn(`[NOAA] Failed ${target.station}: ${e.message}`);
      return [];
    }
  }

}

export default WeatherForecastCollector;