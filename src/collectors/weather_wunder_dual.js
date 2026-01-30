import axios from 'axios';
import * as cheerio from 'cheerio';
import { chromium } from 'playwright';
import BaseCollector from '../core/BaseCollector.js';
import logger from '../utils/logger.js';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import timezone from 'dayjs/plugin/timezone.js';
import Redis from 'ioredis';
import pLimit from 'p-limit';

dayjs.extend(utc);
dayjs.extend(timezone);

const redis = new Redis({
  host: '127.0.0.1',
  port: 6379,
  retryStrategy: times => Math.min(times * 50, 2000)
});

class WunderDualSource extends BaseCollector {
  constructor() {
    // 基础心跳 10s (由 JSON 任务主导)
    super('wunder_dual_monitor', 12 * 1000);
    
    this.cities = [];
    
    // Browser 状态
    this.browser = null;
    this.lastRestartTime = Date.now();
    this.RESTART_INTERVAL = 10 * 60 * 1000; 
    this.isBrowserInitializing = false;

    // DOM 任务调度计时器
    this.lastDomRunTime = 0;
  }

  // --- 配置加载 ---
  async loadCitiesConfig() {
    try {
      const rawData = await redis.hgetall('poly:config:cities');
      this.cities = Object.values(rawData).map(s => {
        try { return JSON.parse(s); } catch (e) { return null; }
      }).filter(Boolean);
    } catch (e) {
      logger.error(`❌ Load Config Error: ${e.message}`);
    }
  }

  convertTemperature(val, fromUnit, toUnit) {
    if (!fromUnit || !toUnit || fromUnit === toUnit) return val;
    if (fromUnit === 'F' && toUnit === 'C') return Math.round((val - 32) / 1.8);
    if (fromUnit === 'C' && toUnit === 'F') return Math.round(val * 1.8 + 32);
    return val;
  }

  // --- Browser 管理 ---
  async ensureBrowser() {
    if (this.isBrowserInitializing) return;
    const now = Date.now();
    const needsRestart = !this.browser || (now - this.lastRestartTime > this.RESTART_INTERVAL);

    if (needsRestart) {
      this.isBrowserInitializing = true;
      try {
        if (this.browser) await this.browser.close().catch(() => {});
        this.browser = await chromium.launch({
          headless: true,
          args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu', '--no-zygote']
        });
        this.lastRestartTime = now;
        logger.info('🚀 Browser (re)started for DOM tasks.');
      } catch (err) {
        logger.error(`❌ Browser launch failed: ${err.message}`);
      } finally {
        this.isBrowserInitializing = false;
      }
    }
  }

  // --- 任务 A: Axios/JSON (10s) ---
  async getAxiosProxy() {
    try {
      const proxyStr = await redis.srandmember('poly:proxylist');
      if (!proxyStr) return null;
      const [host, port, user, pass] = proxyStr.split(':');
      return {
        protocol: 'http',
        host: host,
        port: parseInt(port),
        auth: user ? { username: user, password: pass } : undefined
      };
    } catch (e) { return null; }
  }

  /**
   * 优化后的 JSON 采集逻辑：
   * 1. 过滤掉数组 (排除附近站点列表)
   * 2. 过滤掉非数字 (排除预测数据)
   * 3. 对所有候选数据按时间戳倒序排列，取最新值
   */
  async scrapeJson(cityConfig) {
    const url = `https://www.wunderground.com/weather/${cityConfig.country}/${cityConfig.city}/${cityConfig.station}`;
    const result = { meta: cityConfig, success: false, current: null, station_time: null, source: 'json' };

    try {
      const proxy = await this.getAxiosProxy();
      const response = await axios.get(url, {
        proxy: proxy || false,
        timeout: 12000,
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
          'Accept-Language': 'en-US,en;q=0.9',
          'Cache-Control': 'no-cache'
        }
      });

      const $ = cheerio.load(response.data);
      const scriptContent = $('#app-root-state').html();
      
      if (scriptContent) {
        const rawData = JSON.parse(scriptContent);
        
        // 收集器：存放所有合法的观测数据快照
        const candidates = [];

        for (const key in rawData) {
          const item = rawData[key];
          
          // 核心过滤：
          // 1. item.b 必须存在
          // 2. !Array.isArray(item.b) -> 排除掉是数组的情况（即排除了附近站点列表）
          // 3. typeof item.b.temperature === 'number' -> 排除掉预测数据(数组)或其他非温度对象
          if (item?.b && !Array.isArray(item.b) && typeof item.b.temperature === 'number') {
            
            // 4. 必须包含有效的时间字段
            const timeStr = item.b.validTimeLocal || item.b.obsTimeLocal;
            
            if (timeStr) {
              candidates.push({
                data: item.b,
                ts: dayjs(timeStr).valueOf() // 转换成时间戳方便比较
              });
            }
          }
        }

        // 择优录取：按时间倒序排列，取第一个（最新的）
        if (candidates.length > 0) {
          // 排序：最新的在最前面 (timestamp desc)
          candidates.sort((a, b) => b.ts - a.ts);
          
          const bestMatch = candidates[0].data;
          
          result.current = this.convertTemperature(bestMatch.temperature, 'F', cityConfig.unit);
          result.station_time = bestMatch.validTimeLocal || bestMatch.obsTimeLocal;
          result.success = true;
        }
      }
    } catch (error) {
      // Axios 静默失败，依靠日志排查
    }
    return result;
  }

  // --- 任务 B: Playwright/DOM (30s) ---
  async getDomProxy() {
    try {
      const proxyStr = await redis.srandmember('poly:proxylist');
      if (!proxyStr) return null;
      const parts = proxyStr.split(':');
      if (parts.length < 4) return null;
      return { server: `http://${parts[0]}:${parts[1]}`, username: parts[2], password: parts[3] };
    } catch (e) { return null; }
  }

  async scrapeDom(cityConfig) {
    if (!this.browser) return { success: false, source: 'dom' };
    
    const url = `https://www.wunderground.com/weather/${cityConfig.country}/${cityConfig.city}/${cityConfig.station}?_t=${Date.now()}`;
    const result = { meta: cityConfig, success: false, current: null, station_time: null, source: 'dom' };
    let context = null, page = null;

    try {
      const proxyConfig = await this.getDomProxy();
      context = await this.browser.newContext({
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        viewport: { width: 800, height: 600 },
        proxy: proxyConfig || undefined
      });

      await context.setExtraHTTPHeaders({ 'Cache-Control': 'no-cache', 'Pragma': 'no-cache' });

      page = await context.newPage();
      await page.route('**/*.{png,jpg,jpeg,gif,svg,mp4,woff,woff2,css}', route => route.abort());

      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
      await page.waitForSelector('.current-temp .wu-value', { timeout: 15000 });

      const pageData = await page.evaluate(() => {
        const valueText = document.querySelector('.current-temp .wu-value')?.innerText || "";
        const unitText = document.querySelector('.current-temp .wu-label')?.innerText || "";
        const timeText = document.querySelector('.city-conditions .timestamp strong')?.innerText || "";
        return {
          rawValue: parseInt(valueText.trim()),
          pageUnit: unitText.includes('C') ? 'C' : 'F',
          stationTime: timeText.trim()
        };
      });

      if (!isNaN(pageData.rawValue)) {
        result.current = this.convertTemperature(pageData.rawValue, pageData.pageUnit, cityConfig.unit);
        result.station_time = pageData.stationTime;
        result.success = true;
      }
    } catch (error) {
      logger.error(`❌ [DOM] ${cityConfig.name} Error: ${error.message}`);
    } finally {
      if (page) await page.close().catch(() => {});
      if (context) await context.close().catch(() => {});
    }
    return result;
  }

  // --- 核心同步与推送 ---
  async syncToRedis(data) {
    if (!data.success) return;
    const { meta, current, source } = data;
    const date_std = dayjs().tz(meta.tz).format('YYYY-MM-DD');
    const key = `poly:latest:weather:${meta.station}`;

    const isSameDay = date_std && data?.station_time && this.isSameDay(data?.station_time, date_std);
    if (!isSameDay) return;

    try {
      // 1. 获取旧缓存
      const cached = await redis.hmget(key, 'rt_high', 'target_date');
      const cachedHigh = cached[0];
      const cachedDate = cached[1];

      let rtRollingHigh = current;
      let isNewRecord = false; // 🔥 标记

      // 2. 比较最高温逻辑
      if (cachedHigh && cachedDate === date_std) {
        const oldHigh = parseFloat(cachedHigh);
        if (!isNaN(oldHigh)) {
          if (oldHigh > current) {
            // 旧的更高，保持旧值
            rtRollingHigh = oldHigh;
          } else if (current > oldHigh) {
            // 当前更高 -> 产生新纪录 🔥
            isNewRecord = true;
            rtRollingHigh = current;
          }
        }
      }

      const pipeline = redis.pipeline();
      
      // 3. 写入 Hash
      pipeline.hmset(key, {
        city_name: meta.name,
        target_date: date_std,
        unit: meta.unit,
        rt_current: current,
        rt_high: rtRollingHigh,
        rt_ts: Date.now(),
        rt_station_time: data.station_time || '',
        updated_at: dayjs().tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss'),
        last_source: source
      });

      // 4. 写入时间序列
      const vizKey = `poly:viz:series:${meta.station}`;
      const ts = Date.now();
      pipeline.zadd(vizKey, ts, JSON.stringify({ t: current, s: 'rt', src: source, ts }));
      pipeline.zremrangebyscore(vizKey, '-inf', ts - 86400000);
      
      // 5. 推送
      pipeline.publish(`poly:feed:weather:${meta.station}`, JSON.stringify({ 
        station: meta.station, 
        type: 'rt', 
        source, 
        ts 
      }));
      
      await pipeline.exec();
      
      // 6. 日志输出 (带图标)
      const srcIcon = source === 'json' ? '⚡' : '🐢';
      const fireIcon = isNewRecord ? '🔥' : '';
      
      logger.info(`${srcIcon} [${meta.name}] ${current}°${meta.unit} (High: ${rtRollingHigh}) 【${dayjs().tz(meta.tz).format('YYYY-MM-DD HH:mm:ss')}】 ${fireIcon}`);

    } catch (e) {
      logger.error(`❌ Redis Sync Error: ${e.message}`);
    }
  }

  /**
   * 判断 station_time 和 date_std 是否为同一天
   * @param {string} stationTime - "5:08 AM CST on January 30, 2026" 或 "2026-01-30T06:38:32-0500"
   * @param {string} dateStd - "2026-01-30"
   * @returns {boolean}
   */
  isSameDay(stationTime, dateStd) {
    if (!stationTime || !dateStd) return false;

    let targetDateStr = '';

    // 格式 1: "5:08 AM CST on January 30, 2026"
    if (stationTime.includes(' on ')) {
        // 以 " on " 分割，取最后一部分 "January 30, 2026"
        const parts = stationTime.split(' on ');
        const datePart = parts[parts.length - 1].trim(); 
        
        // dayjs 可以直接解析 "Month DD, YYYY" 这种标准英语格式
        const d = dayjs(datePart);
        if (d.isValid()) {
            targetDateStr = d.format('YYYY-MM-DD');
        }
    } 
    // 格式 2: "2026-01-30T06:38:32-0500"
    else {
        // 直接截取前 10 位 "2026-01-30"
        // 这样做的好处是完全忽略时区转换，强制读取字面上的“当地日期”
        targetDateStr = stationTime.substring(0, 10);
    }

    return targetDateStr === dateStd;
  }

  // --- 调度器 ---
  async fetch() {
    await this.loadCitiesConfig();
    if (this.cities.length === 0) return;

    const now = Date.now();
    const tasks = [];

    // 1. JSON 任务 (总是执行, 10s)
    const limitJson = pLimit(4);
    const jsonBatch = this.cities.map(city => limitJson(async () => {
      const data = await this.scrapeJson(city);
      await this.syncToRedis(data);
    }));
    tasks.push(Promise.allSettled(jsonBatch));

    // 2. DOM 任务 (每 50s 执行)
    if (now - this.lastDomRunTime >= 50 * 1000) {
      await this.ensureBrowser();
      this.lastDomRunTime = now;
      
      const limitDom = pLimit(3);
      const domBatch = this.cities.map(city => limitDom(async () => {
        await new Promise(r => setTimeout(r, Math.random() * 2000));
        const data = await this.scrapeDom(city);
        await this.syncToRedis(data);
      }));
      
      tasks.push(Promise.allSettled(domBatch));
      logger.info('🕒 Triggering DOM batch sync...');
    }

    await Promise.all(tasks);
    return { timestamp: now };
  }
}

export default WunderDualSource;