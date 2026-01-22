import axios from 'axios';
import BaseCollector from '../core/BaseCollector.js';
import logger from '../utils/logger.js';

class WeatherMonitorCollector extends BaseCollector {
  constructor() {
    // 监控源：每 10 分钟轮询一次 (600000ms)
    // 既能保证数据较新，又不会对 NOAA/OpenMeteo 造成太大压力
    super('weather_monitor_global', 10 * 60 * 1000);

    // 城市配置 (直接复用 Manus 的配置，但去掉了经纬度中不必要的部分，保留核心)
    this.targets = [
      // === 美国城市 (NOAA) ===
      { name: 'New York JFK', code: 'KJFK', type: 'us', stationId: 'KJFK' },
      { name: 'New York LGA', code: 'KLGA', type: 'us', stationId: 'KLGA' },
      { name: 'Atlanta', code: 'KATL', type: 'us', stationId: 'KATL' },
      { name: 'Dallas', code: 'KDFW', type: 'us', stationId: 'KDFW' },
      { name: 'Seattle', code: 'KSEA', type: 'us', stationId: 'KSEA' },

      // === 国际城市 (Open-Meteo) ===
      { name: 'London', code: 'EGLL', type: 'intl', lat: 51.4700, lon: -0.4543 },
      { name: 'Seoul', code: 'RKSI', type: 'intl', lat: 37.4602, lon: 126.4407 },
      { name: 'Toronto', code: 'CYYZ', type: 'intl', lat: 43.6777, lon: -79.6248 },
      { name: 'Buenos Aires', code: 'SAEZ', type: 'intl', lat: -34.8222, lon: -58.5358 }
    ];
  }

  /**
   * 核心采集入口
   * BaseCollector 会自动调用此方法，并处理返回值存入 Redis
   */
  async fetch() {
    // 并行请求所有城市
    const promises = this.targets.map(target => {
      if (target.type === 'us') {
        return this.fetchNOAA(target);
      } else {
        return this.fetchOpenMeteo(target);
      }
    });

    const results = await Promise.all(promises);

    // 过滤掉失败的 (null) 结果
    const validResults = results.filter(r => r !== null);

    if (validResults.length === 0) return null;

    // 返回给 BaseCollector 进行统一封装和推送
    return {
      timestamp: Date.now(),
      summary: `Collected ${validResults.length}/${this.targets.length} cities`,
      cities: validResults
    };
  }

  // --- 下面复用 Manus 的逻辑，稍作类方法改造 ---

  async fetchNOAA(target) {
    try {
      const url = `https://api.weather.gov/stations/${target.stationId}/observations/latest`;
      const response = await axios.get(url, {
        headers: {
          // NOAA 必须要求 User-Agent，否则 403
          'User-Agent': 'WeatherMonitor/1.0 (PolyBot)'
        },
        timeout: 10000
      });

      const props = response.data?.properties;
      const tempC = props?.temperature?.value;

      if (tempC === null || tempC === undefined) return null;

      // 转换为华氏度
      const tempF = Math.round((tempC * 9 / 5) + 32);

      return {
        city: target.name,
        code: target.code,
        provider: 'noaa', // 标记数据源
        temp_c: Math.round(tempC * 10) / 10,
        temp_f: tempF,
        obs_time: props.timestamp
      };
    } catch (error) {
      logger.warn(`[NOAA] Failed for ${target.name}: ${error.message}`);
      return null;
    }
  }

  async fetchOpenMeteo(target) {
    try {
      // 获取当天的日期 YYYY-MM-DD
      const date = new Date().toISOString().split('T')[0];
      // 请求 daily max 数据
      const url = `https://archive-api.open-meteo.com/v1/archive?latitude=${target.lat}&longitude=${target.lon}&start_date=${date}&end_date=${date}&daily=temperature_2m_max&timezone=auto`;
      
      const response = await axios.get(url, { timeout: 10000 });
      const tempC = response.data?.daily?.temperature_2m_max?.[0];

      if (tempC === null || tempC === undefined) return null;

      const tempF = Math.round((tempC * 9 / 5) + 32);

      return {
        city: target.name,
        code: target.code,
        provider: 'open_meteo', // 标记数据源
        temp_c: Math.round(tempC * 10) / 10,
        temp_f: tempF,
        obs_time: date // OpenMeteo 只有日期，没有具体时间
      };
    } catch (error) {
      logger.warn(`[OpenMeteo] Failed for ${target.name}: ${error.message}`);
      return null;
    }
  }
}

export default WeatherMonitorCollector;