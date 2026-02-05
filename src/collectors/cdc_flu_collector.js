import axios from 'axios';
import BaseCollector from '../core/BaseCollector.js';
import redis from '../core/redis.js';
import logger from '../utils/logger.js';

class CDCFluCollector extends BaseCollector {
  constructor() {
    super('cdc_flu',60 * 60 * 1000); // Key: poly:latest:cdc_flu
    this.baseUrl = 'https://www.cdc.gov/fluview/modules';
    
    // 标记：是否是进程启动后的第一次运行
    this.isFirstRun = true;
  }

  getTargetWeeks() {
    const now = new Date();
    const startOfYear = new Date(now.getFullYear(), 0, 1);
    const days = Math.floor((now - startOfYear) / (24 * 60 * 60 * 1000));
    const currentWeek = Math.ceil((days + 1) / 7) - 1;
    const currentYear = now.getFullYear();
    const targets = [];
    
    for (let i = 0; i < 3; i++) {
      let w = currentWeek - i;
      let y = currentYear;
      while (w <= 0) {
        y = y - 1;
        w = 53 + w;
      }
      targets.push({ year: y, week: w });
    }
    return targets;
  }

  async fetch() {
    const targets = this.getTargetWeeks();
    const newFindings = [];

    for (const { year, week } of targets) {
      const archiveKey = `poly:archive:flu:${year}:${week}`;

      try {
        // --- 核心修改：如果是首次运行，跳过缓存检查 ---
        if (!this.isFirstRun) {
            const exists = await redis.exists(archiveKey);
            if (exists) continue;
        }

        // 构造请求
        const url = `${this.baseUrl}/${year}-week-${ week<10 ? `0${week}` : week}/Weekly-Data-Bites.json`;
        
        // 首次运行时，不管找没找到，都打印个 debug 告诉我们正在强制检查
        if (this.isFirstRun) logger.debug(`[Init] Force probing Flu: ${year}-W${week}`);

        const { data } = await axios.get(`${url}?_t=${Date.now()}`, { timeout: 5000 });

        if (data && Array.isArray(data)) {
          const fluItem = data.find(item => item.Type === 'FluSurv-NET');
          
          const parsedData = {
            year,
            week,
            rate: fluItem ? parseFloat(fluItem.Value) : 0,
            raw_text: fluItem?.Text,
            full_source: url,
            archived_at: Date.now()
          };

          // 存入归档 (如果是 FirstRun，这里会覆盖旧值，这是预期的自我修复行为)
          await redis.set(archiveKey, JSON.stringify(parsedData));
          newFindings.push(parsedData);
          
          logger.info(`📊【Flu Update】 Week: ${year}-W${week}, Rate: ${parsedData.rate}`);
        }

      } catch (error) {
        if (!error.response || error.response.status !== 404) {
          logger.warn(`Flu probe error ${year}-W${week}: ${error.message}`);
        }
      }
    }

    // 第一次运行结束后，关闭标记
    if (this.isFirstRun) {
        this.isFirstRun = false;
        logger.info('[Flu] Initialization check complete.');
    }

    if (newFindings.length === 0) return null;

    return { updates: newFindings };
  }
}

export default CDCFluCollector;