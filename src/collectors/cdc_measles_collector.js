import axios from 'axios';
import BaseCollector from '../core/BaseCollector.js';
import redis from '../core/redis.js'; // 引入 redis
import logger from '../utils/logger.js';

class CDCMeaslesCollector extends BaseCollector {
  constructor() {
    super('cdc_measles', 60 * 60 * 1000); // Key: poly:latest:cdc_measles
    this.urls = {
      annual: 'https://www.cdc.gov/wcms/vizdata/measles/measles_hosp.json',
      weekly: 'https://www.cdc.gov/wcms/vizdata/measles/MeaslesCasesWeekly.json'
    };
    this.lastHash = null;
    this.isFirstRun = true;
  }

  /**
   * 辅助：根据日期字符串计算是一年中的第几周
   * e.g. "2026-01-04" -> 1
   */
  getWeekNumber(dateStr) {
    if (!dateStr) return 0;
    const d = new Date(dateStr);
    const date = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()));
    const yearStart = new Date(Date.UTC(date.getUTCFullYear(), 0, 1));
    const weekNo = Math.ceil((((date - yearStart) / 86400000) + 1) / 7);
    return weekNo;
  }

  async fetch() {
    try {
      // 并行获取
      const [annualRes, weeklyRes] = await Promise.all([
        axios.get(`${this.urls.annual}?_t=${Date.now()}`, { timeout: 8000 }),
        axios.get(`${this.urls.weekly}?_t=${Date.now()}`, { timeout: 8000 })
      ]);
      
      const annualData = annualRes.data;
      const weeklyData = weeklyRes.data;
      const currentYear = '2026'; 

      // --- 1. 处理归档 (Archiving Logic) ---
      // 只有在获取到周数据时才处理
      if (Array.isArray(weeklyData)) {
         // 过滤出 2026 年的数据
         const currentYearWeeks = weeklyData.filter(i => i.week_start?.startsWith(currentYear));
         
         for (const item of currentYearWeeks) {
             const weekNum = this.getWeekNumber(item.week_start);
             if (weekNum <= 0) continue;

             const archiveKey = `poly:archive:measles:${currentYear}:${weekNum}`;
             
             // 如果是首次运行，或者 Redis 里没有，就存进去
             // 这里不用 await redis.exists 是为了提高效率，直接用 setnx (set if not exists) 更好，
             // 但为了逻辑统一，我们用 exists 检查
             let shouldSave = false;
             
             if (this.isFirstRun) {
                 shouldSave = true; // 启动时强制覆盖/确认
             } else {
                 const exists = await redis.exists(archiveKey);
                 if (!exists) shouldSave = true;
             }

             if (shouldSave) {
                 const archivePayload = {
                     year: parseInt(currentYear),
                     week: weekNum,
                     week_start: item.week_start,
                     week_end: item.week_end,
                     cases: parseInt(item.cases, 10) || 0,
                     archived_at: Date.now()
                 };
                 
                 await redis.set(archiveKey, JSON.stringify(archivePayload));
                 if(this.isFirstRun) logger.debug(`[Init] Archived Measles ${currentYear}-W${weekNum}`);
             }
         }
      }

      // --- 2. 处理最新状态 (Latest Logic - 原有逻辑) ---
      const annualCount = parseInt(annualData[currentYear]?.total_cases?.[0] || 0, 10);
      
      let weeklySum = 0;
      if (Array.isArray(weeklyData)) {
        weeklySum = weeklyData
          .filter(i => i.week_start?.startsWith(currentYear))
          .reduce((sum, i) => sum + (parseInt(i.cases, 10) || 0), 0);
      }

      const finalCount = Math.max(annualCount, weeklySum);
      
      // 首次运行结束后，关闭标记
      if (this.isFirstRun) {
          this.isFirstRun = false;
          logger.info('[Measles] Initialization check complete.');
      }

      // 内存去重逻辑 (控制 poly:feed 推送)
      const currentHash = `${finalCount}-${annualCount}-${weeklySum}`;
      if (this.lastHash === currentHash) return null;
      
      this.lastHash = currentHash;
      
      const result = {
        final_count: finalCount,
        breakdown: { annual: annualCount, weekly_sum: weeklySum },
        source_used: annualCount > weeklySum ? 'annual' : 'weekly_sum'
      };

      const _weekData = weeklyData.filter(i => i.week_start?.startsWith(currentYear)) || [];
      const week = this.getWeekNumber(_weekData[_weekData.length - 1]?.week_start) || '';
      logger.info(`📊【Measles Update】 Week: ${currentYear}-W${week}, Total: ${finalCount}`);
      return result;

    } catch (error) {
      logger.error(`Measles fetch failed: ${error.message}`);
      throw error; 
    }
  }
}

export default CDCMeaslesCollector;