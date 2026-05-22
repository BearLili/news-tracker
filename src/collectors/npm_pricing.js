import axios from 'axios';
import BaseCollector from '../core/BaseCollector.js';
import logger from '../utils/logger.js';
import redis from '../core/redis.js';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import timezone from 'dayjs/plugin/timezone.js';

dayjs.extend(utc);
dayjs.extend(timezone);

/**
 * Nasdaq Private Market - 私募公司估值采集器
 *
 * 数据源:
 *   GET <NPM_API_BASE>/<companyId>
 *
 * 返回包含两套同结构的"最新价"字段：
 *   - latest_npm_price : NPM 自家撮合 / 模型给出的每日指示价
 *   - latest_tape_d    : 来源于 SEC Form D 申报的价格
 * 这俩在多数公司日常更新里是一致的（如 Anthropic 完全相同），偶尔会有差异。
 * 我们以 NPM 为准，缺失才回落到 Tape D。
 *
 * NPM 价格每日 13:00 ET 更新。本采集器策略：
 *   - 基础轮询 30s
 *   - 12:30 - 13:30 ET 窗口每轮都打 API（高频捕捉变化）
 *   - 窗口外每 10 轮（约 5 分钟）打一次 API，避免空转浪费
 *
 * Redis schema:
 *   poly:config:companies (hash, field=companyId, value=JSON)
 *     { id, name, fetchData, ... }
 *
 *   poly:npm:<companyId> (hash)
 *     name              公司展示名
 *     current_date      最新数据日期（YYYY-MM-DD）
 *     current_price     最新价（USD/share）
 *     current_iv        最新隐含估值（USD）
 *     current_source    'npm' | 'tape_d'
 *     current_seen_at   首次拿到 current_date 那条数据的时间戳
 *     prev_date         上一个不同日期的快照
 *     prev_price
 *     prev_iv
 *     prev_source
 *     changed_at        current_* 上次发生变化的时间戳
 *     last_check_at     最后一次 fetch 的时间戳（不论是否变化）
 */
class NPMPricingCollector extends BaseCollector {
    constructor() {
        super('npm_pricing', 30 * 1000);
        this.companies = [];
        this.tickCount = 0;
        this.baseUrl = (process.env.NPM_API_BASE
            || 'https://api-npm17-data-company-pricing-review-prod.k8s-prod-1.npmdev.net/api/public/companies')
            .replace(/\/+$/, '');
        // 默认绕开 axios 的 HTTPS_PROXY 自动消费（开发机本地代理会触发 ERR_FR_TOO_MANY_REDIRECTS）。
        // 若部署环境必须走 corp proxy 才能访问 NPM，设置 NPM_USE_PROXY=true 让 axios 复用环境变量。
        this.useEnvProxy = process.env.NPM_USE_PROXY === 'true';
    }

    async loadCompaniesConfig() {
        try {
            const raw = await redis.hgetall('poly:config:companies');
            this.companies = Object.values(raw).map(s => {
                try { return JSON.parse(s); } catch { return null; }
            }).filter(c => c && c.id && c.fetchData !== false);
            logger.debug(`📋 NPM: loaded ${this.companies.length} companies from Redis config`);
        } catch (e) {
            logger.error(`❌ NPM load config error: ${e.message}`);
            this.companies = [];
        }
    }

    /** 判断当前是否处于 13:00 ET 前后的高频窗口 */
    isHotWindow() {
        const et = dayjs().tz('America/New_York');
        const h = et.hour();
        const m = et.minute();
        // 12:30 - 13:30 ET（覆盖时区切换 EST/EDT 都正确，因为 .tz() 已处理）
        return (h === 12 && m >= 30) || (h === 13 && m <= 30);
    }

    async fetchCompany(c) {
        const url = `${this.baseUrl}/${encodeURIComponent(c.id)}`;
        const resp = await axios.get(url, {
            timeout: 10000,
            headers: {
                'Accept': 'application/json',
                'User-Agent': 'news-tracker/1.0'
            },
            // 默认 proxy: false 避开 HTTPS_PROXY 误代理；详见 constructor 注释
            proxy: this.useEnvProxy ? undefined : false
        });
        const d = resp.data || {};
        const npm = d.latest_npm_price;
        const tape = d.latest_tape_d;

        let pick = null, source = null;
        if (npm && typeof npm.implied_valuation === 'number' && npm.date) {
            pick = npm; source = 'npm';
        } else if (tape && typeof tape.implied_valuation === 'number' && tape.date) {
            pick = tape; source = 'tape_d';
        }
        if (!pick) return null;

        return {
            date: pick.date,
            price: typeof pick.price === 'number' ? pick.price : null,
            implied_valuation: pick.implied_valuation,
            source,
            dba_name: d.company?.dba_name || null,
        };
    }

    /**
     * 处理单个公司：拉数据 + 与 Redis 中状态对比 + 必要时落地
     * @returns 变化的描述（用于本轮 fetch 汇总），未变化返回 null
     */
    async processCompany(c) {
        let latest;
        try {
            latest = await this.fetchCompany(c);
        } catch (e) {
            logger.warn(`[NPM] ${c.name || c.id} fetch failed: ${e.message}`);
            return null;
        }
        if (!latest) {
            logger.warn(`[NPM] ${c.name || c.id} no usable pricing data in response`);
            return null;
        }

        const key = `poly:npm:${c.id}`;
        const now = Date.now();
        const existing = await redis.hgetall(key) || {};
        const curDate = existing.current_date;
        const curIv = existing.current_iv ? Number(existing.current_iv) : null;

        const basePayload = {
            name: c.name || latest.dba_name || c.id,
            last_check_at: now,
        };

        let changed = false;
        let reason = '';

        const fillCurrent = (target) => {
            target.current_date = latest.date;
            target.current_price = latest.price == null ? '' : latest.price;
            target.current_iv = latest.implied_valuation;
            target.current_source = latest.source;
            target.current_seen_at = now;
            target.changed_at = now;
        };

        const payload = { ...basePayload };

        if (!curDate) {
            // 首次写入
            fillCurrent(payload);
            changed = true;
            reason = 'initial';
        } else if (latest.date > curDate) {
            // 新日期：把现在的 current 推到 prev
            payload.prev_date = existing.current_date;
            payload.prev_price = existing.current_price ?? '';
            payload.prev_iv = existing.current_iv ?? '';
            payload.prev_source = existing.current_source ?? '';
            fillCurrent(payload);
            changed = true;
            reason = 'new_date';
        } else if (latest.date === curDate
                   && curIv !== null
                   && Math.abs(curIv - latest.implied_valuation) > 1e-6) {
            // 同日 IV 被修正（罕见，但 NPM 偶尔会盘后小幅校准）
            payload.current_price = latest.price == null ? '' : latest.price;
            payload.current_iv = latest.implied_valuation;
            payload.current_source = latest.source;
            payload.current_seen_at = now;
            payload.changed_at = now;
            changed = true;
            reason = 'same_date_iv_correction';
        }

        // 至少要更新 last_check_at；HSET 多字段一次性写
        await redis.hset(key, payload);

        if (changed) {
            const ivB = (latest.implied_valuation / 1e9).toFixed(2);
            const priceStr = latest.price != null ? latest.price.toFixed(2) : '?';
            const oldIvStr = curIv !== null ? `$${(curIv / 1e9).toFixed(2)}B` : 'n/a';
            // 区分两类对比：跨日推进时跟昨天比 (prev=)，同日修正时跟同日旧值比 (old=)
            const diffLabel = reason === 'same_date_iv_correction' ? 'old' : 'prev';
            logger.info(
                `💰 [NPM|${payload.name}] ${reason}: ${latest.date} `
                + `price=${priceStr} IV=$${ivB}B (${diffLabel}=${oldIvStr}) src=${latest.source}`
            );
            return {
                companyId: c.id,
                name: payload.name,
                date: latest.date,
                price: latest.price,
                implied_valuation: latest.implied_valuation,
                source: latest.source,
                prev_date: existing.current_date || null,
                prev_iv: curIv,
                reason,
                changed_at: now,
            };
        }
        return null;
    }

    async fetch() {
        this.tickCount++;

        const hot = this.isHotWindow();
        // 非高频窗口每 10 tick 才打一次（约 5 分钟）；高频窗口每 tick 都打
        if (!hot && (this.tickCount % 10) !== 0) {
            return null;
        }

        await this.loadCompaniesConfig();
        if (this.companies.length === 0) {
            return null;
        }

        const updates = [];
        // 公司数量少，串行 + 小延迟，避免 burst 对 NPM 端不友好
        for (let i = 0; i < this.companies.length; i++) {
            const c = this.companies[i];
            const u = await this.processCompany(c);
            if (u) updates.push(u);
            if (i < this.companies.length - 1) {
                await new Promise(r => setTimeout(r, 300));
            }
        }

        // 没变化就返回 null，触发 BaseCollector 心跳；有变化才广播
        if (updates.length === 0) return null;
        return {
            hot_window: hot,
            updates,
            ts: Date.now(),
        };
    }
}

export default NPMPricingCollector;
