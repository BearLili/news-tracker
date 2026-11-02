import { createHash } from 'node:crypto';

const TTL_SECONDS = 14 * 24 * 60 * 60;
const HEARTBEAT_MS = 30_000;
const MAX_ENTRIES = 256;

/** Bounded write deduplication for the new, isolated rule-observation namespace. */
export class WeatherRuleMetarStore {
    /**
     * @param {object} redis Existing ioredis client; no new connection.
     * @param {object} logger Existing collector logger.
     * @throws {never} No I/O at construction.
     */
    constructor(redis, logger) {
        this.redis = redis;
        this.logger = logger;
        this.written = new Map();
    }

    /**
     * Write changed raw evidence once; unchanged polls only refresh a small hash
     * at most every 30 seconds. No legacy hash, feed or trade state is touched.
     * @param {object} byDate aggregateRuleMetar output.
     * @returns {Promise<number>} Number of date hashes written.
     * @throws {Error} Redis failure; caller isolates it from legacy collectors.
     */
    async commit(byDate) {
        // Reuse the connection, but never enqueue new work while it reconnects.
        if (this.redis.status !== 'ready') throw new Error('rule observation Redis not ready');
        let writes = 0;
        for (const row of Object.values(byDate)) {
            const { detail, ...payload } = row;
            const key = `poly:rule_metar:${row.station}:${row.date}`;
            const raw = JSON.stringify(detail);
            const { updated_at, window_start_ms, ...stable } = payload;
            const fingerprint = createHash('sha256').update(JSON.stringify(stable)).update(raw).digest('hex');
            const previous = this.written.get(key);
            const changed = previous?.fingerprint !== fingerprint;
            if (!changed && updated_at - previous.at < HEARTBEAT_MS) continue;
            const pipeline = this.redis.pipeline();
            pipeline.hset(key, payload).expire(key, TTL_SECONDS);
            const detailKey = `poly:rule_metar:obs:${row.station}:${row.date}`;
            if (changed) pipeline.set(detailKey, raw, 'EX', TTL_SECONDS);
            else pipeline.expire(detailKey, TTL_SECONDS);
            const results = await pipeline.exec();
            if (!Array.isArray(results) || results.some(([error]) => error)) throw new Error('rule observation write failed');
            this.written.delete(key);
            this.written.set(key, { fingerprint, at: updated_at, status: row.status });
            while (this.written.size > MAX_ENTRIES) this.written.delete(this.written.keys().next().value);
            if (previous?.status !== row.status) {
                const message = `[RuleMETAR] station=${row.station} date=${row.date} status=${row.status} observations=${row.obs_count}`;
                if (row.status === 'ok') this.logger.info(message);
                else this.logger.warn(message);
            }
            writes++;
        }
        return writes;
    }
}
