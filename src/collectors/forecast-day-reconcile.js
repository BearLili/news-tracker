import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import timezone from 'dayjs/plugin/timezone.js';
import { RULE_METAR_VERSION } from './weather-rule-metar.js';

dayjs.extend(utc);
dayjs.extend(timezone);

const MAX_RULE_METAR_AGE_MS = 30 * 60 * 1000;
const MAX_RULE_METAR_OBS_AGE_MS = 2 * 60 * 60 * 1000;
const MAX_RULE_METAR_FUTURE_SKEW_MS = 5 * 60 * 1000;

function normalizeTimestampMs(raw) {
    if (raw == null || raw === '') return 0;
    const value = Number(raw);
    if (!Number.isFinite(value) || value <= 0) return 0;
    return value < 1e12 ? value * 1000 : value;
}

/**
 * Resolve the explicit Open-Meteo timezone query value from city configuration.
 * @param {unknown} tz IANA timezone identifier from `poly:config:cities`.
 * @returns {string} URL-encoded timezone parameter, falling back to `'auto'`.
 * @throws {never} Invalid inputs fall back safely.
 */
export function encodeOpenMeteoTimezone(tz) {
    const raw = typeof tz === 'string' ? tz.trim() : '';
    return encodeURIComponent(raw || 'auto');
}

/**
 * Parse a `poly:rule_metar:<station>:<date>` hash into validated T0 extremes.
 * @param {Record<string,string>|null|undefined} hash Redis hash payload.
 * @param {{station:string,date:string,unit:'C'|'F',nowMs?:number}} context Expected target metadata.
 * @returns {{valid:boolean,high:number|null,low:number|null}}
 * @throws {never} Malformed or stale hashes return `valid: false`.
 */
export function parseTodayRuleMetarExtremes(hash, { station, date, unit, nowMs = Date.now() }) {
    if (!hash || typeof hash !== 'object') return { valid: false, high: null, low: null };
    if (hash.status !== 'ok' || hash.rule_version !== RULE_METAR_VERSION) {
        return { valid: false, high: null, low: null };
    }
    if (String(hash.station || '').toUpperCase() !== String(station || '').toUpperCase()
        || String(hash.date || '') !== String(date || '')
        || String(hash.unit || '').toUpperCase() !== String(unit || '').toUpperCase()) {
        return { valid: false, high: null, low: null };
    }
    const updatedAtMs = normalizeTimestampMs(hash.updated_at);
    if (!updatedAtMs || updatedAtMs > nowMs + MAX_RULE_METAR_FUTURE_SKEW_MS
        || nowMs - updatedAtMs > MAX_RULE_METAR_AGE_MS) {
        return { valid: false, high: null, low: null };
    }
    const latestObsTsMs = normalizeTimestampMs(hash.latest_obs_ts);
    if (!latestObsTsMs || latestObsTsMs > nowMs + MAX_RULE_METAR_FUTURE_SKEW_MS
        || nowMs - latestObsTsMs > MAX_RULE_METAR_OBS_AGE_MS) {
        return { valid: false, high: null, low: null };
    }
    const high = hash.high !== '' && hash.high != null && Number.isFinite(Number(hash.high)) ? Number(hash.high) : null;
    const low = hash.low !== '' && hash.low != null && Number.isFinite(Number(hash.low)) ? Number(hash.low) : null;
    if (high == null && low == null) return { valid: false, high: null, low: null };
    return { valid: true, high, low };
}

/**
 * Aggregate MET Norway future-only hourly points into calendar-day extremes,
 * reconciling T0 against already-observed `poly:rule_metar` extremes so evening
 * fetches never mistake remaining night hours for the full-day maximum/minimum.
 * @param {Array<object>} series `properties.timeseries` array from MET Norway compact API.
 * @param {{station:string,tz:string,unit:'C'|'F'}} target City configuration.
 * @param {{nowMs?:number,ruleMetarHash?:Record<string,string>|null}} [options]
 * @returns {Array<{date:string,high:number,low:number|null}>}
 * @throws {never} Malformed points are skipped safely.
 */
export function aggregateMetNoSeries(series, target, { nowMs = Date.now(), ruleMetarHash = null } = {}) {
    if (!Array.isArray(series) || series.length === 0 || !target?.tz) return [];
    const convert = tempC => (target.unit === 'F'
        ? Math.round((tempC * 9) / 5 + 32)
        : Math.round(tempC));

    const maxByDate = new Map();
    const minByDate = new Map();
    for (const point of series) {
        const iso = point?.time;
        const tempC = point?.data?.instant?.details?.air_temperature;
        if (!iso || typeof tempC !== 'number' || !Number.isFinite(tempC)) continue;
        const dateStr = dayjs(iso).tz(target.tz).format('YYYY-MM-DD');
        const prevMax = maxByDate.get(dateStr);
        if (prevMax === undefined || tempC > prevMax) maxByDate.set(dateStr, tempC);
        const prevMin = minByDate.get(dateStr);
        if (prevMin === undefined || tempC < prevMin) minByDate.set(dateStr, tempC);
    }

    const nowLocal = dayjs(nowMs).tz(target.tz);
    const todayStr = nowLocal.format('YYYY-MM-DD');
    const localHour = nowLocal.hour() + nowLocal.minute() / 60;
    const todayObs = parseTodayRuleMetarExtremes(ruleMetarHash, {
        station: target.station,
        date: todayStr,
        unit: target.unit,
        nowMs,
    });

    const todayStart = nowLocal.startOf('day');
    const result = [];
    for (let i = 0; i < 5; i++) {
        const d = todayStart.add(i, 'day').format('YYYY-MM-DD');
        if (!maxByDate.has(d)) continue;
        let high = convert(maxByDate.get(d));
        const lowRaw = minByDate.get(d);
        let low = lowRaw !== undefined ? convert(lowRaw) : null;

        if (d === todayStr) {
            if (todayObs.valid) {
                if (todayObs.high != null) high = Math.max(high, todayObs.high);
                if (todayObs.low != null) low = low != null ? Math.min(low, todayObs.low) : todayObs.low;
            } else if (localHour >= 6) {
                // After local 06:00, future-only timeseries omits earlier morning/afternoon
                // hours. Skip T0 rather than overwrite Redis with a partial-day fragment.
                continue;
            }
        }
        result.push({ date: d, high, low });
    }
    return result;
}
