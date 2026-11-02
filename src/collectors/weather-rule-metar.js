import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import timezone from 'dayjs/plugin/timezone.js';

dayjs.extend(utc);
dayjs.extend(timezone);

export const RULE_METAR_VERSION = 'awc-routine-t-v1';
export const RULE_METAR_MAX_REPORTS = 400;

function receiptMs(value) {
    if (typeof value !== 'string') return null;
    const ms = Date.parse(value);
    return Number.isFinite(ms) ? ms : null;
}

/**
 * Parse the original AWC report, never an interpolated/decoded temperature.
 * Non-hourly and unlabelled routine reports qualify; a leading SPECI does not.
 * @param {object} report AWC JSON record with rawOb, icaoId and obsTime.
 * @param {string} station Expected ICAO identifier.
 * @returns {object} Classification, integer tenths Celsius and report identity.
 * @throws {never} Malformed reports are returned as invalid evidence.
 */
export function parseRuleMetar(report, station) {
    const raw = typeof report?.rawOb === 'string' ? report.rawOb.trim() : '';
    const ts = report?.obsTime;
    const base = { ts, raw, receivedAt: receiptMs(report?.receiptTime) };
    if (report?.icaoId !== station || !Number.isSafeInteger(ts) || ts <= 0
        || raw.length > 4096 || /[\r\n]/.test(raw)) return { ...base, kind: 'invalid' };
    const header = raw.match(/^(?:(METAR|SPECI)\s+)?(?:COR\s+)?([A-Z0-9]{4})\s+(?:COR\s+)?(\d{2})(\d{2})(\d{2})Z\b/);
    if (!header || header[2] !== station) return { ...base, kind: 'invalid' };
    const time = new Date(ts * 1000);
    if (time.getUTCDate() !== Number(header[3]) || time.getUTCHours() !== Number(header[4])
        || time.getUTCMinutes() !== Number(header[5])) return { ...base, kind: 'invalid' };
    if (header[1] === 'SPECI') return { ...base, kind: 'speci' };
    const [body, remarks = ''] = raw.split(/\sRMK\s/, 2);
    const tGroups = [...remarks.matchAll(/(?:^|\s)T(\d{8})(?=\s|=|$)/g)].map(match => match[1]);
    // The last four digits are explicitly unused by the market rule. Do not
    // reject a valid temperature because of the dew-point encoding.
    if (tGroups.some(group => !['0', '1'].includes(group[0]))
        || new Set(tGroups.map(group => group.slice(0, 4))).size > 1) return { ...base, kind: 'invalid' };
    const tGroup = tGroups[0];
    const bodyGroup = body.match(/(?:^|\s)(M?\d{2})\/(?:M?\d{2}|\/{1,2})?(?=\s|=|$)/);
    let tenths = null;
    if (tGroup) tenths = (tGroup[0] === '1' ? -1 : 1) * Number(tGroup.slice(1, 4));
    else if (bodyGroup) tenths = Number(bodyGroup[1].replace('M', '-')) * 10;
    if (tenths != null && (tenths < -900 || tenths > 650)) return { ...base, kind: 'invalid' };
    return { ...base, kind: 'routine', tenthsC: tenths === 0 ? 0 : tenths,
        precision: tGroup ? 'T' : (bodyGroup ? 'body' : 'missing'), corrected: /(?:^|\s)COR(?:\s|$)/.test(body) };
}

/**
 * Convert exact tenths Celsius once, then round half toward positive infinity.
 * Integer arithmetic also handles negative Fahrenheit half-degree boundaries.
 * @param {number} tenths Integer tenths Celsius.
 * @param {'C'|'F'} unit Market unit.
 * @returns {number} Whole-degree market temperature, with zero normalised.
 * @throws {RangeError} If the input or unit is invalid.
 */
export function roundRuleTemperature(tenths, unit) {
    if (!Number.isSafeInteger(tenths) || !['C', 'F'].includes(unit)) throw new RangeError('invalid rule temperature');
    const numerator = unit === 'F' ? tenths * 9 + 1600 : tenths;
    const denominator = unit === 'F' ? 50 : 10;
    const rounded = Math.floor((2 * numerator + denominator) / (2 * denominator));
    return rounded === 0 ? 0 : rounded;
}

function chooseRevision(reports) {
    const byRaw = new Map();
    for (const report of reports) {
        const previous = byRaw.get(report.raw);
        if (!previous || (report.receivedAt ?? -Infinity) > (previous.receivedAt ?? -Infinity)) byRaw.set(report.raw, report);
    }
    const distinct = [...byRaw.values()];
    if (distinct.length === 1) return distinct[0];
    if (distinct.every(report => report.receivedAt != null)) {
        const latest = Math.max(...distinct.map(report => report.receivedAt));
        const candidates = distinct.filter(report => report.receivedAt === latest);
        if (candidates.length === 1) return candidates[0];
    }
    // Never take max/min across conflicting revisions of the same observation.
    return null;
}

/**
 * Build provisional, rule-qualified daily extremes from an existing AWC response.
 * This is NOT a final market settlement oracle: absent observations stay unknown;
 * WU fallback, deadline freezing and exceptional Polymarket clarifications are not
 * fabricated from a missing response. An optional as-of cutoff is for replay.
 * @param {object[]} reports Original AWC records for one station.
 * @param {{station:string,tz:string,unit:'C'|'F'}} target Station identity.
 * @param {string[]} dates Local dates to materialise, including empty dates.
 * @param {{complete?:boolean,windowStartMs?:number,nowMs?:number,cutoffMs?:number}} options Fetch coverage.
 * @returns {object} Date-keyed compact hashes and bounded raw audit details.
 * @throws {RangeError} Invalid station/unit/timezone or excessive input.
 */
export function aggregateRuleMetar(reports, target, dates, {
    complete = true, windowStartMs = -Infinity, nowMs = Date.now(), cutoffMs = Infinity,
} = {}) {
    if (!/^[A-Z0-9]{4}$/.test(target?.station || '') || !['C', 'F'].includes(target?.unit)
        || !target.tz || !Array.isArray(reports) || reports.length > RULE_METAR_MAX_REPORTS
        || !Array.isArray(dates) || dates.length > 3 || !Number.isFinite(nowMs)) throw new RangeError('invalid rule observation input');
    const output = {};
    for (const date of dates) {
        const start = dayjs.tz(`${date} 00:00:00`, target.tz).valueOf();
        if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || !Number.isFinite(start)
            || dayjs(start).tz(target.tz).format('YYYY-MM-DD') !== date) throw new RangeError('invalid rule date');
        output[date] = { station: target.station, date, unit: target.unit, timezone: target.tz,
            source: 'rule_metar', rule_version: RULE_METAR_VERSION, provisional: 'true',
            status: complete && windowStartMs <= start ? 'ok' : 'incomplete',
            high: '', low: '', obs_count: 0, excluded_speci: 0, ignored_no_temperature: 0,
            invalid_count: 0, revision_conflicts: 0, latest_obs_ts: '', high_obs_ts: '', low_obs_ts: '',
            updated_at: nowMs, window_start_ms: Number.isFinite(windowStartMs) ? windowStartMs : '',
            detail: [], groups: new Map() };
    }
    for (const report of reports) {
        if (!Number.isSafeInteger(report?.obsTime)) {
            for (const row of Object.values(output)) row.status = 'incomplete';
            continue;
        }
        const date = dayjs.unix(report.obsTime).tz(target.tz).format('YYYY-MM-DD');
        const row = output[date];
        if (!row) continue;
        const parsed = parseRuleMetar(report, target.station);
        if (parsed.ts * 1000 > nowMs + 5 * 60_000 || parsed.kind === 'invalid') {
            row.invalid_count++; row.status = 'incomplete'; continue;
        }
        if (parsed.kind === 'speci') { row.excluded_speci++; continue; }
        if (Number.isFinite(cutoffMs) && (parsed.receivedAt == null || parsed.receivedAt > cutoffMs)) {
            if (parsed.receivedAt == null) row.status = 'incomplete';
            continue;
        }
        if (!row.groups.has(parsed.ts)) row.groups.set(parsed.ts, []);
        row.groups.get(parsed.ts).push(parsed);
    }
    for (const row of Object.values(output)) {
        for (const revisions of row.groups.values()) {
            const report = chooseRevision(revisions);
            if (!report) { row.revision_conflicts++; row.status = 'incomplete'; continue; }
            if (report.tenthsC == null) { row.ignored_no_temperature++; continue; }
            row.detail.push({ ts: report.ts, temp_tenths_c: report.tenthsC,
                temp: roundRuleTemperature(report.tenthsC, target.unit), precision: report.precision,
                receipt_ms: report.receivedAt, raw: report.raw });
        }
        row.detail.sort((a, b) => a.ts - b.ts);
        row.obs_count = row.detail.length;
        if (row.obs_count) {
            const high = row.detail.reduce((a, b) => b.temp_tenths_c > a.temp_tenths_c ? b : a);
            const low = row.detail.reduce((a, b) => b.temp_tenths_c < a.temp_tenths_c ? b : a);
            row.high = high.temp; row.low = low.temp;
            row.high_obs_ts = high.ts; row.low_obs_ts = low.ts;
            row.latest_obs_ts = row.detail.at(-1).ts;
        } else if (row.status === 'ok') row.status = 'no_routine_temperature';
        delete row.groups;
    }
    return output;
}
