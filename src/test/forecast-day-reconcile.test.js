import test from 'node:test';
import assert from 'node:assert/strict';
import {
    aggregateMetNoSeries,
    encodeOpenMeteoTimezone,
    parseTodayRuleMetarExtremes,
} from '../collectors/forecast-day-reconcile.js';
import { RULE_METAR_VERSION } from '../collectors/weather-rule-metar.js';

const NOW_PARIS_EVENING = Date.parse('2026-11-19T19:00:00Z'); // 20:00 Europe/Paris
const PARIS = { station: 'LFPB', tz: 'Europe/Paris', unit: 'C' };

function point(iso, tempC) {
    return { time: iso, data: { instant: { details: { air_temperature: tempC } } } };
}

test('encodeOpenMeteoTimezone encodes explicit city timezone and falls back safely', () => {
    assert.equal(encodeOpenMeteoTimezone('America/New_York'), 'America%2FNew_York');
    assert.equal(encodeOpenMeteoTimezone('Asia/Shanghai'), 'Asia%2FShanghai');
    assert.equal(encodeOpenMeteoTimezone(''), 'auto');
    assert.equal(encodeOpenMeteoTimezone(null), 'auto');
});

test('evening MET.NO series merges T0 with rule_metar so daytime high and morning low are preserved', () => {
    const series = [
        point('2026-11-19T19:00:00Z', 10.2),
        point('2026-11-19T21:00:00Z', 8.4),
        point('2026-11-20T06:00:00Z', 4.1),
        point('2026-11-20T14:00:00Z', 13.7),
    ];
    const ruleMetarHash = {
        station: 'LFPB',
        date: '2026-11-19',
        unit: 'C',
        status: 'ok',
        rule_version: RULE_METAR_VERSION,
        high: '16',
        low: '5',
        updated_at: String(NOW_PARIS_EVENING - 60_000),
        latest_obs_ts: String(Math.floor((NOW_PARIS_EVENING - 30 * 60_000) / 1000)),
    };
    const rows = aggregateMetNoSeries(series, PARIS, {
        nowMs: NOW_PARIS_EVENING,
        ruleMetarHash,
    });
    assert.deepEqual(rows, [
        { date: '2026-11-19', high: 16, low: 5 },
        { date: '2026-11-20', high: 14, low: 4 },
    ]);
});

test('evening MET.NO without valid rule_metar omits partial T0 fragment while keeping T1+', () => {
    const series = [
        point('2026-11-19T19:00:00Z', 10.2),
        point('2026-11-19T21:00:00Z', 8.4),
        point('2026-11-20T06:00:00Z', 4.1),
        point('2026-11-20T14:00:00Z', 13.7),
    ];
    const rows = aggregateMetNoSeries(series, PARIS, {
        nowMs: NOW_PARIS_EVENING,
        ruleMetarHash: null,
    });
    assert.deepEqual(rows, [
        { date: '2026-11-20', high: 14, low: 4 },
    ]);
});

test('early-morning MET.NO before local 06:00 still emits T0 when rule_metar is absent', () => {
    const nowEarly = Date.parse('2026-11-19T02:00:00Z'); // 03:00 Europe/Paris
    const series = [
        point('2026-11-19T02:00:00Z', 6.1),
        point('2026-11-19T13:00:00Z', 14.8),
    ];
    const rows = aggregateMetNoSeries(series, PARIS, {
        nowMs: nowEarly,
        ruleMetarHash: null,
    });
    assert.deepEqual(rows, [
        { date: '2026-11-19', high: 15, low: 6 },
    ]);
});

test('parseTodayRuleMetarExtremes rejects stale, incomplete or mismatched station hashes', () => {
    const base = {
        station: 'LFPB',
        date: '2026-11-19',
        unit: 'C',
        status: 'ok',
        rule_version: RULE_METAR_VERSION,
        high: '16',
        low: '5',
        updated_at: String(NOW_PARIS_EVENING - 60_000),
        latest_obs_ts: String(Math.floor((NOW_PARIS_EVENING - 30 * 60_000) / 1000)),
    };
    assert.equal(parseTodayRuleMetarExtremes(base, { ...PARIS, date: '2026-11-19', nowMs: NOW_PARIS_EVENING }).valid, true);
    assert.equal(parseTodayRuleMetarExtremes({ ...base, status: 'incomplete' }, { ...PARIS, date: '2026-11-19', nowMs: NOW_PARIS_EVENING }).valid, false);
    assert.equal(parseTodayRuleMetarExtremes({ ...base, station: 'EGLC' }, { ...PARIS, date: '2026-11-19', nowMs: NOW_PARIS_EVENING }).valid, false);
    assert.equal(parseTodayRuleMetarExtremes({ ...base, updated_at: String(NOW_PARIS_EVENING - 40 * 60_000) }, { ...PARIS, date: '2026-11-19', nowMs: NOW_PARIS_EVENING }).valid, false);
    assert.equal(parseTodayRuleMetarExtremes({ ...base, latest_obs_ts: '' }, { ...PARIS, date: '2026-11-19', nowMs: NOW_PARIS_EVENING }).valid, false);
    assert.equal(parseTodayRuleMetarExtremes({ ...base, latest_obs_ts: String(Math.floor((NOW_PARIS_EVENING - 3 * 60 * 60_000) / 1000)) }, { ...PARIS, date: '2026-11-19', nowMs: NOW_PARIS_EVENING }).valid, false);
    assert.equal(parseTodayRuleMetarExtremes({ ...base, latest_obs_ts: String(Math.floor((NOW_PARIS_EVENING + 10 * 60_000) / 1000)) }, { ...PARIS, date: '2026-11-19', nowMs: NOW_PARIS_EVENING }).valid, false);
});
