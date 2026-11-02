import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import dayjs from 'dayjs';
import { aggregateRuleMetar, parseRuleMetar, roundRuleTemperature } from '../collectors/weather-rule-metar.js';
import { WeatherRuleMetarStore } from '../collectors/weather-rule-metar-store.js';
import { getRuleMetarTargets } from '../collectors/weather-rule-metar-targets.js';

const DATE = '2026-11-01';
const TARGET = { station: 'KLGA', unit: 'C', tz: 'America/New_York' };
const NOW = Date.parse('2026-11-02T08:00:00Z');
function report(iso, temperature = '18/12', { prefix = 'METAR ', remarks = '', ...overrides } = {}) {
    const date = new Date(iso);
    const headerTime = date.toISOString().slice(8, 10) + date.toISOString().slice(11, 16).replace(':', '');
    return { icaoId: 'KLGA', obsTime: date.getTime() / 1000, receiptTime: new Date(date.getTime() + 60_000).toISOString(),
        rawOb: `${prefix}KLGA ${headerTime}Z 00000KT 10SM CLR ${temperature} A3000${remarks ? ' RMK ' + remarks : ''}`, ...overrides };
}
function aggregate(reports, options = {}, target = TARGET, dates = [DATE]) {
    return aggregateRuleMetar(reports, target, dates, { nowMs: NOW, ...options });
}

test('routine raw T group wins over body/decoded temp; excludes leading SPECI only', () => {
    const rows = [
        report('2026-11-01T05:51Z', '18/12', { remarks: 'AO2 T01830120', temp: 99 }),
        report('2026-11-01T06:10Z', '31/12', { prefix: 'SPECI ' }),
        report('2026-11-01T06:20Z', '17/12', { prefix: '' }),
        report('2026-11-01T07:17Z', '16/12'),
    ];
    const row = aggregate(rows, {}, { ...TARGET, unit: 'F' })[DATE];
    assert.equal(row.high, 65); assert.equal(row.low, 61);
    assert.equal(row.obs_count, 3); assert.equal(row.excluded_speci, 1);
    assert.equal(row.detail[0].temp_tenths_c, 183);
    assert.equal(row.detail[0].precision, 'T');
    assert.equal(roundRuleTemperature(180, 'F'), 64);
    const unusualDew = report('2026-11-01T08:00Z', '18/12', { remarks: 'T01839999' });
    assert.equal(aggregate([unusualDew], {}, { ...TARGET, unit: 'F' })[DATE].high, 65);
    const conflict = { ...unusualDew, rawOb: unusualDew.rawOb + ' T01900120' };
    assert.equal(aggregate([conflict])[DATE].status, 'incomplete');
    const badSign = { ...unusualDew, rawOb: unusualDew.rawOb.replace('T01839999', 'T21839999') };
    assert.equal(aggregate([badSign])[DATE].status, 'incomplete');
});

test('exact rounding includes negative halves, negative zero and malformed/non-hourly remarks', () => {
    for (const [tenths, unit, expected] of [[-85, 'C', -8], [-225, 'F', -8], [0, 'C', 0], [-5, 'C', 0], [175, 'C', 18]]) {
        assert.equal(roundRuleTemperature(tenths, unit), expected);
    }
    const base = report('2026-11-01T08:00Z', 'M11/M17', { remarks: 'AO2 10183 20122 T018301200' });
    assert.equal(parseRuleMetar(base, 'KLGA').tenthsC, -110);
    assert.equal(parseRuleMetar({ ...base, rawOb: base.rawOb + ' T10171022' }, 'KLGA').tenthsC, -17);
    assert.equal(parseRuleMetar(report('2026-11-01T08:00Z', '00/M01'), 'KLGA').tenthsC, 0);
    assert.equal(parseRuleMetar(report('2026-11-01T08:00Z', '//'), 'KLGA').tenthsC, null);
    assert.equal(parseRuleMetar(report('2026-11-01T08:00Z', '18///'), 'KLGA').tenthsC, 180);
});

test('local observation day, not UTC/receipt/nominal hour; DST repeated hour is retained', () => {
    const row = aggregate([
        report('2026-11-01T03:59Z', '40/12'), // prior local date
        report('2026-11-01T04:00Z', '10/05'),
        report('2026-11-01T05:30Z', '11/05'), // first 01:30
        report('2026-11-01T06:30Z', '12/05'), // second 01:30
        report('2026-11-02T04:59Z', '09/05', { receiptTime: '2026-11-02T05:01:00Z' }),
        report('2026-11-02T05:00Z', '40/12'), // following local day
    ])[DATE];
    assert.equal(row.high, 12); assert.equal(row.low, 9); assert.equal(row.obs_count, 4);
});

test('latest published revision replaces the old extreme; ambiguous revisions are unusable', () => {
    const original = report('2026-11-01T10:00Z', '35/12');
    const corrected = report('2026-11-01T10:00Z', '30/12', { prefix: 'METAR COR ', receiptTime: '2026-11-01T10:05:00Z' });
    const unchangedOlder = { ...original, receiptTime: '2026-11-01T09:59:00Z' };
    for (const rows of [[original, corrected, unchangedOlder], [unchangedOlder, corrected, original]]) {
        assert.equal(aggregate(rows)[DATE].high, 30);
        assert.equal(aggregate(rows)[DATE].obs_count, 1);
    }
    const ambiguous = { ...corrected, receiptTime: original.receiptTime };
    assert.equal(aggregate([original, ambiguous])[DATE].status, 'incomplete');
    assert.equal(aggregate([original, { ...corrected, receiptTime: null }])[DATE].revision_conflicts, 1);
    const noTempCorrection = { ...corrected, rawOb: corrected.rawOb.replace('30/12', '//') };
    assert.equal(aggregate([original, noTempCorrection])[DATE].status, 'no_routine_temperature');
    const cutoff = Date.parse('2026-11-01T10:02:00Z');
    assert.equal(aggregate([original, corrected], { cutoffMs: cutoff })[DATE].high, 35);
});

test('empty/partial/malformed evidence never becomes 0, a final settlement or WU fallback', () => {
    const row = aggregate([])[DATE];
    assert.equal(row.status, 'no_routine_temperature'); assert.equal(row.high, ''); assert.equal(row.low, '');
    assert.equal(row.provisional, 'true');
    const r = report('2026-11-01T10:00Z');
    assert.equal(aggregate([r], { complete: false })[DATE].status, 'incomplete');
    assert.equal(aggregate([r], { windowStartMs: Date.parse('2026-11-01T04:01Z') })[DATE].status, 'incomplete');
    assert.equal(aggregate([{ ...r, rawOb: r.rawOb.replace('011000Z', '011100Z') }])[DATE].status, 'incomplete');
    assert.equal(aggregate([{ ...r, icaoId: 'KJFK' }])[DATE].status, 'incomplete');
    assert.equal(aggregate([{ ...r, rawOb: r.rawOb + '\nOTHER' }])[DATE].status, 'incomplete');
    assert.equal(aggregate([{ ...r, obsTime: undefined }])[DATE].status, 'incomplete');
    assert.throws(() => aggregate(Array(401).fill(r)), RangeError);
    assert.throws(() => aggregate([r], {}, TARGET, ['2026-02-31']), RangeError);
});

function fakeRedis() {
    const writes = [];
    return { writes, status: 'ready', failure: false, pipeline() {
        const commands = [];
        const chain = { hset: (...args) => add('hset', args), expire: (...args) => add('expire', args), set: (...args) => add('set', args),
            exec: async () => { writes.push(commands); return commands.map(() => [this.failure ? new Error('offline') : null, 1]); } };
        const add = (name, args) => { commands.push([name, ...args]); return chain; };
        return chain;
    } };
}
test('bounded writes: unchanged polls only heartbeat; raw writes on change, failures retry', async () => {
    const redis = fakeRedis(); const messages = [];
    const store = new WeatherRuleMetarStore(redis, { info: m => messages.push(m), warn: m => messages.push(m) });
    const input = aggregate([report('2026-11-01T10:00Z')]);
    for (let n = 0; n < 1000; n++) {
        input[DATE].updated_at = NOW + n * 1000;
        await store.commit(input);
    }
    assert.equal(redis.writes.length, 34);
    assert.equal(redis.writes.flat().filter(c => c[0] === 'set').length, 1);
    assert.equal(messages.length, 1);
    assert.ok(redis.writes.flat().every(c => c[1].startsWith('poly:rule_metar:')));
    input[DATE].high = 19; redis.failure = true;
    await assert.rejects(store.commit(input));
    redis.failure = false; await store.commit(input);
    assert.equal(redis.writes.at(-1).some(c => c[0] === 'set'), true);
    for (let n = 0; n < 300; n++) await store.commit({ x: { ...input[DATE], station: `S${n}` } });
    assert.equal(store.written.size, 256);
    redis.status = 'reconnecting';
    const before = redis.writes.length;
    await assert.rejects(store.commit(input));
    assert.equal(redis.writes.length, before);
});

test('collector isolates slow/failed new writes; original metar updates keep running unchanged', async () => {
    const source = await readFile(new URL('../collectors/weather_observations.js', import.meta.url), 'utf8');
    const start = source.indexOf('    async publishRuleMetarSnapshots(');
    const end = source.indexOf('    // =====================', start);
    const warnings = [];
    const Collector = vm.runInNewContext(`(class { ${source.slice(start, end)} })`, {
        dayjs, aggregateRuleMetar, getRuleMetarTargets, Date, logger: { warn: m => warnings.push(m), info() {}, debug() {} },
    });
    const collector = new Collector();
    collector.targets = [TARGET]; collector.lastRuleMetarErrorAt = 0;
    let release; let commits = 0; let legacy = 0;
    collector.ruleMetarStore = { commit: async () => { commits++; await new Promise(resolve => { release = resolve; }); } };
    const byStation = new Map([['KLGA', [{ obsTime: 1, temp: 1 }]]]);
    byStation.ruleSnapshots = new Map([['KLGA', { reports: [], complete: true }]]);
    collector.fetchMetarBatch = async () => byStation;
    collector.aggregateMetarByDate = () => ({ legacy: true });
    collector.commitObservations = async (_target, rows, sourceName) => {
        assert.equal(sourceName, 'metar'); assert.equal(rows.legacy, true); legacy++; return [];
    };
    await collector.runMetarTick();
    await collector.runMetarTick();
    assert.equal(legacy, 2); assert.equal(commits, 1); assert.equal(collector.ruleMetarRunning, true);
    release(); await new Promise(resolve => setImmediate(resolve));
    assert.equal(collector.ruleMetarRunning, false);
    collector.ruleMetarStore.commit = async () => { throw new Error('offline'); };
    await collector.runMetarTick(); await new Promise(resolve => setImmediate(resolve));
    assert.equal(legacy, 3); assert.equal(warnings.length, 1); assert.equal(collector.ruleMetarRunning, false);
});

const fixture = JSON.parse(await readFile(new URL('./fixtures/rule-metar-resolved.json', import.meta.url)));
test('16 resolved markets / six stations: raw routine extrema match the actual resolved winning bin', () => {
    for (const event of fixture.events) {
        assert.equal(event.closed, true);
        const snapshot = fixture.snapshots.find(s => s.url === event.resolutionSource);
        const query = new URL(snapshot.url);
        const endMs = Date.parse(query.searchParams.get('date'));
        const row = aggregateRuleMetar(snapshot.reports, event.target, [event.date], {
            nowMs: Date.parse(fixture.retrievedAt), windowStartMs: endMs - Number(query.searchParams.get('hours')) * 3600_000,
        })[event.date];
        assert.equal(row.status, 'ok', event.slug);
        const value = row[event.tempField];
        const winning = event.markets.filter(m => m.umaResolutionStatus === 'resolved' && m.closed
            && Number(m.outcomePrices[m.outcomes.indexOf('Yes')]) === 1);
        assert.equal(winning.length, 1, event.slug);
        const label = winning[0].groupItemTitle;
        const numbers = label.match(/-?\d+/g).map(Number);
        const inBin = label.includes('or below') ? value <= numbers[0]
            : label.includes('or higher') ? value >= numbers[0]
                : numbers.length === 1 ? value === numbers[0] : value >= numbers[0] && value <= Math.abs(numbers[1]);
        assert.equal(inBin, true, `${event.slug}: raw=${value} winning=${label}`);
        console.log(JSON.stringify({ slug: event.slug, value, winning: label, routine: row.obs_count, excludedSpeci: row.excluded_speci }));
    }
});
