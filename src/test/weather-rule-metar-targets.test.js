import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import dayjs from 'dayjs';
import { aggregateRuleMetar } from '../collectors/weather-rule-metar.js';
import { getRuleMetarTargets } from '../collectors/weather-rule-metar-targets.js';

const legacy = Object.freeze({ station: 'KBKF', unit: 'F', tz: 'America/Denver', sliceMinute: 58 });
test('append one real KDEN target; leave legacy identity and schedule untouched', () => {
    const input = Object.freeze([legacy]);
    const targets = getRuleMetarTargets(input);
    assert.equal(targets[0], legacy);
    assert.equal(targets.length, 2);
    assert.equal(targets[1].station, 'KDEN');
    assert.equal(targets[1].sliceMinute, undefined);
    assert.equal(getRuleMetarTargets(targets).length, 2);
    assert.equal(getRuleMetarTargets([]).length, 0);
    for (const bad of [{ ...legacy, unit: 'C' }, { ...legacy, tz: 'UTC' }, { ...legacy, station: 'KLGA' }]) {
        assert.equal(getRuleMetarTargets([bad]).length, 1);
    }
});

test('collector fetches KDEN but writes it only to rule namespace, without copying KBKF weather', async () => {
    const source = await readFile(new URL('../collectors/weather_observations.js', import.meta.url), 'utf8');
    const start = source.indexOf('    async publishRuleMetarSnapshots(');
    const end = source.indexOf('    // =====================', start);
    const Collector = vm.runInNewContext(`(class { ${source.slice(start, end)} })`, {
        dayjs, aggregateRuleMetar, getRuleMetarTargets, Date, logger: { warn() {}, info() {}, debug() {} },
    });
    const collector = new Collector();
    collector.targets = [legacy];
    const now = Date.now();
    const date = new Date(now);
    const header = date.toISOString().slice(8, 10) + date.toISOString().slice(11, 16).replace(':', '');
    const byStation = new Map(['KBKF', 'KDEN'].map(station => [station, [{ obsTime: now / 1000, temp: 1 }]]));
    byStation.ruleSnapshots = new Map(['KBKF', 'KDEN'].map((station, i) => [station, {
        reports: [{ icaoId: station, obsTime: Math.floor(now / 1000), receiptTime: date.toISOString(),
            rawOb: `${station} ${header}Z 00000KT 10SM CLR ${i ? '20' : '10'}/05 A3000` }],
        complete: true, windowStartMs: now - 48 * 3600_000, nowMs: now,
    }]));
    collector.fetchMetarBatch = async stations => {
        assert.deepEqual([...stations], ['KBKF', 'KDEN']); return byStation;
    };
    const legacyWrites = []; const ruleWrites = [];
    collector.aggregateMetarByDate = () => ({});
    collector.commitObservations = async target => { legacyWrites.push(target.station); return []; };
    collector.ruleMetarStore = { commit: async rows => ruleWrites.push(...Object.values(rows)) };
    await collector.runMetarTick();
    await new Promise(resolve => setImmediate(resolve));
    assert.deepEqual(legacyWrites, ['KBKF']);
    const kden = ruleWrites.find(row => row.station === 'KDEN' && row.obs_count > 0);
    const kbkf = ruleWrites.find(row => row.station === 'KBKF' && row.obs_count > 0);
    assert.equal(kden.high, 68); assert.equal(kbkf.high, 50);
    assert.equal(collector.targets[0], legacy);
    // Both hot/cold lookup and automatic slice detection use the appended target.
    const fetchBody = source.slice(source.indexOf('    async fetchMetarBatch('), source.indexOf('    _detectSliceFromObs('));
    assert.match(fetchBody, /new Map\(metarTargets.map/);
    assert.match(fetchBody, /for \(const target of metarTargets\)/);
});
