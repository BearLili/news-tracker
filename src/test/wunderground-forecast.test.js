import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import { fetchWundergroundForecast, WundergroundForecastError } from '../collectors/wunderground-forecast.js';

const NOW = Date.parse('2026-11-02T04:30:00Z');
const TARGET = { station: 'WMKK', wunderUrl: 'https://www.wunderground.com/weather/my/sepang-district/WMKK', tz: 'Asia/Kuala_Lumpur', unit: 'C' };
const PLACE = 'a'.repeat(64);
const TEST_KEY = 'not-a-real-api-key';
const page = (station = 'WMKK', api = 'https://api.weather.com') => `
<script> (function () { const data = ${JSON.stringify({ API_KEY: TEST_KEY, API_URL: api, UNITS_PREF: 'm' })};
const readOnly = ["API_KEY"]; }()); </script>
<current-conditions-view data-placeid="${PLACE}" data-icaocode="${station}"></current-conditions-view>`;
const body = () => ({
    validTimeLocal: ['2026-11-02T07:00:00+0800', '2026-11-03T07:00:00+0800'],
    calendarDayTemperatureMax: [91, 92], calendarDayTemperatureMin: [77, 76],
    // Deliberately different from calendar days: these MUST NOT be used.
    temperatureMax: [null, 94], temperatureMin: [76, 75],
    daypart: [{ temperature: [95, 75] }],
    expirationTimeUtc: [NOW / 1000 + 3600, NOW / 1000 + 3600]
});
async function run({ html = page(), data = body(), target = TARGET, error, status = 200 } = {}) {
    const calls = [];
    const proxy = { protocol: 'http', host: 'test.invalid', port: 8080, auth: { username: 'fake', password: 'fake' } };
    const rows = await fetchWundergroundForecast(target, { now: NOW, proxy, get: async (url, options) => {
        calls.push({ url, options });
        if (error) throw error;
        return calls.length === 1 ? { status, data: html } : { status: 200, data };
    } });
    return { rows, calls, proxy };
}

test('new page uses matching station and same calendar-day F values, converted once to C', async () => {
    const { rows, calls, proxy } = await run();
    assert.deepEqual(rows, [{ date: '2026-11-02', high: 33, low: 25 }, { date: '2026-11-03', high: 33, low: 24 }]);
    assert.equal(calls.length, 2);
    const url = new URL(calls[1].url);
    assert.equal(url.origin, 'https://api.weather.com');
    assert.equal(url.pathname, '/v3/wx/forecast/daily/10day');
    assert.equal(url.searchParams.get('units'), 'e');
    assert.equal(url.searchParams.get('placeid'), PLACE);
    assert.equal(url.searchParams.get('apiKey'), TEST_KEY);
    for (const call of calls) {
        assert.equal(call.options.proxy, proxy);
        assert.equal(call.options.timeout, 15000);
        assert.equal(call.options.maxRedirects, 0);
    }
});

test('F target stays F; zero and negative temperatures are valid', async () => {
    const data = body(); data.calendarDayTemperatureMax = [32, 0]; data.calendarDayTemperatureMin = [0, -8];
    assert.deepEqual((await run({ data, target: { ...TARGET, unit: 'F' } })).rows.map(r => [r.high, r.low]), [[32, 0], [0, -8]]);
});

test('legacy embedded JSON needs no extra HTTP request and preserves date and calendar-day product', async () => {
    const { rows, calls } = await run({ html: `<script id="app-root-state">${JSON.stringify({ forecast: { b: body() } })}</script>` });
    assert.equal(calls.length, 1);
    assert.equal(rows[0].date, '2026-11-02');
    assert.equal(rows[0].low, 25);
});

test('before US 07:00 the prior day may be included, with current day still required', async () => {
    const data = body();
    data.validTimeLocal = ['2026-11-01T07:00:00-0500', '2026-11-02T07:00:00-0500'];
    const { rows } = await run({ data, target: { ...TARGET, tz: 'America/New_York' } });
    assert.deepEqual(rows.map(r => r.date), ['2026-11-01', '2026-11-02']);
});

test('reject station mismatch and untrusted API host before any forecast request', async () => {
    await assert.rejects(run({ html: page('EGLC') }), { code: 'page_station_mismatch' });
    await assert.rejects(run({ html: page('WMKK', 'https://evil.invalid') }), { code: 'page_config_invalid' });
    await assert.rejects(run({ html: page().replace(PLACE, 'bad-place') }), { code: 'page_place_invalid' });
    await assert.rejects(run({ target: { ...TARGET, wunderUrl: 'https://evil.invalid/weather' } }), { code: 'target_invalid' });
});

test('reject absent/malformed page data, HTTP failure and incorrect units', async () => {
    await assert.rejects(run({ html: '<html>unavailable</html>' }), { code: 'page_station_mismatch' });
    await assert.rejects(run({ html: page().replace('"API_URL"', '"UNKNOWN_URL"') }), { code: 'page_config_invalid' });
    await assert.rejects(run({ html: '<script id="app-root-state">not JSON</script>' }), { code: 'legacy_json_invalid' });
    await assert.rejects(run({ status: 503 }), { code: 'page_response_invalid' });
    await assert.rejects(run({ target: { ...TARGET, unit: 'K' } }), { code: 'target_invalid' });
});

test('cache expiry does not introduce a new cutoff for valid calendar-day forecasts', async () => {
    const data = body();
    data.expirationTimeUtc = [NOW / 1000 - 1, NOW / 1000 + 3600];
    assert.equal((await run({ data })).rows.length, 2);
});

test('reject historical-only, duplicate, offset-mismatched and malformed forecast rows', async () => {
    const cases = [
        [d => { d.validTimeLocal = ['2026-10-30T07:00:00+0800', '2026-10-31T07:00:00+0800']; }, 'forecast_dates_stale'],
        [d => { d.validTimeLocal[1] = d.validTimeLocal[0]; }, 'forecast_date_invalid'],
        [d => { d.validTimeLocal[0] = 'garbage'; }, 'forecast_date_invalid'],
        [d => { d.validTimeLocal[0] = '2026-11-02T20:00:00-0500'; }, 'forecast_date_invalid'],
        [d => { d.calendarDayTemperatureMax.pop(); }, 'forecast_schema_invalid'],
        [d => { d.calendarDayTemperatureMin.pop(); }, 'forecast_schema_invalid'],
        [d => { d.calendarDayTemperatureMax[0] = null; }, 'forecast_temperature_invalid'],
        [d => { d.calendarDayTemperatureMax[0] = Infinity; }, 'forecast_temperature_invalid'],
        [d => { d.calendarDayTemperatureMax[0] = '91'; }, 'forecast_temperature_invalid'],
        [d => { d.calendarDayTemperatureMin[0] = 100; }, 'forecast_temperature_invalid']
    ];
    for (const [mutate, code] of cases) {
        const data = body(); mutate(data);
        await assert.rejects(run({ data }), { code });
    }
});

test('safe diagnostics never expose upstream error messages, proxy credentials or keys', async () => {
    const unsafe = new Error(`upstream https://api.weather.com/?apiKey=${TEST_KEY} user:secret@proxy`);
    unsafe.response = { status: 401 };
    await assert.rejects(run({ error: unsafe }), error => {
        assert.ok(error instanceof WundergroundForecastError);
        assert.equal(error.code, 'request_http_401');
        assert.equal(error.message, 'request_http_401');
        assert.ok(!JSON.stringify(error).includes(TEST_KEY));
        return true;
    });
});

test('collector logs once per fetch cycle and one recovery, leaving other providers untouched', async () => {
    const source = await readFile(new URL('../collectors/weather_flow_plus.js', import.meta.url), 'utf8');
    const method = source.match(/    async fetchWunderground\(target\) \{[\s\S]*?\n    \}\n/)[0];
    const logs = [];
    let failing = true;
    const context = vm.createContext({
        axios: { get() {} },
        logger: { warn: value => logs.push(value), info: value => logs.push(value) },
        fetchWundergroundForecast: async () => {
            if (failing) throw new WundergroundForecastError('page_station_mismatch');
            return [{ date: '2026-11-02', high: 33, low: 25 }];
        }
    });
    const collector = vm.runInContext(`({ wunderFailedStations: new Set(), async getAxiosProxy() { return false; }, ${method} })`, context);
    assert.equal((await collector.fetchWunderground(TARGET)).length, 0);
    assert.equal(logs.length, 1);
    assert.match(logs[0], /forecast unavailable.*WMKK.*page_station_mismatch/);
    failing = false;
    await collector.fetchWunderground(TARGET);
    await collector.fetchWunderground(TARGET);
    assert.equal(logs.length, 2);
    assert.match(logs[1], /recovered.*WMKK.*days=1/);
    assert.equal(collector.wunderFailedStations.size, 0);
});
