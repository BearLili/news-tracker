import * as cheerio from 'cheerio';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import timezone from 'dayjs/plugin/timezone.js';

dayjs.extend(utc);
dayjs.extend(timezone);

/** A safe, fixed diagnostic code; never includes a response URL or credentials. */
export class WundergroundForecastError extends Error {
    constructor(code) {
        super(code);
        this.name = 'WundergroundForecastError';
        this.code = code;
    }
}

function fail(code) {
    throw new WundergroundForecastError(code);
}

function parseCalendarDays(data, target, now) {
    const dates = data?.validTimeLocal;
    const highs = data?.calendarDayTemperatureMax;
    const lows = data?.calendarDayTemperatureMin;
    if (!Array.isArray(dates) || !Array.isArray(highs) || dates.length !== highs.length) {
        fail('forecast_schema_invalid');
    }
    if (lows != null && (!Array.isArray(lows) || lows.length !== dates.length)) {
        fail('forecast_schema_invalid');
    }
    const today = dayjs(now).tz(target.tz).format('YYYY-MM-DD');
    const rows = [];
    const seen = new Set();
    const convert = value => target.unit === 'C' ? Math.round((value - 32) * 5 / 9) : value;
    for (let i = 0; i < dates.length; i++) {
        const time = dates[i];
        if (typeof time !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:?\d{2}$/.test(time)) {
            fail('forecast_date_invalid');
        }
        const parsed = dayjs(time);
        if (!parsed.isValid()) fail('forecast_date_invalid');
        const date = parsed.tz(target.tz).format('YYYY-MM-DD');
        if (date !== time.slice(0, 10) || seen.has(date)) fail('forecast_date_invalid');
        seen.add(date);
        const high = highs[i];
        const low = lows?.[i];
        if (!Number.isFinite(high) || high < -150 || high > 150 ||
            (low != null && (!Number.isFinite(low) || low < -150 || low > high))) {
            fail('forecast_temperature_invalid');
        }
        rows.push({ date, high: convert(high), low: low == null ? null : convert(low) });
    }
    // WU can include yesterday before local 07:00. Preserve calendar-day indexing,
    // but never stamp a historical-only response as a new forecast in Redis.
    // expirationTimeUtc describes upstream cache refresh; the previous collector
    // did not gate on it, so this format repair does not add a freshness policy.
    if (!seen.has(today) || !rows.some(row => row.date > today)) fail('forecast_dates_stale');
    return rows;
}

function currentPageRequest($, target) {
    const stationNodes = $('current-conditions-view[data-icaocode]').toArray();
    const node = stationNodes.find(element => $(element).attr('data-icaocode') === target.station);
    if (!node) fail('page_station_mismatch');
    const placeId = $(node).attr('data-placeid');
    if (!/^[a-f0-9]{64}$/i.test(placeId || '')) fail('page_place_invalid');

    let config;
    for (const element of $('script:not([src])').toArray()) {
        const text = $(element).html() || '';
        if (!text.includes('"API_URL"')) continue;
        const match = text.match(/const\s+data\s*=\s*(\{[\s\S]*?\})\s*;\s*const\s+readOnly\b/);
        if (!match) continue;
        try { config = JSON.parse(match[1]); } catch { fail('page_config_invalid'); }
        break;
    }
    if (config?.API_URL !== 'https://api.weather.com' ||
        typeof config.API_KEY !== 'string' || !/^[a-zA-Z0-9_-]{16,128}$/.test(config.API_KEY)) {
        fail('page_config_invalid');
    }
    // This is the same ForecastDailyModel product used by WU's current website.
    // Obtain its public browser key from the page on each cycle; never persist it.
    const url = new URL('/v3/wx/forecast/daily/10day', config.API_URL);
    url.search = new URLSearchParams({ placeid: placeId, units: 'e', language: 'en-US', format: 'json', apiKey: config.API_KEY });
    return url.href;
}

/**
 * Fetch WU calendar-day forecasts in the configured C/F unit without Redis writes.
 * Supports the previous embedded payload and the current WU page + daily API.
 * @param {object} target City configuration with wunderUrl, station, unit and tz.
 * @param {object} options Injected axios-compatible get, proxy, and optional clock.
 * @returns {Promise<Array<{date:string, high:number, low:number|null}>>} Forecast rows.
 * @throws {WundergroundForecastError} Fixed reason on invalid page/data or request failure.
 */
export async function fetchWundergroundForecast(target, { get, proxy = false, now = Date.now() }) {
    if (!target.wunderUrl) return [];
    let pageUrl;
    try { pageUrl = new URL(target.wunderUrl); } catch { fail('page_url_invalid'); }
    if (pageUrl.protocol !== 'https:' || pageUrl.hostname !== 'www.wunderground.com' ||
        pageUrl.username || pageUrl.password || !['C', 'F'].includes(target.unit)) {
        fail('target_invalid');
    }
    const requestOptions = {
        headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Cache-Control': 'no-cache'
        },
        timeout: 15000,
        maxContentLength: 2 * 1024 * 1024,
        maxRedirects: 0,
        proxy: proxy || false
    };
    try {
        const response = await get(pageUrl.href, requestOptions);
        if (response.status !== 200 || typeof response.data !== 'string') fail('page_response_invalid');
        const $ = cheerio.load(response.data);
        const embedded = $('#app-root-state').html();
        if (embedded) {
            let state;
            try { state = JSON.parse(embedded); } catch { fail('legacy_json_invalid'); }
            for (const item of Object.values(state || {})) {
                if (item?.b?.calendarDayTemperatureMax && item.b.validTimeLocal) {
                    return parseCalendarDays(item.b, target, now);
                }
            }
            fail('legacy_forecast_missing');
        }
        const forecastUrl = currentPageRequest($, target);
        const forecast = await get(forecastUrl, requestOptions);
        if (forecast.status !== 200) fail('forecast_response_invalid');
        return parseCalendarDays(forecast.data, target, now);
    } catch (error) {
        if (error instanceof WundergroundForecastError) throw error;
        // Axios errors can contain proxy credentials and the public API key.
        const status = Number(error?.response?.status);
        if (Number.isInteger(status) && status >= 400 && status <= 599) fail(`request_http_${status}`);
        fail('request_failed');
    }
}
