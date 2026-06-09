/**
 * 简易 Web 可视化服务（无依赖，使用 Node 内置 http）
 *
 * 启动后访问 http://127.0.0.1:3000，三个 Tab 分别展示当前所有 active collector 的数据：
 *   - Forecast: weather_flow_plus → poly:forecast:<station>:<date>
 *   - Observations: weather_observations → poly:settlement|metar:<station>:<date>
 *   - NPM: npm_pricing → poly:npm:<companyId> + poly:config:companies
 *
 * 实时性：服务端订阅 collector 的 pub/sub 频道，通过 SSE 推送给前端，前端收到信号后重新拉一次对应 API。
 *
 * 默认 bind 127.0.0.1，仅本机访问。可通过 WEB_PORT / WEB_HOST 环境变量覆盖。
 */

import http from 'http';
import { readFile } from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import Redis from 'ioredis';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import timezone from 'dayjs/plugin/timezone.js';
import config from '../config/index.js';
import redis from '../core/redis.js';
import logger from '../utils/logger.js';

dayjs.extend(utc);
dayjs.extend(timezone);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PUBLIC_DIR = path.join(__dirname, 'public');

const PORT = parseInt(process.env.WEB_PORT) || 3000;
const HOST = process.env.WEB_HOST || '127.0.0.1';

const SUBSCRIBE_CHANNELS = [
    'poly:feed:weather_obs',
    'poly:feed:wethr_obs',
    'poly:feed:npm_pricing',
    'poly:feed:weather_forecast_4days',
];

const MIME = {
    '.html': 'text/html; charset=utf-8',
    '.js': 'application/javascript; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.svg': 'image/svg+xml',
};

let sub = null;
let server = null;
let heartbeatTimer = null;
const sseClients = new Set();

// ===== 数据读取辅助 =====

async function loadCities() {
    const raw = await redis.hgetall('poly:config:cities');
    return Object.values(raw).map(s => {
        try { return JSON.parse(s); } catch { return null; }
    }).filter(c => c && c.fetchData === true);
}

async function loadCompanies() {
    const raw = await redis.hgetall('poly:config:companies');
    return Object.values(raw).map(s => {
        try { return JSON.parse(s); } catch { return null; }
    }).filter(c => c && c.id && c.fetchData !== false);
}

/** 把 forecast hash 的 field（JSON string）解析回对象 */
function parseForecastHash(hash) {
    const out = {};
    if (!hash) return out;
    for (const [k, v] of Object.entries(hash)) {
        if (k === 'updated_at') { out.updated_at = Number(v); continue; }
        try { out[k] = JSON.parse(v); } catch { out[k] = v; }
    }
    return out;
}

async function getForecastData() {
    const cities = await loadCities();
    const results = [];
    for (const c of cities) {
        const tz = c.tz || 'UTC';
        const today = dayjs().tz(tz);
        const dates = Array.from({ length: 4 }, (_, i) => today.add(i, 'day').format('YYYY-MM-DD'));

        const pipe = redis.pipeline();
        for (const d of dates) pipe.hgetall(`poly:forecast:${c.station}:${d}`);
        const rs = await pipe.exec();

        const days = rs.map(([err, data], i) => ({
            date: dates[i],
            sources: parseForecastHash(data || {})
        }));
        results.push({ city: c, days });
    }
    return { cities: results, ts: Date.now() };
}

function parseDetailList(raw) {
    if (!raw) return [];
    try { const v = JSON.parse(raw); return Array.isArray(v) ? v : []; }
    catch { return []; }
}

async function getObservationsData() {
    const cities = await loadCities();
    const results = [];
    for (const c of cities) {
        const tz = c.tz || 'UTC';
        const now = dayjs().tz(tz);
        const today = now.format('YYYY-MM-DD');
        const yesterday = now.subtract(1, 'day').format('YYYY-MM-DD');

        const pipe = redis.pipeline();
        // 主状态 hash 6 个（settlement / metar / wethr × today / yesterday）
        pipe.hgetall(`poly:settlement:${c.station}:${today}`);
        pipe.hgetall(`poly:settlement:${c.station}:${yesterday}`);
        pipe.hgetall(`poly:metar:${c.station}:${today}`);
        pipe.hgetall(`poly:metar:${c.station}:${yesterday}`);
        pipe.hgetall(`poly:wethr:${c.station}:${today}`);
        pipe.hgetall(`poly:wethr:${c.station}:${yesterday}`);
        // 明细列表 6 个
        pipe.get(`poly:settlement:obs:${c.station}:${today}`);
        pipe.get(`poly:settlement:obs:${c.station}:${yesterday}`);
        pipe.get(`poly:metar:obs:${c.station}:${today}`);
        pipe.get(`poly:metar:obs:${c.station}:${yesterday}`);
        pipe.get(`poly:wethr:obs:${c.station}:${today}`);
        pipe.get(`poly:wethr:obs:${c.station}:${yesterday}`);
        const rs = await pipe.exec();

        const enrich = (hash, detail) => ({
            ...(hash || {}),
            detail: parseDetailList(detail)
        });

        results.push({
            city: c,
            local_now: now.format('YYYY-MM-DD HH:mm'),
            settlement: {
                today:     enrich(rs[0][1], rs[6][1]),
                yesterday: enrich(rs[1][1], rs[7][1]),
            },
            metar: {
                today:     enrich(rs[2][1], rs[8][1]),
                yesterday: enrich(rs[3][1], rs[9][1]),
            },
            wethr: {
                today:     enrich(rs[4][1], rs[10][1]),
                yesterday: enrich(rs[5][1], rs[11][1]),
            },
        });
    }
    return { cities: results, ts: Date.now() };
}

async function getNPMData() {
    const companies = await loadCompanies();
    if (companies.length === 0) return { companies: [], ts: Date.now() };
    const pipe = redis.pipeline();
    for (const c of companies) pipe.hgetall(`poly:npm:${c.id}`);
    const rs = await pipe.exec();
    return {
        companies: companies.map((c, i) => ({ config: c, state: rs[i][1] || {} })),
        ts: Date.now(),
    };
}

// ===== 请求处理 =====

async function handleApi(pathname) {
    if (pathname === '/api/forecast')     return await getForecastData();
    if (pathname === '/api/observations') return await getObservationsData();
    if (pathname === '/api/npm')          return await getNPMData();
    return null;
}

async function serveStatic(pathname, res) {
    let rel = pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, '');
    if (rel.includes('..')) {
        res.writeHead(403); res.end('forbidden'); return;
    }
    const full = path.join(PUBLIC_DIR, rel);
    // defense-in-depth：startsWith 仅判前缀会把 /publicXYZ 误判为合法，
    // 必须加 path.sep 分隔
    if (full !== PUBLIC_DIR && !full.startsWith(PUBLIC_DIR + path.sep)) {
        res.writeHead(403); res.end('forbidden'); return;
    }
    try {
        const content = await readFile(full);
        const ext = path.extname(rel).toLowerCase();
        res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
        res.end(content);
    } catch {
        res.writeHead(404); res.end('not found');
    }
}

function attachSSE(req, res) {
    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache, no-transform',
        'Connection': 'keep-alive',
        'X-Accel-Buffering': 'no', // 防止 nginx 缓冲
    });
    res.write(`event: ready\ndata: {"status":"connected"}\n\n`);
    sseClients.add(res);
    req.on('close', () => sseClients.delete(res));
}

async function requestHandler(req, res) {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const pathname = url.pathname;

    if (pathname === '/events') return attachSSE(req, res);

    if (pathname.startsWith('/api/')) {
        try {
            const data = await handleApi(pathname);
            if (data === null) { res.writeHead(404); res.end('not found'); return; }
            res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
            res.end(JSON.stringify(data));
        } catch (e) {
            logger.error(`[Web] API error: ${e.message}`);
            res.writeHead(500); res.end(e.message);
        }
        return;
    }

    return serveStatic(pathname, res);
}

// ===== Pub/Sub 广播给 SSE 客户端 =====

/**
 * @param {string} payload 完整的 SSE 帧（包含尾部 \n\n）
 */
function writeToAllClients(payload) {
    const dead = [];
    for (const c of sseClients) {
        try { c.write(payload); } catch { dead.push(c); }
    }
    dead.forEach(c => sseClients.delete(c));
}

function broadcastSSE(channel, raw) {
    const eventName = channel.replace('poly:feed:', '');
    writeToAllClients(`event: ${eventName}\ndata: ${raw}\n\n`);
}

// ===== 启停 =====

export async function startWebServer() {
    sub = new Redis(config.redisUrl);
    await sub.subscribe(...SUBSCRIBE_CHANNELS);
    sub.on('message', broadcastSSE);

    server = http.createServer(requestHandler);

    // Promise 化 listen，端口占用等错误能正确 reject 出来
    await new Promise((resolve, reject) => {
        const onError = (e) => { server.off('listening', onListen); reject(e); };
        const onListen = () => {
            server.off('error', onError);
            logger.info(`🌐 Web UI:  http://${HOST}:${PORT}`);
            // listen 成功后再挂常规 error 监听（避免静默崩溃）
            server.on('error', (e) => logger.error(`[Web] server error: ${e.message}`));
            resolve();
        };
        server.once('error', onError);
        server.once('listening', onListen);
        server.listen(PORT, HOST);
    });

    // SSE keep-alive：每 25s 发一个 comment 帧防代理切断；走相同 dead-collect 路径
    heartbeatTimer = setInterval(() => writeToAllClients(`:hb\n\n`), 25 * 1000);

    return server;
}

export async function stopWebServer() {
    if (heartbeatTimer) clearInterval(heartbeatTimer);
    heartbeatTimer = null;
    if (sub) { await sub.quit().catch(() => {}); sub = null; }
    if (server) await new Promise(r => server.close(r));
    server = null;
}
