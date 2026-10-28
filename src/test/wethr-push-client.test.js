import assert from 'node:assert/strict';
import test from 'node:test';
import { PassThrough } from 'node:stream';
import http from 'node:http';
import axios from 'axios';
import WethrPushClient from '../collectors/wethrPushClient.js';
import logger from '../utils/logger.js';

const flush = async () => { for (let i = 0; i < 8; i++) await Promise.resolve(); };
const credentials = 'TEST_SECRET_MUST_NOT_APPEAR';

function harness(t, get) {
    const originals = { get: axios.get, setTimeout, clearTimeout, warn: logger.warn, info: logger.info };
    const timers = new Map();
    const logs = [];
    let sequence = 0;
    globalThis.setTimeout = (fn, delay) => {
        const id = ++sequence;
        timers.set(id, { fn, delay });
        return id;
    };
    globalThis.clearTimeout = id => timers.delete(id);
    axios.get = get;
    logger.warn = message => logs.push(message);
    logger.info = message => logs.push(message);
    const events = [];
    const statuses = [];
    const client = new WethrPushClient({
        stations: ['KLGA'], apiKey: credentials,
        onEvent: event => events.push(event), onStatusChange: status => statuses.push(status),
    });
    t.after(async () => {
        await client.stop();
        assert.equal(timers.size, 0);
        assert.ok(!logs.join('\n').includes(credentials));
        axios.get = originals.get;
        globalThis.setTimeout = originals.setTimeout;
        globalThis.clearTimeout = originals.clearTimeout;
        logger.warn = originals.warn;
        logger.info = originals.info;
    });
    const fire = async delay => {
        const matches = [...timers].filter(([, timer]) => timer.delay === delay);
        assert.equal(matches.length, 1, `expected one ${delay}ms timer`);
        const [id, timer] = matches[0];
        timers.delete(id);
        timer.fn();
        await flush();
    };
    return { client, timers, events, statuses, logs, fire };
}

function failedResponse(status, retryAfter) {
    const stream = new PassThrough();
    const request = new PassThrough();
    const error = new Error(`URL api_key=${credentials}; body=${credentials}`);
    error.response = { status, data: stream, request, headers: { 'retry-after': retryAfter } };
    return { error, stream, request };
}

test('repeated 401/403 destroys rejected streams, probes every 5 minutes, recovers', async t => {
    const failures = [];
    const success = new PassThrough();
    const h = harness(t, async () => {
        if (failures.length >= 40) return { data: success };
        const failure = failedResponse(failures.length % 2 ? 403 : 401);
        failures.push(failure);
        throw failure.error;
    });
    h.client.start();
    await flush();
    for (let i = 0; i < 40; i++) {
        assert.equal(h.timers.size, 1);
        assert.equal(h.client.attempt, null);
        assert.ok(failures.every(f => f.stream.destroyed && f.request.destroyed));
        await h.fire(300_000);
    }
    assert.equal(h.client.getStats().authFailures, 40);
    assert.equal(h.statuses.at(-1).state, 'connected');
    success.write('event: observation\ndata: {"temperature":12}\n\n');
    assert.deepEqual(h.events, [{ type: 'observation', data: { temperature: 12 } }]);
});

test('429, HTTP-date Retry-After, 5xx and network errors release resources', async t => {
    let next;
    const h = harness(t, async () => { throw next.error; });
    const cases = [
        [429, '120', 120_000], [429, 'bad secret ' + credentials, 60_000],
        [429, '3600', 3_600_000], [429, '9999999999999', 2_147_483_647],
        [429, new Date(Date.now() + 120_000).toUTCString(), null],
        [503, undefined, 1000], [undefined, undefined, 2000],
    ];
    for (const [status, retry, expectedDelay] of cases) {
        next = failedResponse(status, retry);
        h.client.running = true;
        await h.client._connect();
        assert.ok(next.stream.destroyed && next.request.destroyed);
        assert.equal(h.timers.size, 1);
        const delay = [...h.timers.values()][0].delay;
        if (expectedDelay) assert.equal(delay, expectedDelay);
        else assert.ok(delay >= 118_000 && delay <= 120_000);
        h.client._cancelReconnect();
    }
});

test('handshake timeout retries rather than silently stopping; old rejection cannot affect new connection', async t => {
    let rejectFirst;
    let calls = 0;
    const current = new PassThrough();
    const h = harness(t, () => ++calls === 1
        ? new Promise((_, reject) => { rejectFirst = reject; }) : Promise.resolve({ data: current }));
    h.client.start();
    const first = h.client.attempt;
    await h.fire(15_000);
    assert.ok(first.controller.signal.aborted);
    await h.fire(1000);
    const fresh = h.client.attempt;
    const late = failedResponse(401);
    rejectFirst(late.error);
    await flush();
    assert.ok(late.stream.destroyed && late.request.destroyed);
    assert.equal(h.client.attempt, fresh);
    assert.ok(!fresh.controller.signal.aborted);
    assert.equal(h.timers.size, 1);
    assert.equal(h.client.stats.authFailures, 0);
});

test('stop during handshake and restart: late success is destroyed without replacing current stream', async t => {
    let resolveFirst;
    let calls = 0;
    const fresh = new PassThrough();
    const h = harness(t, () => ++calls === 1
        ? new Promise(resolve => { resolveFirst = resolve; }) : Promise.resolve({ data: fresh }));
    h.client.start();
    const first = h.client.attempt;
    await h.client.stop();
    assert.ok(first.controller.signal.aborted);
    assert.equal(h.timers.size, 0);
    h.client.start();
    await flush();
    const active = h.client.attempt;
    const late = new PassThrough();
    const request = new PassThrough();
    resolveFirst({ data: late, request });
    await flush();
    assert.ok(late.destroyed && request.destroyed);
    late.emit('error', new Error(credentials));
    request.emit('error', new Error(credentials));
    assert.equal(h.client.attempt, active);
    assert.equal(h.timers.size, 1);
});

test('observation parsing, heartbeat and reconnect preserve Last-Event-ID; stale stream callbacks are inert', async t => {
    const streams = [new PassThrough(), new PassThrough()];
    const requests = [];
    const h = harness(t, async (_, options) => {
        requests.push(options);
        return { data: streams[requests.length - 1] };
    });
    h.client.start();
    h.client.start();
    await flush();
    assert.equal(requests.length, 1);
    streams[0].write('id: abc\r\nevent: observation\r\ndata: {"temperature":1,\r\n');
    streams[0].write('data: "temperature_f":33.8}\r\n\r\nevent: heartbeat\ndata: {}\n\n');
    assert.deepEqual(h.events, [{ type: 'observation', data: { temperature: 1, temperature_f: 33.8 } }]);
    await h.fire(60_000);
    assert.ok(streams[0].destroyed);
    await h.fire(1000);
    const active = h.client.attempt;
    assert.equal(requests[1].headers['Last-Event-ID'], 'abc');
    streams[0].emit('error', new Error(credentials));
    streams[0].emit('end');
    streams[0].emit('data', Buffer.from('id: stale\n\n'));
    assert.equal(h.client.attempt, active);
    assert.equal(h.client.lastEventId, 'abc');
    assert.equal(h.timers.size, 1);
});

test('displaced ignores remaining chunk, clears event ID, and schedules exactly one cooldown', async t => {
    const stream = new PassThrough();
    const h = harness(t, async () => ({ data: stream }));
    h.client.start();
    await flush();
    stream.write('id: old\nevent: displaced\ndata: {}\n\nid: stale\nevent: observation\ndata: {}\n\n');
    stream.emit('end');
    stream.emit('close');
    assert.equal(h.client.lastEventId, null);
    assert.ok(stream.destroyed);
    assert.equal(h.events.length, 0);
    assert.equal(h.timers.size, 1);
    assert.equal([...h.timers.values()][0].delay, 30_000);
});

test('unexpected error/end/close produces only one retry; callback exceptions stay redacted', async t => {
    const stream = new PassThrough();
    const h = harness(t, async () => ({ data: stream }));
    h.client.onStatusChange = () => { throw new Error(credentials); };
    h.client.onEvent = () => { throw new Error(credentials); };
    h.client.start();
    await flush();
    stream.write('event: observation\ndata: {}\n\n');
    stream.emit('error', new Error(credentials));
    stream.emit('end');
    stream.emit('close');
    assert.equal(h.timers.size, 1);
    assert.equal(h.client.stats.errors, 1);
    assert.ok(stream.destroyed);
});

test('real HTTP unfinished 401 bodies close sockets on each rejection', async t => {
    const sockets = new Set();
    let maximumSockets = 0;
    const server = http.createServer((request, response) => {
        response.writeHead(401, { 'Content-Type': 'application/json' });
        response.write('{"error":"Invalid or disabled API key"');
        // Intentionally never end: the failed client must destroy the body/socket.
    });
    server.on('connection', socket => {
        sockets.add(socket);
        maximumSockets = Math.max(maximumSockets, sockets.size);
        socket.on('close', () => sockets.delete(socket));
    });
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    const originalGet = axios.get;
    axios.get = (_, options) => originalGet(`http://127.0.0.1:${server.address().port}/stream`, options);
    const client = new WethrPushClient({ stations: ['KLGA'], apiKey: credentials });
    t.after(async () => {
        await client.stop();
        axios.get = originalGet;
        for (const socket of sockets) socket.destroy();
        await new Promise(resolve => server.close(resolve));
    });
    client.running = true;
    for (let i = 0; i < 24; i++) {
        await client._connect();
        for (let poll = 0; sockets.size && poll < 100; poll++) {
            await new Promise(resolve => setTimeout(resolve, 5));
        }
        assert.equal(sockets.size, 0, `socket leak after HTTP rejection ${i + 1}`);
        assert.equal(client.attempt, null);
        assert.equal(client.getStats().authFailures, i + 1);
    }
    assert.equal(maximumSockets, 1);
});

test('real HTTP stop closes both an active SSE socket and a pending handshake', async t => {
    const sockets = new Set();
    let sendHeaders = true;
    const server = http.createServer((request, response) => {
        if (sendHeaders) {
            response.writeHead(200, { 'Content-Type': 'text/event-stream' });
            response.write('event: observation\ndata: {"temperature":5}\n\n');
        }
        // With sendHeaders=false this intentionally hangs before the response headers.
    });
    server.on('connection', socket => {
        sockets.add(socket);
        socket.on('close', () => sockets.delete(socket));
    });
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    const originalGet = axios.get;
    axios.get = (_, options) => originalGet(`http://127.0.0.1:${server.address().port}/stream`, options);
    const events = [];
    const client = new WethrPushClient({
        stations: ['KLGA'], apiKey: credentials, onEvent: event => events.push(event),
    });
    t.after(async () => {
        await client.stop();
        axios.get = originalGet;
        for (const socket of sockets) socket.destroy();
        await new Promise(resolve => server.close(resolve));
    });
    for (const pendingHandshake of [false, true]) {
        sendHeaders = !pendingHandshake;
        client.start();
        for (let poll = 0; poll < 100 && (sockets.size === 0 || (!pendingHandshake && events.length === 0)); poll++) {
            await new Promise(resolve => setTimeout(resolve, 5));
        }
        assert.equal(sockets.size, 1);
        if (!pendingHandshake) assert.deepEqual(events, [{ type: 'observation', data: { temperature: 5 } }]);
        await client.stop();
        for (let poll = 0; sockets.size && poll < 100; poll++) {
            await new Promise(resolve => setTimeout(resolve, 5));
        }
        assert.equal(sockets.size, 0);
        assert.equal(client.attempt, null);
        assert.equal(client.reconnectTimer, null);
    }
});
