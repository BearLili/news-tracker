import axios from 'axios';
import logger from '../utils/logger.js';

const SSE_BASE = 'https://wethr.net:3443/api/v2/stream';
const HEARTBEAT_TIMEOUT_MS = 60_000;
const CONNECT_TIMEOUT_MS = 15_000;
const INITIAL_BACKOFF_MS = 1_000;
const MAX_BACKOFF_MS = 30_000;
const DISPLACED_COOLDOWN_MS = 30_000;
const RATE_LIMIT_COOLDOWN_MS = 60_000;
const AUTH_PROBE_INTERVAL_MS = 300_000;
// Node timers overflow above this value and would retry almost immediately.
const MAX_RETRY_AFTER_MS = 2_147_483_647;
const ignoreDisposedError = () => {};

/** Wethr SSE transport. Each connection owns its timers, controller and streams. */
class WethrPushClient {
    /**
     * @param {Object} opts
     * @param {string[]} opts.stations One to five ICAO stations.
     * @param {string} opts.apiKey Wethr credential (never logged).
     * @param {Function} [opts.onEvent] Receives the unchanged parsed SSE event.
     * @param {Function} [opts.onStatusChange] Receives redacted connection status.
     * @throws {Error} When stations or the credential are missing/invalid.
     */
    constructor({ stations, apiKey, onEvent, onStatusChange }) {
        if (!Array.isArray(stations) || stations.length === 0) {
            throw new Error('WethrPushClient: stations required (1-5)');
        }
        if (stations.length > 5) throw new Error('WethrPushClient: max 5 stations per connection');
        if (!apiKey) throw new Error('WethrPushClient: apiKey required');
        this.stations = stations.slice();
        this.apiKey = apiKey;
        this.onEvent = onEvent || (() => {});
        this.onStatusChange = onStatusChange || (() => {});
        this.running = false;
        this.attempt = null;
        this.reconnectTimer = null;
        this.backoffMs = INITIAL_BACKOFF_MS;
        this.lastEventId = null;
        this.stats = {
            connects: 0, events: 0, displaced: 0, errors: 0, rateLimited: 0,
            authFailures: 0, lastConnectedAt: null,
        };
    }

    /** @returns {void} Starts this client only; repeated calls are idempotent. */
    start() {
        if (this.running) return;
        this.running = true;
        this._connect();
    }

    /** @returns {Promise<void>} Cancels retry/handshake/watchdog and destroys owned resources. */
    async stop() {
        this.running = false;
        this._cancelReconnect();
        this._dispose(this.attempt);
        logger.info('[wethr] client stopped (connection resources released)');
    }

    /** @returns {Object} Copy of non-sensitive connection counters and station identifiers. */
    getStats() {
        return { ...this.stats, running: this.running, stations: this.stations.slice() };
    }

    _isCurrent(attempt) {
        return this.running && this.attempt === attempt && !attempt.disposed;
    }

    _notify(status) {
        try { this.onStatusChange(status); }
        catch { logger.warn('[wethr] status callback failed'); }
    }

    _dispose(attempt) {
        if (!attempt) return;
        // Invalidate before abort/destroy: they can synchronously invoke old callbacks.
        attempt.disposed = true;
        if (this.attempt === attempt) this.attempt = null;
        clearTimeout(attempt.connectTimer);
        clearTimeout(attempt.heartbeatTimer);
        attempt.connectTimer = null;
        attempt.heartbeatTimer = null;
        // A late axios resolution can attach resources after the original disposal.
        // Always release those too; do not early-return just because disposed is true.
        for (const resource of [attempt.stream, attempt.request]) {
            try {
                // Rejected/late streams have no live parser's error listener yet.
                if (resource?.on && !resource.listeners('error').includes(ignoreDisposedError)) {
                    resource.on('error', ignoreDisposedError);
                }
                resource?.destroy?.();
            }
            catch { logger.warn('[wethr] connection resource cleanup failed'); }
        }
        attempt.stream = null;
        attempt.request = null;
        attempt.controller.abort();
    }

    _finish(attempt, reason, delayMs) {
        if (!this._isCurrent(attempt)) return;
        this._dispose(attempt);
        this._notify({ state: reason, ...(delayMs === undefined ? {} : { cooldownMs: delayMs }) });
        this._scheduleReconnect(delayMs);
    }

    async _connect() {
        if (!this.running || this.attempt) return;
        this._cancelReconnect();
        const attempt = {
            controller: new AbortController(), stream: null, request: null,
            connectTimer: null, heartbeatTimer: null, disposed: false,
        };
        this.attempt = attempt;
        this.stats.connects++;
        attempt.connectTimer = setTimeout(() => {
            if (!this._isCurrent(attempt)) return;
            this.stats.errors++;
            logger.warn('[wethr] handshake timeout; reconnect scheduled');
            this._finish(attempt, 'connect_timeout');
        }, CONNECT_TIMEOUT_MS);
        const headers = {
            Accept: 'text/event-stream', 'User-Agent': 'news-tracker/1.0', 'Cache-Control': 'no-cache',
        };
        if (this.lastEventId) headers['Last-Event-ID'] = this.lastEventId;
        try {
            const resp = await axios.get(SSE_BASE, {
                params: { stations: this.stations.join(','), api_key: this.apiKey },
                responseType: 'stream', timeout: 0, headers, proxy: false,
                signal: attempt.controller.signal,
            });
            attempt.stream = resp.data;
            attempt.request = resp.request;
            clearTimeout(attempt.connectTimer);
            attempt.connectTimer = null;
            if (!this._isCurrent(attempt)) {
                this._dispose(attempt);
                return;
            }
            if (!attempt.stream?.on || !attempt.stream?.destroy) {
                throw new Error('Invalid stream response');
            }
            this._attachStream(attempt);
        } catch (error) {
            // Axios rejects non-2xx stream responses without consuming/destroying their body.
            // Capture both handles even if stop/timeout already invalidated this generation.
            attempt.stream = error?.response?.data || attempt.stream;
            attempt.request = error?.response?.request || error?.request || attempt.request;
            const current = this._isCurrent(attempt);
            this._dispose(attempt);
            if (!current) return;
            const status = error?.response?.status;
            if (status === 401 || status === 403) {
                this.stats.authFailures++;
                this.stats.errors++;
                logger.warn(`[wethr] authentication rejected (HTTP ${status}); check credential/subscription; next probe in ${AUTH_PROBE_INTERVAL_MS / 1000}s`);
                this._notify({ state: 'auth_failed', status, cooldownMs: AUTH_PROBE_INTERVAL_MS });
                this._scheduleReconnect(AUTH_PROBE_INTERVAL_MS);
                return;
            }
            if (status === 429) {
                this.stats.rateLimited++;
                const delay = this._retryAfter(error?.response?.headers?.['retry-after']);
                logger.warn(`[wethr] HTTP 429; reconnect in ${delay / 1000}s`);
                this._notify({ state: 'rate_limited', cooldownMs: delay });
                this._scheduleReconnect(delay);
                return;
            }
            this.stats.errors++;
            // Never print Axios messages, URLs, response bodies, or arbitrary server headers.
            const safeStatus = Number.isInteger(status) && status >= 100 && status <= 599 ? status : 'network';
            logger.warn(`[wethr] connection failed (${safeStatus}); reconnect scheduled`);
            this._notify({ state: 'connect_failed', status: safeStatus });
            this._scheduleReconnect();
        }
    }

    _retryAfter(raw) {
        const seconds = Number(raw);
        const delay = Number.isFinite(seconds) && seconds > 0
            ? seconds * 1000 : Date.parse(raw) - Date.now();
        return Number.isFinite(delay) && delay > 0
            ? Math.min(MAX_RETRY_AFTER_MS, Math.max(1000, delay)) : RATE_LIMIT_COOLDOWN_MS;
    }

    _attachStream(attempt) {
        this.stats.lastConnectedAt = Date.now();
        this.backoffMs = INITIAL_BACKOFF_MS;
        let buf = '';
        let curEvent = 'message';
        let curData = '';
        const dispatch = () => {
            if (!curData) { curEvent = 'message'; return; }
            let payload;
            try { payload = JSON.parse(curData); }
            catch { payload = curData; }
            const event = { type: curEvent, data: payload };
            curData = ''; curEvent = 'message';
            if (event.type === 'displaced') {
                this.stats.displaced++;
                this.lastEventId = null;
                logger.warn('[wethr] displaced; reconnect in 30s');
                this._finish(attempt, 'displaced', DISPLACED_COOLDOWN_MS);
                return;
            }
            if (event.type === 'heartbeat') return;
            this.stats.events++;
            try { this.onEvent(event); }
            catch { logger.warn('[wethr] event callback failed'); }
        };
        attempt.stream.on('data', (chunk) => {
            if (!this._isCurrent(attempt)) return;
            this._resetHeartbeat(attempt);
            buf += chunk.toString('utf8');
            let nl;
            while (this._isCurrent(attempt) && (nl = buf.indexOf('\n')) >= 0) {
                const line = buf.slice(0, nl).replace(/\r$/, '');
                buf = buf.slice(nl + 1);
                if (line === '') { dispatch(); continue; }
                if (line.startsWith(':')) continue;
                const ix = line.indexOf(':');
                const field = ix === -1 ? line : line.slice(0, ix);
                const value = ix === -1 ? '' : line.slice(ix + 1).replace(/^ /, '');
                if (field === 'event') curEvent = value;
                else if (field === 'data') curData = curData ? curData + '\n' + value : value;
                else if (field === 'id') this.lastEventId = value;
            }
        });
        attempt.stream.on('error', () => {
            if (!this._isCurrent(attempt)) return;
            this.stats.errors++;
            logger.warn('[wethr] stream error; reconnect scheduled');
            this._finish(attempt, 'stream_error');
        });
        // Some sockets close without an end event. The generation guard deduplicates both.
        for (const event of ['end', 'close']) {
            attempt.stream.on(event, () => {
                if (!this._isCurrent(attempt)) return;
                logger.warn('[wethr] stream closed; reconnect scheduled');
                this._finish(attempt, 'disconnected');
            });
        }
        this._resetHeartbeat(attempt);
        this._notify({ state: 'connected', stations: this.stations.slice() });
        logger.info(`[wethr] connected for ${this.stations.join(',')}`);
    }

    _resetHeartbeat(attempt) {
        clearTimeout(attempt.heartbeatTimer);
        if (!this._isCurrent(attempt)) return;
        attempt.heartbeatTimer = setTimeout(() => {
            if (!this._isCurrent(attempt)) return;
            logger.warn('[wethr] heartbeat timeout; reconnect scheduled');
            this._finish(attempt, 'heartbeat_timeout');
        }, HEARTBEAT_TIMEOUT_MS);
    }

    _scheduleReconnect(forceDelayMs) {
        this._cancelReconnect();
        if (!this.running || this.attempt) return;
        const delay = forceDelayMs ?? this.backoffMs;
        if (forceDelayMs === undefined) this.backoffMs = Math.min(MAX_BACKOFF_MS, this.backoffMs * 2);
        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = null;
            if (this.running) this._connect();
        }, delay);
    }

    _cancelReconnect() {
        clearTimeout(this.reconnectTimer);
        this.reconnectTimer = null;
    }
}

export default WethrPushClient;
