import axios from 'axios';
import logger from '../utils/logger.js';

/**
 * wethr Push SSE 客户端（Professional tier）
 *
 * API 约束（来自实测 + doc）：
 * - 单账号仅 1 条 connection，订阅最多 5 站
 * - 收到 `displaced` 事件说明被新连接踢下线，必须长冷却（30s+）再尝试，否则死循环抢 slot
 * - 心跳 30s/次，60s 没动静要主动重连
 * - 服务端建议每 ~6.7min 主动 refresh（refreshHint=400000ms），我们暂时不主动重连，靠心跳就够
 * - SSE 原始字段：`temperature` 是摄氏，`temperature_f` 才是华氏。本类只透传原始 event，
 *   下游 collector 决定单位转换
 *
 * 防御要点：
 * - axios stream + AbortController，stop() 立即释放 slot 让 server 端清理
 * - 重连指数退避 (1s → 30s 封顶)
 * - displaced 单独走 30s 长冷却
 */

const SSE_BASE = 'https://wethr.net:3443/api/v2/stream';
const HEARTBEAT_TIMEOUT_MS = 60_000;
const INITIAL_BACKOFF_MS = 1_000;
const MAX_BACKOFF_MS = 30_000;
const DISPLACED_COOLDOWN_MS = 30_000;
const RATE_LIMIT_COOLDOWN_MS = 60_000;  // 429 默认冷却（无 Retry-After 时）
const CONNECT_TIMEOUT_MS = 15_000;

class WethrPushClient {
    /**
     * @param {Object} opts
     * @param {string[]} opts.stations 订阅 ICAO 站点列表（1-5）
     * @param {string} opts.apiKey
     * @param {(event: {type: string, data: any}) => void} opts.onEvent 所有解析后事件回调
     * @param {(stats: Object) => void} [opts.onStatusChange] 连接状态变化回调（用于打日志/监控）
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
        this.connecting = false;       // 防止并发 _connect
        this.controller = null;
        this.stream = null;            // 留 ref 用于 stop() 主动 destroy（释放 keep-alive socket）
        this.heartbeatTimer = null;
        this.reconnectTimer = null;
        this.heartbeatTriggered = false;  // 心跳超时主动 abort 标志，区分 user stop
        this.backoffMs = INITIAL_BACKOFF_MS;
        this.lastEventId = null;
        this.connectedAt = null;
        this.stats = { connects: 0, events: 0, displaced: 0, errors: 0, rateLimited: 0, lastConnectedAt: null };
    }

    /** 把 URL 里的 api_key 字段抹掉给日志用，防 pm2 log 泄漏 */
    _safeUrl(url) { return String(url).replace(/api_key=[^&]+/, 'api_key=***'); }

    start() {
        if (this.running) return;
        this.running = true;
        this._connect();
    }

    /**
     * 主动关闭：必须 await！pm2 SIGTERM 关键路径，慢一秒就会让 wethr server 把
     * 我们当成旧连接挂着，新进程上线后被自己 displaced 死循环。
     */
    async stop() {
        this.running = false;
        this._cancelHeartbeat();
        this._cancelReconnect();
        if (this.controller) {
            try { this.controller.abort(); } catch {}
            this.controller = null;
        }
        // 主动 destroy stream，释放 keep-alive socket（仅 abort 不会断 keep-alive）
        if (this.stream) {
            try { this.stream.destroy?.(); } catch {}
            this.stream = null;
        }
        // 给底层 socket 一点时间发 FIN（最多 500ms）
        await new Promise(r => setTimeout(r, 500));
        logger.info('[wethr] client stopped (slot released)');
    }

    getStats() { return { ...this.stats, running: this.running, stations: this.stations.slice() }; }

    async _connect() {
        if (!this.running || this.connecting) return;
        this.connecting = true;
        const url = `${SSE_BASE}?stations=${this.stations.join(',')}&api_key=${this.apiKey}`;
        this.stats.connects++;
        let stream;

        // ⚠️ axios 的 timeout 在 stream 模式下是"socket 空闲超时"，会把 SSE 持久流当
        // 短请求 destroy。wethr heartbeat 30s/次，axios timeout < 30s 必死。
        // 改用：手工 connectTimer 只管初始握手（15s 没回响应头就 abort），
        // 拿到响应头后清掉该 timer；之后靠 app 层 HEARTBEAT_TIMEOUT_MS(60s) 看门狗。
        let connectTimer = null;
        try {
            this.controller = new AbortController();
            this.heartbeatTriggered = false;
            connectTimer = setTimeout(() => {
                try { this.controller?.abort(); } catch {}
            }, CONNECT_TIMEOUT_MS);
            const headers = {
                'Accept': 'text/event-stream',
                'User-Agent': 'news-tracker/1.0',
                'Cache-Control': 'no-cache',
            };
            if (this.lastEventId) headers['Last-Event-ID'] = this.lastEventId;
            const resp = await axios.get(url, {
                responseType: 'stream',
                timeout: 0,              // 关键：不让 axios 杀长流
                headers,
                proxy: false,
                signal: this.controller.signal,
            });
            clearTimeout(connectTimer);
            connectTimer = null;
            stream = resp.data;
            this.stream = stream;
        } catch (e) {
            if (connectTimer) { clearTimeout(connectTimer); connectTimer = null; }
            this.connecting = false;
            if (!this.running) return;
            if (e?.code === 'ERR_CANCELED' || /aborted/i.test(e?.message || '')) {
                if (this.heartbeatTriggered) {
                    this.heartbeatTriggered = false;
                    this._scheduleReconnect();
                }
                return;
            }

            // 429 = Too Many Requests：长冷却，优先用服务端 Retry-After 头
            // 不要走指数退避——这是 server 明确说"歇一会"的信号
            const status = e?.response?.status;
            if (status === 429) {
                this.stats.rateLimited++;
                const retryAfterRaw = e?.response?.headers?.['retry-after'];
                let cooldown = RATE_LIMIT_COOLDOWN_MS;
                if (retryAfterRaw) {
                    // Retry-After 可以是秒数或 HTTP date
                    const sec = parseInt(retryAfterRaw, 10);
                    if (isFinite(sec) && sec > 0) cooldown = sec * 1000;
                }
                logger.warn(`[wethr] 429 Too Many Requests, cooling down ${cooldown/1000}s (retry-after=${retryAfterRaw || 'n/a'})`);
                this._scheduleReconnect(cooldown);
                return;
            }
            // 5xx：服务端临时不可用，也走稍长冷却（用现有 backoff 但起步高一点）
            if (status && status >= 500) {
                logger.warn(`[wethr] ${status} Server error, cooling down ${this.backoffMs/1000}s`);
                this.stats.errors++;
                this._scheduleReconnect();
                return;
            }
            logger.warn(`[wethr] connect failed: ${e.message} (url=${this._safeUrl(url)})`);
            this.stats.errors++;
            this._scheduleReconnect();
            return;
        }
        this.connecting = false;

        this.connectedAt = Date.now();
        this.stats.lastConnectedAt = this.connectedAt;
        this.backoffMs = INITIAL_BACKOFF_MS;
        this.onStatusChange({ state: 'connected', stations: this.stations });
        logger.info(`[wethr] connected for ${this.stations.join(',')}`);
        this._resetHeartbeat();

        // ===== SSE 行解析器（每条事件以空行结束）=====
        let buf = '';
        let curEvent = 'message';
        let curData = '';

        const dispatch = () => {
            // 空 data 跳过（比如纯 comment）
            if (!curData) { curEvent = 'message'; return; }
            let payload;
            try { payload = JSON.parse(curData); }
            catch { payload = curData; }  // 非 JSON 原样传
            const event = { type: curEvent, data: payload };
            curData = ''; curEvent = 'message';

            // displaced：被踢下线，长冷却 30s + 清掉 lastEventId（slot 语义已断，
            // 重连不应带旧 id 让 server 重放过期事件）
            if (event.type === 'displaced') {
                this.stats.displaced++;
                this.lastEventId = null;
                logger.warn(`[wethr] DISPLACED — another connection took over, cooling down ${DISPLACED_COOLDOWN_MS/1000}s`);
                this.onStatusChange({ state: 'displaced', cooldownMs: DISPLACED_COOLDOWN_MS });
                try { this.controller?.abort(); } catch {}
                this._scheduleReconnect(DISPLACED_COOLDOWN_MS);
                return;
            }
            if (event.type === 'heartbeat') {
                // doc 说 30s/次，刷新心跳即可
                this._resetHeartbeat();
                return;
            }
            // connected / observation / new_high / new_low / 其他
            this.stats.events++;
            try { this.onEvent(event); }
            catch (e) { logger.warn(`[wethr] onEvent threw: ${e.message}`); }
        };

        const processLine = (line) => {
            if (line === '') { dispatch(); return; }
            if (line.startsWith(':')) return; // SSE comment
            const ix = line.indexOf(':');
            const field = ix === -1 ? line : line.slice(0, ix);
            const value = ix === -1 ? '' : line.slice(ix + 1).replace(/^ /, '');
            if (field === 'event') curEvent = value;
            else if (field === 'data') curData = curData ? curData + '\n' + value : value;
            else if (field === 'id') this.lastEventId = value;
            // 'retry' 字段忽略（用我们自己的退避）
        };

        stream.on('data', (chunk) => {
            this._resetHeartbeat();
            buf += chunk.toString('utf8');
            let nl;
            while ((nl = buf.indexOf('\n')) >= 0) {
                const line = buf.slice(0, nl).replace(/\r$/, '');
                buf = buf.slice(nl + 1);
                processLine(line);
            }
        });
        stream.on('error', (err) => {
            this._cancelHeartbeat();
            this.stream = null;
            // 用户 stop 触发的 abort → 不重连
            if (!this.running) return;
            // 心跳超时主动 abort 也会以 ERR_CANCELED 出现，仍要重连
            if (err?.code === 'ERR_CANCELED' && !this.heartbeatTriggered) return;
            this.heartbeatTriggered = false;
            logger.warn(`[wethr] stream error: ${err.message || err.code}`);
            this.stats.errors++;
            this._scheduleReconnect();
        });
        stream.on('end', () => {
            this._cancelHeartbeat();
            this.stream = null;
            if (!this.running) return;
            logger.warn('[wethr] stream ended unexpectedly');
            this._scheduleReconnect();
        });
    }

    _resetHeartbeat() {
        this._cancelHeartbeat();
        if (!this.running) return;
        this.heartbeatTimer = setTimeout(() => {
            logger.warn(`[wethr] heartbeat timeout (${HEARTBEAT_TIMEOUT_MS/1000}s), forcing reconnect`);
            this.heartbeatTriggered = true;   // 让 error 路径知道这是心跳触发，必须重连
            try { this.controller?.abort(); } catch {}
        }, HEARTBEAT_TIMEOUT_MS);
    }

    _cancelHeartbeat() {
        if (this.heartbeatTimer) { clearTimeout(this.heartbeatTimer); this.heartbeatTimer = null; }
    }

    _scheduleReconnect(forceDelayMs) {
        this._cancelReconnect();
        if (!this.running) return;
        const delay = forceDelayMs ?? this.backoffMs;
        // 指数退避（仅 forceDelayMs 未指定时才升）
        if (forceDelayMs === undefined) {
            this.backoffMs = Math.min(MAX_BACKOFF_MS, this.backoffMs * 2);
        }
        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = null;
            if (this.running) this._connect();
        }, delay);
    }

    _cancelReconnect() {
        if (this.reconnectTimer) { clearTimeout(this.reconnectTimer); this.reconnectTimer = null; }
    }
}

export default WethrPushClient;
