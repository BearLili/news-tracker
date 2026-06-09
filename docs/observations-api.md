# Weather Observations — 策略接入文档

策略 agent 消费温度观测数据用，**单文档自包含**。读完即可写订阅代码。

> 模块：`src/collectors/weather_observations.js`
> 数据落地：Redis（本机）
> 推送方式：Redis Pub/Sub
> 配置入口：`poly:config:cities` Hash

---

## 1. 数据源总览

每个城市最多有 **3 路并行观测数据**：

| Source     | 来源                                  | 频率                   | 用途                                   |
|------------|---------------------------------------|------------------------|----------------------------------------|
| settlement | api.weather.com (TWC / Wunderground)  | 18s 全城（后台跑）    | **Polymarket 裁决基准**（最稳）        |
| metar      | aviationweather.gov                   | 3s hot / 15s cold      | 近实时实测（结算前 1.5min 拿到 METAR） |
| wethr      | wethr.net SSE Push                    | 推送驱动（5min HFM）   | 仅 US 5 站，比 aviationweather 早 1-3min |

> **国际站只有 settlement + metar**；wethr Push 上限 5 站，只在 US 配置（KLGA / KORD / KSFO / KMIA / KSEA）。

---

## 2. Redis Keys 一览

### 2.1 配置（只读）

| Key                     | Type | 说明                                                        |
|-------------------------|------|-------------------------------------------------------------|
| `poly:config:cities`    | Hash | field = ICAO (e.g. `KLGA`), value = JSON（见下）             |
| `poly:proxylist`        | Set  | 代理列表，策略一般不用关心                                   |

**city JSON 字段**（每个 city hash value 的 JSON 结构）：

```json
{
  "country": "us",
  "city": "ny/new-york-city",
  "station": "KLGA",
  "name": "New York",
  "unit": "F",
  "tz": "America/New_York",
  "fetchData": true,
  "sliceMinute": 51,
  "wethrEnabled": true
}
```

| 字段          | 含义                                                                                          |
|---------------|-----------------------------------------------------------------------------------------------|
| `country`     | ISO 国家代码（lower case）                                                                    |
| `station`     | **ICAO 4 字母代码**——所有 Redis key 都以此为标识                                              |
| `name`        | 显示名                                                                                        |
| `unit`        | **`F`** 或 **`C`** —— Redis 里所有温度都按这个单位存（settlement/metar/wethr 三路都已转换好） |
| `tz`          | IANA 时区——`date` 字段是按这个时区的本地日                                                    |
| `fetchData`   | 是否参与采集                                                                                  |
| `sliceMinute` | METAR 整点报的分钟（UTC），自动探测 OR 手工配置；策略可读但通常无需用                          |
| `wethrEnabled`| 是否订阅 wethr Push（仅 US 5 站会是 true）                                                    |

### 2.2 主状态 Hash

按城市 × 日期 × 数据源各一个 Hash：

```
poly:settlement:{station}:{date}     ←  TWC 结算数据
poly:metar:{station}:{date}          ←  aviationweather METAR
poly:wethr:{station}:{date}          ←  wethr Push（仅 US 5 站）
```

- `{station}` = ICAO 大写，如 `KLGA`
- `{date}` = `YYYY-MM-DD` **城市本地日**（按 city.tz 切）
- TTL: **14 天**

**Hash 字段（三种 source 完全对称）**：

| 字段                    | 类型      | 单位/格式            | 说明                                                                                  |
|-------------------------|-----------|----------------------|---------------------------------------------------------------------------------------|
| `station`               | string    | ICAO                 | 站点代码                                                                              |
| `name`                  | string    | -                    | 城市显示名                                                                            |
| `date`                  | string    | `YYYY-MM-DD`         | **城市本地日**（按 city.tz）                                                          |
| `unit`                  | string    | `F` / `C`            | 温度单位                                                                              |
| `source`                | string    | `settlement` / `metar` / `wethr` | 数据源标识                                                                |
| `high`                  | int (str) | °unit                | 当日最高温（整数，已按 unit 转换）                                                    |
| `low`                   | int (str) | °unit                | 当日最低温                                                                            |
| `latest_temp`           | int (str) | °unit                | **最新一条观测的温度**（不一定是 high！METAR/wethr 高敏感场景常用）                   |
| `obs_count`             | int (str) | -                    | 累计观测条数（48h 窗口内本日的）                                                       |
| `high_obs_ts`           | int (str) | **unix 秒 (UTC)**     | high 对应那条观测在站点端的发生时间                                                    |
| `low_obs_ts`            | int (str) | unix 秒              | low 对应观测时间                                                                       |
| `latest_obs_ts`         | int (str) | unix 秒              | 最新观测时间                                                                           |
| `first_obs_ts`          | int (str) | unix 秒              | 窗口内最早一条观测（注：窗口滑动会让此值向后跳，不是绝对意义的"今日首条"）              |
| `last_obs_ts`           | int (str) | unix 秒              | ⚠️ **= `latest_obs_ts` 的别名**，老代码兼容，新代码用 `latest_obs_ts`                  |
| `high_first_seen_at`    | int (str) | **unix 毫秒 (UTC)**   | **我们 collector 首次记录到这个 high 值的本地 wall clock**                            |
| `low_first_seen_at`     | int (str) | unix 毫秒            | 同理 low                                                                              |
| `latest_first_seen_at`  | int (str) | unix 毫秒            | 同理 latest                                                                            |
| `first_seen_at`         | int (str) | unix 毫秒            | 整个 hash key 首次创建时间                                                            |
| `updated_at`            | int (str) | unix 毫秒            | 最近一次写入时间                                                                       |
| `last_raw`              | string    | -                    | 最新一条 obs 的报文文本：metar=完整 METAR 字符串；wethr=合成的 "ASOS-HFM 68°F"；settlement 不存 |
| `latest_product`        | string    | -                    | 最新一条 obs 的类型：`ASOS-HFM` / `ASOS-HR` / `SPECI` / `METAR` / 空字符串 ← **策略只用这个判类型** |

> ⚠️ **重要时间单位差异**
> - `*_obs_ts`（数据维度时间）= **unix 秒**
> - `*_first_seen_at` / `updated_at`（系统维度时间）= **unix 毫秒**

> ⚠️ **important**: Redis Hash 所有字段值都是**字符串**，需要 `Number()` 转换才能算数。

### 2.3 观测明细 String（JSON）

```
poly:settlement:obs:{station}:{date}
poly:metar:obs:{station}:{date}
poly:wethr:obs:{station}:{date}
```

- Value = JSON 字符串
- 内容 = `[{ ts, temp, product? }, ...]`，按 `ts` 升序
- TTL: 14 天

样例：

```json
[
  { "ts": 1780620000, "temp": 25, "product": "ASOS-HR" },
  { "ts": 1780620300, "temp": 25, "product": "ASOS-HFM" },
  { "ts": 1780623600, "temp": 26, "product": "SPECI" },
  { "ts": 1780653600, "temp": 33, "product": "ASOS-HFM" }
]
```

**`product` 字段语义**（用来区分数据类型，**策略可据此过滤**）：

| Source | product 可能值 | 含义 | **是否进 Polymarket 结算** |
|---|---|---|---|
| `wethr` | `ASOS-HFM` | wethr SSE 推送的 5 分钟高频均温 | **❌ 不进结算** |
| `wethr` | `ASOS-HR` | wethr SSE 推送的整点 METAR | **✅** |
| `wethr` | `SPECI` | wethr SSE 推送的特选报 | **✅** |
| `metar` | `METAR` | aviationweather 拉的整点报 | **✅** |
| `metar` | `SPECI` | aviationweather 拉的特选报 | **✅** |
| `settlement` | `METAR` / `SPECI` / null | TWC 历史 metar 数据；TWC 偶尔不带类型字段 → null | **✅**（settlement 全是结算口径） |

字段缺失（`product` 不在对象里）= 老数据 / 数据源未提供——策略可视为兜底 metar 类。

**策略只算"结算口径 high"的样板**：

```js
const obsList = JSON.parse(await reader.get('poly:wethr:obs:KSFO:2026-06-09'));
const settleableMax = Math.max(...obsList
  .filter(o => o.product !== 'ASOS-HFM')   // 过滤掉 HFM
  .map(o => o.temp));
```

→ 用 detail 明细做：
- 时序图绘制
- 多源对比（同时段 settlement vs metar 哪个先到这个温度）
- 自定义阈值穿越检测
- 结算口径过滤（product != 'ASOS-HFM'）

### 2.4 Pub/Sub 最新快照（兜底缓存）

```
poly:latest:weather_observations    ← 与 poly:feed:weather_obs 最后一条相同
poly:latest:wethr_obs               ← 与 poly:feed:wethr_obs 最后一条相同
```

`GET` 拿到的字符串即下一节"Pub/Sub Payload"的内容。**策略冷启动时可以先 GET 一次拿最近状态**，再 SUBSCRIBE 拿后续增量。

---

## 3. Pub/Sub 推送

### 3.1 频道列表

| Channel                        | 来源 collector                | 触发条件                                              |
|--------------------------------|-------------------------------|-------------------------------------------------------|
| `poly:feed:weather_obs`        | weather_observations          | METAR / Settlement 任一城市的 high/low/latest/count 变化 |
| `poly:feed:wethr_obs`          | weather_observations (wethr)  | wethr buffer flush 出现 high/low/latest/count 变化     |
| `poly:feed:weather_forecast_4days` | weather_flow_plus         | 每 15 分钟全城市预报刷新                              |
| `poly:feed:npm_pricing`        | npm_pricing                   | NPM 私募估值变化（与温度无关，参考用）                 |

> 推荐策略**同时订阅** `weather_obs` + `wethr_obs` 两个频道（settlement / metar / wethr 三路都收）。

### 3.2 Payload 格式

所有频道的消息体都是 JSON：

```json
{
  "source": "weather_observations",   // 或 "wethr_push"
  "ts": 1780654321456,                // 发布时刻 (unix ms)
  "data": {
    "updates": [ /* 数组，每元素一条变化 */ ]
  }
}
```

### 3.3 weather_obs / wethr_obs 的 `update` 字段

每条 update 描述**一个城市的一种数据源在某一日**的状态变化：

```json
{
  "type": "wethr",                    // "settlement" / "metar" / "wethr"
  "station": "KSFO",
  "name": "San Francisco",
  "date": "2026-06-09",               // 城市本地日
  "unit": "F",
  "high": 68,
  "prev_high": 67,                    // 变化前的值（首次为 null）
  "low": 54,
  "prev_low": 54,
  "latest_temp": 68,
  "prev_latest_temp": 67,
  "latest_obs_ts": 1780654200,        // unix sec
  "prev_latest_obs_ts": 1780650600,
  "latest_product": "ASOS-HFM",       // 最新一条 obs 的类型：ASOS-HFM/ASOS-HR/SPECI/METAR/null
  "high_obs_ts": 1780654200,
  "low_obs_ts": 1780624800,
  "obs_count": 13,
  "prev_count": 12,
  "first_obs_ts": 1780611600,
  "last_obs_ts": 1780654200,
  "first_seen_at": 1780611800000      // unix ms
}
```

**触发推送的条件**（满足任一）：

- `high` 变化（含首次创建）
- `low` 变化
- `obs_count` 变化（即有新观测进来）
- `latest_temp` 变化

→ 推荐策略**比对 `high` vs `prev_high`** 判定"新高温事件"：

```js
const isNewHigh = u.prev_high !== null && Number(u.high) > Number(u.prev_high);
const isNewLow  = u.prev_low  !== null && Number(u.low)  < Number(u.prev_low);
```

> ⚠️ 首次创建时 `prev_* = null`，**不要当成新高/新低**——只是初始化。

### 3.4 `latest_product` 字段说明

**update 里只携带"最新一条 obs 的 product"**——不含每条 obs 的 product（那个要 GET detail key）。

| `latest_product` 值 | 含义 | 数据源 |
|---|---|---|
| `"ASOS-HFM"` | wethr 5-min HFM 推送 | wethr |
| `"ASOS-HR"` | wethr 整点 METAR | wethr |
| `"SPECI"` | wethr 特选报 / aviationweather SPECI / TWC SPECI | wethr/metar/settlement |
| `"METAR"` | aviationweather 整点 METAR / TWC METAR | metar/settlement |
| `null` | TWC 没标 metar_type 的老数据兜底 | settlement |

**用法举例**：

```js
sub.on('message', (channel, raw) => {
  const msg = JSON.parse(raw);
  for (const u of msg.data.updates) {
    if (u.type === 'wethr' && u.latest_product === 'ASOS-HFM') {
      // 5-min HFM 新数据：可用于预测下个整点 METAR
      // 不进结算！
    }
    if (u.type === 'wethr' && (u.latest_product === 'ASOS-HR' || u.latest_product === 'SPECI')) {
      // 进结算口径，与 metar/settlement 最终一致
    }
  }
});
```

### 3.5 推送顺序保证

| 维度 | 保证 |
|---|---|
| **同频道内** | Redis pub/sub 严格按 publish 顺序投递 ✅ |
| **同一 publish 消息内的 updates** | 数组按 collector 处理顺序（通常城市配置序） |
| **跨频道 (weather_obs vs wethr_obs)** | **不保证**——策略要靠每条 update 的 `latest_obs_ts` 自己排序 |
| **掉线重连** | Redis pub/sub **不持久化**——断线期间消息丢失。策略重连后用 `GET poly:latest:weather_observations` + `GET poly:latest:wethr_obs` 拿最近快照 |

---

## 4. 策略订阅样例代码（Node.js + ioredis）

```js
import Redis from 'ioredis';
const sub = new Redis({ host: '127.0.0.1', port: 6379 });
const reader = new Redis({ host: '127.0.0.1', port: 6379 }); // 单独连接读 hash

// 1) 冷启动：先 GET 最近一次快照
const lastSnapshot = await reader.get('poly:latest:weather_observations');
if (lastSnapshot) {
  const msg = JSON.parse(lastSnapshot);
  for (const u of msg.data.updates) handleUpdate(u);
}

// 2) 订阅增量
await sub.subscribe('poly:feed:weather_obs', 'poly:feed:wethr_obs');
sub.on('message', (channel, raw) => {
  let msg;
  try { msg = JSON.parse(raw); } catch { return; }
  for (const u of msg.data?.updates || []) handleUpdate(u);
});

function handleUpdate(u) {
  const isNewHigh = u.prev_high !== null && Number(u.high) > Number(u.prev_high);
  if (isNewHigh) {
    console.log(`🔥 NEW HIGH ${u.station} ${u.type}: ${u.prev_high}° → ${u.high}°${u.unit} on ${u.date}`);
    // 你的策略逻辑：触发买入/卖出/告警
  }
  // 也可以分别判 isNewLow / latest_temp / obs_count++
}
```

### 4.1 想要某城某日完整数据时

```js
// 主状态 hash
const settlement = await reader.hgetall(`poly:settlement:${station}:${date}`);
// 注：字段值都是 string，要数字得 Number(settlement.high)

// 全量观测明细
const detailStr = await reader.get(`poly:metar:obs:${station}:${date}`);
const detail = detailStr ? JSON.parse(detailStr) : [];
// detail = [{ts, temp}, ...]
```

### 4.2 想要城市配置时

```js
const cities = await reader.hgetall('poly:config:cities');
// cities 是 {KLGA: '{"country":"us",...}', ...}
const parsed = Object.values(cities).map(s => {
  try { return JSON.parse(s); } catch { return null; }
}).filter(Boolean);
```

---

## 5. 字段语义与时区注意事项

### 5.1 时区一览

| 字段                                | 时区              | 单位        |
|-------------------------------------|-------------------|-------------|
| `date`                              | **城市本地日**    | YYYY-MM-DD  |
| `*_obs_ts` / `*_obs_time`           | UTC               | unix **秒**  |
| `*_first_seen_at` / `updated_at`    | UTC               | unix **毫秒** |
| obs detail 内的 `ts`                | UTC               | unix **秒** |

### 5.2 把 obs_ts 转回城市本地时间显示

```js
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import timezone from 'dayjs/plugin/timezone.js';
dayjs.extend(utc); dayjs.extend(timezone);

const cityLocalStr = dayjs.unix(u.high_obs_ts).tz(cityTz).format('YYYY-MM-DD HH:mm');
```

### 5.3 温度单位

- US 站点 `unit=F`，所有温度字段都是华氏（整数）
- 国际站点多为 `unit=C`，所有温度字段都是摄氏
- 三个 source 都已转换好，**字段值就按 unit 字段判别即可**，不要二次换算

### 5.4 "新最高"判定的边界

```js
// 安全版本
function isStrictNewHigh(u) {
  if (u.prev_high === null || u.prev_high === undefined) return false; // 首次创建
  return Number(u.high) > Number(u.prev_high);
}
```

不要用 `u.high > u.prev_high`——Redis 出来的是字符串，会按字典序比较出错（"9" > "10" 是 true）。

---

## 6. 三路数据用途差异

| 场景 / 问题                                                 | 用哪一路                                |
|-------------------------------------------------------------|-----------------------------------------|
| Polymarket 会怎么结算？                                     | **settlement.high** / **settlement.low** |
| 现在到底多少度？                                             | wethr.latest_temp (US) 或 metar.latest_temp |
| METAR 一发布我能在多少秒内拿到？                            | metar 频道，hot 期 3s 内                |
| 5 分钟级别的 ASOS HFM 温度？                                | wethr 频道（仅 US 5 站）                |
| settlement 和 metar 哪个先到达某温度？                      | 对比两路 detail（按 obsTime） vs first_seen_at |
| 历史温度曲线？                                              | 任意源的 `*:obs:*` detail               |

---

## 7. 节奏 & 延迟期望

### Settlement
- 周期 18s 全城市批量拉（后台跑，单轮跑完才进下一轮，避免抢 proxy）
- API 自身延迟 5-10 min（TWC 端缓存）
- **策略不要用 settlement 做实时决策**——做结算对账

### METAR
- hot 期（站点 sliceMinute ~ +8 min）：3s/次直到捕获本期 METAR
- 捕获后降回 cold（15s/次）
- cold 期：15s/次
- API 自身延迟 ~1-3 min（aviationweather 缓存）
- **策略可用 latest_obs_ts 判定数据新鲜度**

### wethr Push
- SSE 长连接，事件驱动
- ASOS-HFM 每 5 分钟发一条
- 服务端推送→ 我们收到 → 写 Redis 通常 < 100ms
- 可能偶发 reconnect（每隔几分钟一次正常），有自动恢复
- **延迟最低的实时通道**，但仅 US 5 站

---

## 8. 边界 / 异常处理

### 8.1 字段可能为空 / null

| 字段                  | 何时为空                                          |
|-----------------------|---------------------------------------------------|
| `low_obs_ts`          | 当日还没采到任何观测时（早晨刚开始）              |
| `latest_temp`         | 首次创建 hash 时（observe 还没成功）              |
| `prev_*`              | 首次创建（用 `!== null` 判定）                    |
| `last_raw`            | settlement 没有（仅 metar/wethr 提供）            |
| 整个 hash 为空        | 当天该城市该源还没产生过任何数据                  |

### 8.2 wethr 特殊字段

wethr 原始事件可能带：
- `anomaly: true` → 异常观测，**我们 collector 已 drop**，不会进 Redis
- `suspect_temperature: true` → 同上

所以 Redis 里的 wethr 数据是已经过滤过的"干净版"。

### 8.3 数据滞后判定

```js
const lastObsAgoSec = Date.now() / 1000 - Number(u.latest_obs_ts);
if (lastObsAgoSec > 3600) {
  // 超过 1 小时没有新观测，数据可能 stale，下游判定要警惕
}
```

---

## 9. FAQ

**Q: 我能信哪个温度做交易？**
- **结算判定** → settlement.high / settlement.low（Polymarket 同源）
- **实时跟踪** → wethr.latest_temp (US) 或 metar.latest_temp

**Q: settlement 比 metar 慢，为什么还要它？**
- METAR 没有完整 SPECI（特殊报）历史，settlement 包括
- TWC 后端对原始 METAR 做了 QC（quality control），更稳

**Q: 我可以只看 wethr 不看 metar 吗？**
- US 5 站可以，wethr 是 metar 的超集 + 早到
- 但**结算必须看 settlement**，wethr 数据 ≠ 结算数据

**Q: 收到 update 之后我能直接判定"新最高记录今天"吗？**
- 几乎可以。但注意 `prev_high === null` 是首次写入，不算"新高"。

**Q: 时区怎么处理？**
- `date` 字段已经是城市本地日，不需要换算
- `*_obs_ts` 是 UTC unix sec，要展示给人看时按 `city.tz` 转

**Q: 半小时报的站点 hot 窗口怎么算？**
- 自动探测会识别出两个 slice（如 `[20, 50]`），分别按各自 +8 min 算 hot 窗口

---

## 10. 字段速查（按 type 分）

```
poly:settlement:{station}:{date}   →  Hash
poly:metar:{station}:{date}        →  Hash
poly:wethr:{station}:{date}        →  Hash

  字段（顺序无意义，HGETALL 任意顺序）：
    station, name, date, unit, source
    high, low, obs_count                     ← 整数（字符串存）
    latest_temp                              ← 整数
    high_obs_ts, low_obs_ts, latest_obs_ts   ← unix 秒（字符串存）
    first_obs_ts, last_obs_ts                ← unix 秒
    high_first_seen_at, low_first_seen_at    ← unix 毫秒
    latest_first_seen_at, first_seen_at      ← unix 毫秒
    updated_at                               ← unix 毫秒
    last_raw                                  ← 字符串（仅 metar/wethr）

poly:{settlement|metar|wethr}:obs:{station}:{date}  →  String (JSON)

  值结构：[ { ts: <unix sec>, temp: <int> }, ... ]

poly:feed:weather_obs                →  Pub/Sub channel
poly:feed:wethr_obs                  →  Pub/Sub channel
poly:latest:weather_observations     →  String（最新一条 weather_obs 消息）
poly:latest:wethr_obs                →  String（最新一条 wethr_obs 消息）

poly:config:cities                   →  Hash（field=ICAO, value=JSON）
```
