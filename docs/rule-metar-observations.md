# 新规则的实时观测（2026-11-02）

这是 **AWC routine METAR 当日已观测极值**，不是最终结算、预报或概率。
适用规则取自已结算的 NYC/London/Kuala Lumpur/Taipei/Shenzhen/Paris
2026-10-30、31 市场，以及当前 2026-11-03 市场。消费端必须核对具体市场规则。

## 口径

- 原始 `rawOb` 解析，站点与 UTC 报文时间须和 AWC 元数据一致。
- 仅排除行首 `SPECI`；非整点、无 METAR 标签的 routine 报文仍计入。
- 优先 RMK 中 `T` 后恰好八位数字的温度部分（整数十分之一摄氏度）；
  否则用正文温度/露点组的温度。没有温度不参与，不能回退到 JSON `temp` 猜值。
- 按站点时区、包含 DST 的当地日期分组。先比较原始十分之一摄氏度极值，
  再转市场单位并一次舍入；半数向正无穷，负数同样适用。
- 同一观测时间有多个不同报文时，用最新 `receiptTime`；无法确定先后则标记不完整。
  不把原报与 COR 同时参与极值。报文纠错可改变已观测极值。
- 请求截断风险（400 行）、错误站点/时间、冲突、覆盖不到当地零点均标记不完整。
  `ok` 只表示响应覆盖和解析检查通过，不保证上游没有遗漏应发布的报文。

## Redis 契约

新增键，TTL 14 天：

| 键 | 类型 | 内容 |
|---|---|---|
| `poly:rule_metar:{ICAO}:{YYYY-MM-DD}` | Hash | 小型实时汇总 |
| `poly:rule_metar:obs:{ICAO}:{YYYY-MM-DD}` | JSON String | 被采用的原始 routine 报文及精确温度 |

汇总包含 `source=rule_metar`、`rule_version=awc-routine-t-v1`、
`station/date/unit/timezone`、`provisional=true`、`status`、`high/low`、
`obs_count/latest_obs_ts/high_obs_ts/low_obs_ts`、`updated_at`（毫秒）、
`window_start_ms`、`excluded_speci/ignored_no_temperature/invalid_count/revision_conflicts`。
观测时间均为 Unix 秒，原始明细的 `receipt_ms` 为毫秒。

`status` 为 `ok`、`incomplete` 或 `no_routine_temperature`。空数据 `high/low` 为空串，
绝不为零。消费者须核对身份、版本、完整性及新鲜度；不能只判断数值存在。
每个日期最多 400 行明细。原始报文在发生变化时才写，未变只每 30 秒更新小汇总心跳。

仍复用原来的 48 小时 AWC 请求、代理和连接；**没有新增 HTTP 轮询**。
新增写入单任务执行，不积压轮次，不在 Redis reconnect 时排新命令；
新字段失败不阻塞原有 `metar`、`settlement`、wethr、预测采集或原 Pub/Sub。
写入去重缓存最多 256 项；故障日志最多每分钟一条，健康状态变化另行记录。

## 刻意不冒充结算的部分

市场还有“次日第一条 routine/次日 ET 截止点”、WU 无报备用、无足够数据最低档、
异常数据澄清等最终结算条款。这次不实现最终裁决器，也不在盘中把缺报转成 WU
或最低档。昨日值也仍标记 provisional，历史更正不会被伪称为截止时的原始快照。
V2 只在 T0 使用此字段；最终收益仍以实际市场结算为准。

## 可复现验证

运行 `node --test src/test/weather-rule-metar.test.js`，全程离线。
公开原始证据保存在 `src/test/fixtures/rule-metar-resolved.json`，包含抓取时间、
12 个 AWC 原始 URL/JSON 快照、16 个 Gamma 已结算事件规则和获胜 bin。
这些是查询时的历史报文，不是过去结算瞬间冻结的快照；不据此声称减少了多少亏损。

| 日期 | 站点 | 计算 high | 计算 low（有对应低温市场） |
|---|---|---|---|
| Oct 30 | KLGA | 70°F → 70–71 获胜档 | 55°F → 54–55 获胜档 |
| Oct 31 | KLGA | 60°F → 60–61 获胜档 | 50°F → 50–51 获胜档 |
| Oct 30 / 31 | EGLC | 15 / 15°C | 9 / 8°C |
| Oct 30 / 31 | WMKK | 32 / 33°C | — |
| Oct 30 / 31 | RCSS | 29 / 26°C | — |
| Oct 30 / 31 | ZGSZ | 30 / 30°C | — |
| Oct 30 / 31 | LFPB | 16 / 15°C | — |

16/16 与市场实际已结算获胜档相符。这 12 个站日共排除 3 条 SPECI；
合成回归另覆盖 SPECI 极值改变结果、T 组与正文跨 Fahrenheit 档、负数舍入、
DST、COR 撤销温度、修订冲突、无报、截断、Redis 断线及原采集隔离。

来源：每条市场见 fixture 中 `slug` 对应 `https://polymarket.com/event/{slug}`；
AWC 接口说明 https://aviationweather.gov/data/api/ （400 行上限、204 无数据）。

## 后续上线顺序（本 PR 不部署）

先部署 news 并验证新键，再部署依赖它的 V2 PR。V1/DV1 不读取新键。
回退 V2 不需要改 Redis；回退 news 后新字段会过期，V2 的观测覆盖失效，
预测策略继续，不能把缺字段变成全局停买。
