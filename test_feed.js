import Redis from 'ioredis';
import dayjs from 'dayjs';

// === 配置 ===
const CHANNEL = 'poly:feed:weather';

// 需要两个连接：
// 1. sub: 专门用来“听”广播 (不能干别的)
// 2. reader: 专门用来收到广播后去“查”数据
const sub = new Redis({ host: '127.0.0.1', port: 6379 });
const reader = new Redis({ host: '127.0.0.1', port: 6379 });

async function startMonitor(station) {
  console.clear();
  console.log(`📡 [Monitor] 正在监听频道: ${CHANNEL}:${station}`);
  console.log(`⏳ 等待采集器发送更新通知...\n`);
  console.log('------------------------------------------------------------------------------------------------------');
  console.log('  时间 (Utc+8)  |     时间 (Local)    |    来源   |  站点  | RT Rolling | Hist High | 🏆 Final High');
  console.log('------------------------------------------------------------------------------------------------------');

  // 1. 订阅频道
  await sub.subscribe(`${CHANNEL}:${station}`);

  // 2. 监听消息
  sub.on('message', async (channel, message) => {
    try {
      // 解析通知消息
      const event = JSON.parse(message);
      
      // 3. 收到通知后，立即去查该站点的最新状态
      // 这里使用传入的 station 或者 event 中的 station (理论上是一样的)
      const key = `poly:latest:weather:${station}`;
      const data = await reader.hgetall(key);

      if (!data || !data.target_date) return;

      // 4. 执行“读时合并”策略 (Read-Merge)
      const rtHigh = parseInt(data.rt_high); 
      const histHigh = parseInt(data.hist_high);
      const rtStationTime = data.rt_station_time;
      const histStationTime = data.hist_station_time;
      
      // 处理空值情况
      const valRT = isNaN(rtHigh) ? -999 : rtHigh;
      const valHist = isNaN(histHigh) ? -999 : histHigh;

      // 核心：取最大值作为最终决策
      const finalHigh = Math.max(valRT, valHist);

      // 5. 打印一行日志
      const timeStr = dayjs().format('HH:mm:ss');
      const sourceStr = event.type === 'rt' ? 'Realtime' : 'History ';
      
      // 优先显示对应来源的站点时间，如果没有则显示另一个，做个兜底
      let stationTimeStr = event.type === 'rt' ? rtStationTime : histStationTime;
      if (!stationTimeStr) stationTimeStr = event.type === 'rt' ? histStationTime : rtStationTime;
      if (!stationTimeStr) stationTimeStr = '--:--:--';

      const displayRT = valRT === -999 ? '--' : `${valRT}°`;
      const displayHist = valHist === -999 ? '--' : `${valHist}°`;
      const displayFinal = finalHigh === -999 ? '--' : `${finalHigh}°`;

      console.log(`   ${timeStr}     | ${stationTimeStr.padEnd(19)} | ${sourceStr}  | ${station}   | ${displayRT.padEnd(10)} | ${displayHist.padEnd(9)} | \x1b[32m${displayFinal}\x1b[0m`);

    } catch (e) {
      console.error('解析错误:', e);
    }
  });
}

// === 启动入口 ===

// 获取命令行第三个参数 (索引为2)
const targetStation = process.argv[2];

if (!targetStation) {
    console.error('\n❌ 错误: 未指定站点代码。');
    console.error('👉 用法示例: node monitor.js RKSI');
    console.error('👉 可用站点: KLGA, KDAL, KATL, KSEA, RKSI, EGLC, SAEZ, CYYZ, KMIA, KORD, NZWN, LTAC\n');
    process.exit(1);
}

// 转换为大写并启动 (防止输入 rksi 导致订阅失败)
startMonitor(targetStation.toUpperCase());