import Redis from 'ioredis';

const redis = new Redis({ host: '127.0.0.1', port: 6379 });

const CONFIG_KEY = 'poly:config:cities';
const PROXY_KEY = 'poly:proxylist';

// 城市配置列表
const cities = [
  { country: 'us', city: 'ny/new-york-city', station: 'KLGA', name: 'New York', unit: 'F', tz: 'America/New_York' },
  { country: 'us', city: 'tx/dallas', station: 'KDAL', name: 'Dallas', unit: 'F', tz: 'America/Chicago' },
  { country: 'us', city: 'ga/atlanta', station: 'KATL', name: 'Atlanta', unit: 'F', tz: 'America/New_York' },
  { country: 'us', city: 'wa/seatac', station: 'KSEA', name: 'Seattle', unit: 'F', tz: 'America/Los_Angeles' },
  { country: 'kr', city: 'incheon', station: 'RKSI', name: 'Seoul', unit: 'C', tz: 'Asia/Seoul' },
  { country: 'gb', city: 'london', station: 'EGLC', name: 'London', unit: 'C', tz: 'Europe/London' },
  { country: 'ar', city: 'ezeiza', station: 'SAEZ', name: 'Buenos Aires', unit: 'C', tz: 'America/Argentina/Buenos_Aires' },
  { country: 'ca', city: 'mississauga', station: 'CYYZ', name: 'Toronto', unit: 'C', tz: 'America/Toronto' },
  { country: 'us', city: 'fl/miami', station: 'KMIA', name: 'Miami', unit: 'F', tz: 'America/New_York' },
  { country: 'us', city: 'il/chicago', station: 'KORD', name: 'Chicago', unit: 'F', tz: 'America/Chicago' },
  { country: 'nz', city: 'wellington', station: 'NZWN', name: 'Wellington', unit: 'C', tz: 'Pacific/Auckland' },
  { country: 'tr', city: 'cubuk', station: 'LTAC', name: 'Ankara', unit: 'C', tz: 'Europe/Istanbul' },
];

// 代理列表
const proxies = [
  "192.46.187.108:6686:jjcxqsgk:7moya22r496n:82.22.69.101",
  "103.105.164.206:5375:jjcxqsgk:7moya22r496n:82.22.69.32",
  "9.142.208.196:5862:jjcxqsgk:7moya22r496n:82.22.69.34",
  "192.46.189.78:6071:jjcxqsgk:7moya22r496n:82.22.69.104",
  "9.142.8.134:5791:jjcxqsgk:7moya22r496n:82.22.89.224"
];

async function seed() {
  console.log('🌱 Seeding data to Redis...');
  
  const pipeline = redis.pipeline();
  
  // --- 1. 处理城市配置 (Hash) ---
  console.log('--- Cities Config ---');
  // pipeline.del(CONFIG_KEY); // 如果你想每次都完全重置城市配置，可以解开这行

  cities.forEach(city => {
    pipeline.hset(CONFIG_KEY, city.station, JSON.stringify(city));
    console.log(` -> Staged City: ${city.name} (${city.station})`);
  });

  // --- 2. 处理代理列表 (Set) ---
  console.log('--- Proxy List ---');
  // 建议：每次 Seed 代理时，先清空旧的列表，防止失效的代理一直留在池子里
  pipeline.del(PROXY_KEY); 
  
  proxies.forEach(proxy => {
    pipeline.sadd(PROXY_KEY, proxy);
    // 只打印 IP 以保持日志整洁
    console.log(` -> Staged Proxy: ${proxy.split(':')[0]}...`);
  });

  // --- 3. 执行 ---
  await pipeline.exec();
  console.log('✅ Done! Redis config & proxies updated.');
  process.exit(0);
}

seed();