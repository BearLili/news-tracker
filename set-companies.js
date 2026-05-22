import Redis from 'ioredis';

const redis = new Redis({ host: '127.0.0.1', port: 6379 });

const CONFIG_KEY = 'poly:config:companies';

// NPM 私募公司配置，id 来源于 NPM 公司详情页 URL（company-<uuid>）
const companies = [
  {
    id: 'company-3e197763-4ff8-4d8c-bd1f-cc2792937757',
    name: 'Anthropic',
    fetchData: true,
  },
];

async function seed() {
  console.log('🌱 Seeding NPM companies to Redis...');
  const pipeline = redis.pipeline();
  companies.forEach(c => {
    pipeline.hset(CONFIG_KEY, c.id, JSON.stringify(c));
    console.log(` -> ${c.name} (${c.id})`);
  });
  await pipeline.exec();
  console.log('✅ Done.');
  process.exit(0);
}

seed();
