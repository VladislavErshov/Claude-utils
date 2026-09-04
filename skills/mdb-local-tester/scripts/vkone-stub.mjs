// Локальный стаб vkone для UI mdb (порт 8090).
// Запуск: node ~/.claude/skills/mdb-local-tester/scripts/vkone-stub.mjs
// В .env репозитория mdb: VKONE_API_URL=http://localhost:8090 (+ PROXY_API_PREFIX=/proxy)
// Запросы UI идут localhost:3012/proxy/_vkone/* -> vite proxy -> localhost:8090/*
// Контракты: src/shared/api/vkone/__generated__/data-contracts.ts (UserDC, AllFlagsDC)

import { createServer } from 'node:http';

const PORT = 8090;

const user = {
  id: 334804,
  username: 'vl.ershov',
  first_name: 'Владислав',
  last_name: 'Ершов',
  avatar_url:
    'https://home.vk.team/avatar/c85b067c26daf8bbf51d5460252763c5.jpg',
  intranet_url: 'https://intranet.vk.team/people/vl.ershov',
  email: 'vl.ershov@vkteam.ru',
};

const flags = {
  flags: {},
  valid: true,
};

const server = createServer((req, res) => {
  const send = (code, body) => {
    res.writeHead(code, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(body));
  };

  const url = req.url?.split('?')[0] ?? '';

  if (req.method === 'GET' && (url === '/api/v1/user/info' || url === '/api/v2/user/info')) {
    return send(200, user);
  }
  if (req.method === 'GET' && url === '/api/v2/allflags') {
    return send(200, flags);
  }
  if (req.method === 'GET' && url === '/api/v1/dcs') {
    return send(200, []);
  }

  console.warn(`[vkone-stub] no route: ${req.method} ${url}`);
  return send(404, { error_message: `vkone-stub: no route ${req.method} ${url}` });
});

server.listen(PORT, () => {
  console.log(`[vkone-stub] listening on http://localhost:${PORT}`);
});
