const assert = require('assert'); const WebSocket = require('ws'); const { createRelay } = require('./server.js');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const cli = (port, origin) => new Promise((res, rej) => { const ws = new WebSocket('ws://127.0.0.1:' + port + '/ws', origin ? { origin } : undefined); ws.msgs = []; ws.on('message', (d) => ws.msgs.push(JSON.parse(d))); ws.on('open', () => res(ws)); ws.on('error', rej); ws.on('unexpected-response', (q, r) => rej(new Error('http ' + r.statusCode))); });
const J = (ws, o) => ws.send(JSON.stringify(o)); const last = (ws, t) => ws.msgs.filter((m) => m.t === t).pop();
(async () => {
  const r = createRelay({ maxRooms: 3, maxPerIp: 30, roomSize: 3, createPerMin: 100 }); await new Promise((ok) => r.server.listen(0, ok)); const port = r.server.address().port;
  const a = await cli(port), b = await cli(port), c = await cli(port); await sleep(50);
  assert(last(a, 'hi'), 'hello'); 
  J(b, { t: 'join', room: 'vs-abcd' }); await sleep(60); assert.equal(last(b, 'err').why, 'none', 'joining a missing room says none');
  J(a, { t: 'join', room: 'vs-abcd', create: true }); await sleep(60); assert(last(a, 'ok'), 'host creates');
  J(c, { t: 'join', room: 'vs-abcd', create: true }); await sleep(60); assert.equal(last(c, 'err').why, 'taken', 'duplicate code refused');
  J(b, { t: 'join', room: 'vs-abcd' }); await sleep(60); assert(last(b, 'ok'), 'guest joins');
  J(a, { t: 'pres', room: 'vs-abcd', patch: { host: 1, n: 'H' } }); await sleep(80);
  assert.equal(last(b, 'peers').list.length, 2, 'peers list'); assert(last(b, 'peers').list.some((p) => p[1].host === 1), 'presence relayed');
  J(a, { t: 'emit', room: 'vs-abcd', topic: 's', data: { k: 's', x: 1 } }); await sleep(60); const ev = last(b, 'ev'); assert.equal(ev.data.x, 1); assert.equal(ev.from, last(a, 'hi').id);
  assert(!a.msgs.some((m) => m.t === 'ev'), 'no echo to sender');
  J(c, { t: 'emit', room: 'vs-abcd', topic: 's', data: { x: 9 } }); await sleep(60); assert(!b.msgs.some((m) => m.t === 'ev' && m.data.x === 9), 'non-members cannot emit');
  const d = await cli(port), e = await cli(port); J(c, { t: 'join', room: 'vs-abcd' }); J(d, { t: 'join', room: 'vs-abcd' }); J(e, { t: 'join', room: 'vs-abcd' }); await sleep(100);
  assert.equal(last(e, 'err').why, 'full', 'room size cap (3)');
  J(d, { t: 'join', room: 'vs-bad room!' }); await sleep(40);
  for (const n of ['vs-r1', 'vs-r2']) { const x = await cli(port); J(x, { t: 'join', room: n, create: true }); await sleep(40); }
  const f = await cli(port); J(f, { t: 'join', room: 'vs-r3', create: true }); await sleep(60); assert.equal(last(f, 'err').why, 'full', 'room cap');
  a.close(); b.close(); await sleep(100); const h = await new Promise((ok) => require('http').get('http://127.0.0.1:' + port + '/health', (q) => { let s = ''; q.on('data', (x) => s += x); q.on('end', () => ok(JSON.parse(s))); })); assert(h.ok && h.rooms >= 2, 'health');
  r.close();
  const r2 = createRelay({ maxPerIp: 2, origins: ['https://good.example'] }); await new Promise((ok) => r2.server.listen(0, ok)); const p2 = r2.server.address().port;
  await assert.rejects(cli(p2, 'https://evil.example'), /403/); const g1 = await cli(p2, 'https://good.example'), g2 = await cli(p2, 'https://good.example'); J(g1, { t: 'join', room: 'vs-g1', create: true }); J(g2, { t: 'join', room: 'vs-g2', create: true }); await sleep(80); await assert.rejects(cli(p2, 'https://good.example'), /429/);   // both are in rooms, so nothing can be evicted g1.close(); g2.close(); r2.close();
  // leaked sockets: an old client never closes its connection after leaving. They must not lock a player out, and they must not live forever.
  const r3 = createRelay({ maxPerIp: 2, idleMs: 150, beatMs: 60 }); await new Promise((ok) => r3.server.listen(0, ok)); const p3 = r3.server.address().port;
  const l1 = await cli(p3), l2 = await cli(p3); const l3 = await cli(p3); assert(last(l3, 'hi'), 'a third connection gets in by evicting a room-less one'); await sleep(60);
  assert(l1.readyState !== 1 || l2.readyState !== 1, 'one leaked socket was dropped');
  await sleep(400); assert.equal(r3.wss.clients.size, 0, 'room-less sockets are swept after the idle limit');
  const k1 = await cli(p3); J(k1, { t: 'join', room: 'vs-keep', create: true }); await sleep(500); assert.equal(k1.readyState, 1, 'a socket inside a room is never swept'); r3.close();
  r2.close(); console.log('relay: all tests passed'); process.exit(0);
})().catch((e) => { console.error('FAIL', e); process.exit(1); });
