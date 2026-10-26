/* Void Survivors room relay. It only forwards small messages between the players of a room; the game itself runs on the host's machine.
 * Env: PORT, ALLOWED_ORIGINS (comma list, empty = any), MAX_ROOMS, MAX_PER_IP, TRUST_PROXY=1 (set automatically on Render). */
'use strict';
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const NAME = /^[a-z0-9][a-z0-9_.-]{0,47}$/, TOPIC = /^[a-z][a-z0-9_.-]{0,47}$/;
function createRelay(opt = {}) {
  const O = Object.assign({ maxRooms: 400, maxPerIp: 12, roomSize: 10, createPerMin: 12, origins: [], trustProxy: false, maxPayload: 24 * 1024 }, opt);
  const rooms = new Map(), perIp = new Map(), creates = new Map(), dirty = new Set();
  const stats = { started: Date.now(), joins: 0, creates: 0, rejects: 0, msgs: 0 };
  const ipOf = (req) => { if (O.trustProxy) { const f = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim(); if (f) return f; } return req.socket.remoteAddress || '?'; };
  const server = http.createServer((req, res) => {
    const h = { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-store' };
    if (req.method === 'OPTIONS') { res.writeHead(204, h); return res.end(); }
    if (req.url === '/' || req.url.startsWith('/health')) { let clients = 0; for (const r of rooms.values()) clients += r.size; res.writeHead(200, h); return res.end(JSON.stringify({ ok: true, svc: 'vs-relay', v: 1, rooms: rooms.size, clients, up: Math.round((Date.now() - stats.started) / 1000) })); }
    res.writeHead(404, h); res.end('{"ok":false}');
  });
  const wss = new WebSocketServer({ server, path: '/ws', maxPayload: O.maxPayload, verifyClient: (info, cb) => {
    const ip = ipOf(info.req); const o = info.origin || '';
    if (O.origins.length && o && !O.origins.some((a) => o === a || o.endsWith(a.replace(/^\*/, '')))) { stats.rejects++; return cb(false, 403, 'origin'); }
    if ((perIp.get(ip) || 0) >= O.maxPerIp) { stats.rejects++; return cb(false, 429, 'too many connections'); }
    cb(true);
  } });
  const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
  function leaveRoom(c, name) { const r = rooms.get(name); if (!r) return; r.delete(c.id); c.rooms.delete(name); if (!r.size) rooms.delete(name); else dirty.add(name); }
  wss.on('connection', (ws, req) => {
    const ip = ipOf(req); perIp.set(ip, (perIp.get(ip) || 0) + 1);
    const c = { id: crypto.randomBytes(6).toString('hex'), rooms: new Set(), alive: true }; ws.isAlive = true; ws.on('pong', () => { ws.isAlive = true; });
    send(ws, { t: 'hi', id: c.id });
    let win = Date.now(), cnt = 0;
    ws.on('message', (raw) => {
      const nw = Date.now(); if (nw - win > 1000) { win = nw; cnt = 0; } if (++cnt > 240) return;   // flood guard
      stats.msgs++;
      let m; try { m = JSON.parse(raw); } catch { return; }
      if (!m || typeof m.room !== 'string' || !NAME.test(m.room)) return;
      if (m.t === 'join') {
        if (c.rooms.has(m.room)) return;
        if (c.rooms.size >= 2) return send(ws, { t: 'err', room: m.room, why: 'busy' });
        const ex = rooms.get(m.room);
        if (m.create) {   // the host opens a room: refuse if the code is already in use
          if (ex) return send(ws, { t: 'err', room: m.room, why: 'taken' });
          const mins = Math.floor(Date.now() / 60000), k = ip + ':' + mins; creates.set(k, (creates.get(k) || 0) + 1);
          if (creates.get(k) > O.createPerMin) return send(ws, { t: 'err', room: m.room, why: 'rate' });
          if (rooms.size >= O.maxRooms) return send(ws, { t: 'err', room: m.room, why: 'full' });
          rooms.set(m.room, new Map()); stats.creates++;
        } else if (!ex) return send(ws, { t: 'err', room: m.room, why: 'none' });
        else if (ex.size >= O.roomSize) return send(ws, { t: 'err', room: m.room, why: 'full' });
        rooms.get(m.room).set(c.id, { ws, presence: {} }); c.rooms.add(m.room); dirty.add(m.room); stats.joins++;
        send(ws, { t: 'ok', room: m.room });
      } else if (!c.rooms.has(m.room)) return;
      else if (m.t === 'leave') leaveRoom(c, m.room);
      else if (m.t === 'emit' && typeof m.topic === 'string' && TOPIC.test(m.topic)) {
        const out = JSON.stringify({ t: 'ev', room: m.room, topic: m.topic, from: c.id, data: m.data });
        for (const [id, p] of rooms.get(m.room)) if (id !== c.id && p.ws.readyState === 1) {
          if (m.topic === 's' && p.ws.bufferedAmount > 131072) continue;   // slow link: drop stale snapshots instead of queueing delay
          p.ws.send(out);
        }
      } else if (m.t === 'pres' && m.patch && typeof m.patch === 'object') {
        const me = rooms.get(m.room).get(c.id);
        for (const k of Object.keys(m.patch)) { if (k === '__proto__' || k === 'constructor' || k === 'prototype') continue; if (m.patch[k] === null) delete me.presence[k]; else me.presence[k] = m.patch[k]; }
        if (JSON.stringify(me.presence).length > 4096) me.presence = {};
        dirty.add(m.room);
      }
    });
    ws.on('close', () => { for (const n of [...c.rooms]) leaveRoom(c, n); const n = (perIp.get(ip) || 1) - 1; if (n <= 0) perIp.delete(ip); else perIp.set(ip, n); });
    ws.on('error', () => {});
  });
  const tick = setInterval(() => {
    for (const name of dirty) { const r = rooms.get(name); if (!r) continue; const out = JSON.stringify({ t: 'peers', room: name, list: [...r].map(([id, p]) => [id, p.presence]) }); for (const p of r.values()) if (p.ws.readyState === 1) p.ws.send(out); }
    dirty.clear();
  }, 33);
  const beat = setInterval(() => {   // drop dead connections and keep proxies from closing quiet ones
    for (const ws of wss.clients) { if (ws.isAlive === false) { ws.terminate(); continue; } ws.isAlive = false; try { ws.ping(); } catch {} }
    const mins = Math.floor(Date.now() / 60000); for (const k of creates.keys()) if (!k.endsWith(':' + mins)) creates.delete(k);
  }, 25000);
  return { server, wss, rooms, stats, close() { clearInterval(tick); clearInterval(beat); for (const ws of wss.clients) ws.terminate(); wss.close(); server.close(); } };
}
module.exports = { createRelay };
if (require.main === module) {
  const origins = String(process.env.ALLOWED_ORIGINS || '').split(',').map((s) => s.trim()).filter(Boolean);
  const r = createRelay({ origins, trustProxy: process.env.TRUST_PROXY === '1' || !!process.env.RENDER, maxRooms: +process.env.MAX_ROOMS || 400, maxPerIp: +process.env.MAX_PER_IP || 12 });
  const port = +process.env.PORT || 8790;
  r.server.listen(port, () => console.log('vs-relay listening on ' + port + (origins.length ? ' · origins: ' + origins.join(', ') : ' · any origin')));
}
