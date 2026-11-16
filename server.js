// 강의실 라이브 — 의존성 없는 Node.js 서버 (Node 18+)
// 실행: node server.js   (PORT, DATA_FILE 환경변수로 포트/저장 위치 변경 가능)
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data.json');
const INDEX = path.join(__dirname, 'public', 'index.html');

const LIMITS = { title: 60, post: 8000, question: 1000, name: 30, reply: 2000, items: 500, rooms: 2000 };

let rooms = {};
try { rooms = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); } catch { /* 첫 실행 */ }

let saveTimer = null;
function save() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    fs.writeFile(DATA_FILE + '.tmp', JSON.stringify(rooms), (err) => {
      if (!err) fs.rename(DATA_FILE + '.tmp', DATA_FILE, () => {});
    });
  }, 300);
}

const streams = new Map(); // 방 코드 -> 열려 있는 SSE 응답들
const id = () => crypto.randomBytes(6).toString('hex');
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
function newCode() {
  let c;
  do { c = Array.from(crypto.randomBytes(6), (b) => CODE_CHARS[b % CODE_CHARS.length]).join(''); } while (rooms[c]);
  return c;
}

function isHost(room, key) {
  if (typeof key !== 'string' || key.length !== room.hostKey.length) return false;
  return crypto.timingSafeEqual(Buffer.from(key), Buffer.from(room.hostKey));
}

function publicState(code) {
  const room = rooms[code];
  return {
    title: room.title,
    online: streams.get(code)?.size || 0,
    posts: room.posts,
    questions: room.questions.map((q) => ({
      id: q.id, name: q.name, text: q.text, at: q.at,
      votes: q.voters.length, answered: q.answered, reply: q.reply,
    })),
  };
}

function broadcast(code) {
  const set = streams.get(code);
  if (!set) return;
  const msg = `data: ${JSON.stringify(publicState(code))}\n\n`;
  for (const res of set) res.write(msg);
}

function changed(code) { save(); broadcast(code); }

// 간단한 속도 제한: key별 최소 간격(ms)
const lastHit = new Map();
function tooFast(key, gapMs) {
  const now = Date.now();
  if (now - (lastHit.get(key) || 0) < gapMs) return true;
  lastHit.set(key, now);
  if (lastHit.size > 50000) lastHit.clear();
  return false;
}

function send(res, status, body) {
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(body));
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0; const chunks = [];
    req.on('data', (c) => {
      size += c.length;
      if (size > 64 * 1024) { reject(new Error('too large')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => {
      try { resolve(chunks.length ? JSON.parse(Buffer.concat(chunks).toString('utf8')) : {}); }
      catch { reject(new Error('bad json')); }
    });
    req.on('error', reject);
  });
}

const text = (v, max) => (typeof v === 'string' ? v.trim().slice(0, max) : '');

async function api(req, res, parts, ip) {
  const method = req.method;

  // POST /api/rooms — 방 만들기
  if (parts.length === 1) {
    if (method !== 'POST') return send(res, 405, { error: '허용되지 않는 요청입니다.' });
    if (tooFast('room:' + ip, 5000)) return send(res, 429, { error: '잠시 후 다시 시도해 주세요.' });
    if (Object.keys(rooms).length >= LIMITS.rooms) return send(res, 503, { error: '방을 더 만들 수 없습니다.' });
    const body = await readBody(req);
    const title = text(body.title, LIMITS.title) || '강의실';
    const code = newCode();
    const hostKey = crypto.randomBytes(16).toString('hex');
    rooms[code] = { title, hostKey, createdAt: Date.now(), posts: [], questions: [] };
    save();
    return send(res, 201, { code, hostKey });
  }

  const code = (parts[1] || '').toUpperCase();
  const room = rooms[code];
  if (!room) return send(res, 404, { error: '방을 찾을 수 없습니다. 코드를 확인해 주세요.' });

  // GET /api/rooms/:code?hostKey= — 방 확인
  if (parts.length === 2 && method === 'GET') {
    const key = new URL(req.url, 'http://x').searchParams.get('hostKey');
    return send(res, 200, { title: room.title, isHost: isHost(room, key) });
  }

  // GET /api/rooms/:code/events — 실시간 스트림
  if (parts[2] === 'events' && method === 'GET') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    });
    res.write('retry: 3000\n\n');
    if (!streams.has(code)) streams.set(code, new Set());
    streams.get(code).add(res);
    broadcast(code);
    const beat = setInterval(() => res.write(': ping\n\n'), 25000);
    req.on('close', () => {
      clearInterval(beat);
      streams.get(code)?.delete(res);
      broadcast(code);
    });
    return;
  }

  const body = method === 'GET' ? {} : await readBody(req);
  const host = isHost(room, body.hostKey);
  const needHost = () => send(res, 403, { error: '강사만 할 수 있는 작업입니다.' });

  if (parts[2] === 'posts') {
    if (!host) return needHost();
    if (parts.length === 3 && method === 'POST') {
      const t = text(body.text, LIMITS.post);
      if (!t) return send(res, 400, { error: '내용을 입력해 주세요.' });
      if (room.posts.length >= LIMITS.items) room.posts.shift();
      room.posts.push({ id: id(), text: t, at: Date.now(), pinned: false });
      changed(code);
      return send(res, 201, { ok: true });
    }
    const post = room.posts.find((p) => p.id === parts[3]);
    if (!post) return send(res, 404, { error: '이미 삭제된 자료입니다.' });
    if (parts[4] === 'pin' && method === 'POST') { post.pinned = !post.pinned; changed(code); return send(res, 200, { ok: true }); }
    if (parts.length === 4 && method === 'DELETE') {
      room.posts = room.posts.filter((p) => p !== post);
      changed(code);
      return send(res, 200, { ok: true });
    }
  }

  if (parts[2] === 'questions') {
    const clientId = text(body.clientId, 40);
    if (parts.length === 3 && method === 'POST') {
      const t = text(body.text, LIMITS.question);
      if (!t) return send(res, 400, { error: '질문을 입력해 주세요.' });
      if (tooFast('q:' + ip + clientId, 3000)) return send(res, 429, { error: '질문은 3초에 한 번 보낼 수 있습니다.' });
      if (room.questions.length >= LIMITS.items) return send(res, 409, { error: '질문이 가득 찼습니다. 강사에게 정리를 요청해 주세요.' });
      room.questions.push({
        id: id(), name: text(body.name, LIMITS.name) || '익명', text: t, at: Date.now(),
        voters: [], answered: false, reply: '',
      });
      changed(code);
      return send(res, 201, { ok: true });
    }
    const q = room.questions.find((x) => x.id === parts[3]);
    if (!q) return send(res, 404, { error: '이미 삭제된 질문입니다.' });
    if (parts[4] === 'vote' && method === 'POST') {
      if (!clientId) return send(res, 400, { error: '잘못된 요청입니다.' });
      const i = q.voters.indexOf(clientId);
      if (i >= 0) q.voters.splice(i, 1); else q.voters.push(clientId);
      changed(code);
      return send(res, 200, { voted: i < 0 });
    }
    if (parts[4] === 'answer' && method === 'POST') {
      if (!host) return needHost();
      q.answered = body.answered !== false;
      if (typeof body.reply === 'string') q.reply = text(body.reply, LIMITS.reply);
      changed(code);
      return send(res, 200, { ok: true });
    }
    if (parts.length === 4 && method === 'DELETE') {
      if (!host) return needHost();
      room.questions = room.questions.filter((x) => x !== q);
      changed(code);
      return send(res, 200, { ok: true });
    }
  }

  return send(res, 404, { error: '없는 주소입니다.' });
}

http.createServer((req, res) => {
  const pathname = new URL(req.url, 'http://x').pathname;
  const ip = (req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket.remoteAddress || '';
  const parts = pathname.split('/').filter(Boolean);

  if (parts[0] === 'api' && parts[1] === 'rooms') {
    api(req, res, parts.slice(1), ip).catch(() => {
      if (!res.headersSent) send(res, 400, { error: '요청을 처리하지 못했습니다.' });
    });
    return;
  }
  if (req.method === 'GET' && (pathname === '/' || parts[0] === 'r')) {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    fs.createReadStream(INDEX).pipe(res);
    return;
  }
  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Not found');
}).listen(PORT, () => console.log(`강의실 라이브: http://localhost:${PORT}`));
