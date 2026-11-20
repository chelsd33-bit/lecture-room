// 강의실 라이브 — 의존성 없는 Node.js 서버 (Node 18+)
// 실행: node server.js   (PORT, DATA_FILE 환경변수로 포트/저장 위치 변경 가능)
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data.json');
const INDEX = path.join(__dirname, 'public', 'index.html');

const LIMITS = { title: 60, post: 8000, question: 1000, name: 30, reply: 2000, note: 200, items: 500, rooms: 2000, responders: 1000, history: 50, timerSec: 3 * 60 * 60 };

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

const streams = new Map(); // 방 코드 -> 열려 있는 SSE 연결들 { res, host, cid }
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
    now: Date.now(),
    title: room.title,
    online: streams.get(code)?.size || 0,
    posts: room.posts,
    questions: room.questions.map((q) => ({
      id: q.id, name: q.name, text: q.text, at: q.at,
      votes: q.voters.length, answered: q.answered, reply: q.reply,
    })),
    timer: room.timer || null,
    checkHistory: room.checkHistory || [],
  };
}

// 실습 체크 집계. 참여 인원 = 지금 접속한 수강생 + 이미 응답한 사람
function checkSummary(code) {
  const room = rooms[code];
  const c = room.check;
  if (!c) return null;
  const entries = Object.values(c.entries);
  const cids = new Set(Object.keys(c.entries));
  for (const s of streams.get(code) || []) if (!s.host && s.cid) cids.add(s.cid);
  return {
    id: c.id, title: c.title, startedAt: c.startedAt,
    done: entries.filter((e) => e.status === 'done').length,
    stuck: entries.filter((e) => e.status === 'stuck').length,
    participants: cids.size,
  };
}

function broadcast(code) {
  const set = streams.get(code);
  if (!set) return;
  const room = rooms[code];
  const base = publicState(code);
  const sum = checkSummary(code);
  const cache = new Map(); // 같은 내용을 받는 연결끼리 문자열을 재사용
  for (const s of set) {
    const mine = sum && !s.host ? room.check.entries[s.cid]?.status || null : null;
    const key = s.host ? 'host' : 'mine:' + mine;
    if (!cache.has(key)) {
      let check = null;
      if (sum) {
        check = { ...sum, mine };
        // 누가 막혔는지(이름·메모)는 강사에게만 보낸다
        if (s.host) check.list = Object.values(room.check.entries).sort((a, b) => a.at - b.at);
      }
      cache.set(key, `data: ${JSON.stringify({ ...base, check })}\n\n`);
    }
    s.res.write(cache.get(key));
  }
}

function archiveCheck(room, code) {
  const sum = checkSummary(code);
  if (!sum) return;
  room.checkHistory = room.checkHistory || [];
  room.checkHistory.push({ title: sum.title, startedAt: sum.startedAt, endedAt: Date.now(), done: sum.done, stuck: sum.stuck, participants: sum.participants });
  if (room.checkHistory.length > LIMITS.history) room.checkHistory.shift();
  room.check = null;
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
    const q = new URL(req.url, 'http://x').searchParams;
    const conn = { res, host: isHost(room, q.get('hostKey')), cid: text(q.get('cid'), 40) };
    if (!streams.has(code)) streams.set(code, new Set());
    streams.get(code).add(conn);
    broadcast(code);
    const beat = setInterval(() => res.write(': ping\n\n'), 25000);
    req.on('close', () => {
      clearInterval(beat);
      streams.get(code)?.delete(conn);
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

  // 실습 체크
  if (parts[2] === 'check') {
    if (parts.length === 3 && method === 'POST') {
      if (!host) return needHost();
      const title = text(body.title, LIMITS.title) || '실습';
      archiveCheck(room, code);
      room.check = { id: id(), title, startedAt: Date.now(), entries: {} };
      changed(code);
      return send(res, 201, { ok: true });
    }
    if (parts.length === 3 && method === 'DELETE') {
      if (!host) return needHost();
      archiveCheck(room, code);
      changed(code);
      return send(res, 200, { ok: true });
    }
    if (parts[3] === 'respond' && method === 'POST') {
      const clientId = text(body.clientId, 40);
      if (!clientId) return send(res, 400, { error: '잘못된 요청입니다.' });
      if (!room.check || room.check.id !== body.checkId) return send(res, 409, { error: '이미 끝난 실습입니다.' });
      if (tooFast('c:' + clientId, 700)) return send(res, 429, { error: '잠시 후 다시 눌러 주세요.' });
      const entries = room.check.entries;
      if (body.status === 'none') delete entries[clientId];
      else if (body.status === 'done' || body.status === 'stuck') {
        if (!entries[clientId] && Object.keys(entries).length >= LIMITS.responders) return send(res, 409, { error: '참여 인원이 가득 찼습니다.' });
        entries[clientId] = {
          status: body.status,
          name: text(body.name, LIMITS.name) || '익명',
          note: body.status === 'stuck' ? text(body.note, LIMITS.note) : '',
          at: Date.now(),
        };
      } else return send(res, 400, { error: '잘못된 요청입니다.' });
      changed(code);
      return send(res, 200, { ok: true });
    }
  }

  // 실습 타이머
  if (parts[2] === 'timer' && method === 'POST') {
    if (!host) return needHost();
    const sec = Math.round(Number(body.seconds));
    if (!Number.isFinite(sec) || sec < 0 || sec > LIMITS.timerSec) return send(res, 400, { error: '시간을 다시 확인해 주세요.' });
    const now = Date.now();
    if (parts[3] === 'add') {
      if (!room.timer) return send(res, 409, { error: '진행 중인 타이머가 없습니다.' });
      const from = Math.max(room.timer.endsAt, now);
      room.timer = { endsAt: Math.min(from + sec * 1000, now + LIMITS.timerSec * 1000), total: room.timer.total + sec };
    } else {
      room.timer = sec ? { endsAt: now + sec * 1000, total: sec } : null;
    }
    changed(code);
    return send(res, 200, { ok: true });
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
  if (req.method === 'GET' && pathname === '/qrcode.js') {
    res.writeHead(200, { 'Content-Type': 'text/javascript; charset=utf-8', 'Cache-Control': 'public, max-age=86400' });
    fs.createReadStream(path.join(__dirname, 'public', 'qrcode.js')).pipe(res);
    return;
  }
  if (req.method === 'GET' && pathname === '/healthz') { res.writeHead(200); res.end('ok'); return; }
  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Not found');
}).listen(PORT, () => console.log(`강의실 라이브: http://localhost:${PORT}`));
