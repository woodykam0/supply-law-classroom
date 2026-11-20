/*
 * 市場均衡與供需調整｜課堂互動 — 自有後端
 * 零依賴 Node.js 伺服器（僅用內建模組）
 * 功能：靜態檔案託管 + 事件儲存（JSON 檔案持久化）+ Server-Sent Events 即時推送
 * 環境變數：
 *   PORT              監聽埠（Render 等平台會自動注入，本機預設 3000）
 *   TEACHER_PASSWORD  教師密碼（預設 @W423021k）
 *   DATA_FILE         資料檔案路徑（預設 ./data/events.json）
 */
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = Number(process.env.PORT) || 3000;
// 密碼一律從環境變數讀取，不寫進版本庫。未設定時教師驗證一律失敗（安全鎖定）。
const TEACHER_PASSWORD = process.env.TEACHER_PASSWORD || '';
function teacherOk(pw) { return !!TEACHER_PASSWORD && pw === TEACHER_PASSWORD; }
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data', 'events.json');
const PUBLIC_DIR = __dirname; // 扁平佈局：index.html 與 server.js 同目錄，便於網頁拖放上傳
const MAX_BODY = 64 * 1024;          // 單個請求最大 64KB（遠超課堂資料所需）
const MAX_ITEMS = 20000;             // 每實例保留的事件上限（防止磁碟無限增長）
const RATE_LIMIT = 120;              // 每個 IP 每分鐘最多 120 次寫入
const PROTECTED = new Set(['room', 'activity_reset', 'matrix_reset', 'trades']); // 需教師密碼的事件

// ---------- 資料載入與持久化 ----------
let items = [];
try {
  const raw = fs.readFileSync(DATA_FILE, 'utf8');
  const parsed = JSON.parse(raw);
  if (Array.isArray(parsed)) items = parsed;
  console.log(`[data] 已從 ${DATA_FILE} 載入 ${items.length} 條事件`);
} catch (e) {
  if (e.code !== 'ENOENT') console.warn('[data] 無法讀取既有資料，將重新開始：', e.message);
  items = [];
}
let version = items.length;
let writeChain = Promise.resolve();

function persist() {
  // 串行寫入，避免並發覆蓋；原子替換避免半寫檔案
  writeChain = writeChain.then(() => new Promise((resolve) => {
    try {
      fs.mkdirSync(path.dirname(DATA_FILE), { recursive: true });
      const tmp = DATA_FILE + '.tmp';
      fs.writeFileSync(tmp, JSON.stringify(items));
      fs.renameSync(tmp, DATA_FILE);
    } catch (err) {
      console.error('[data] 寫入失敗：', err.message);
    }
    resolve();
  }));
  return writeChain;
}

// ---------- SSE 客戶端管理 ----------
const sseClients = new Set(); // { res, instanceId }
function broadcast(instanceId) {
  const payload = `data: ${JSON.stringify({ version })}\n\n`;
  for (const client of [...sseClients]) {
    if (client.instanceId !== instanceId) continue;
    try { client.res.write(payload); } catch { sseClients.delete(client); }
  }
}
setInterval(() => { // 心跳，保持代理/瀏覽器連線
  for (const client of [...sseClients]) {
    try { client.res.write(': hb\n\n'); } catch { sseClients.delete(client); }
  }
}, 25000).unref();

// ---------- 速率限制 ----------
const rateBuckets = new Map(); // ip -> { count, resetAt }
function rateAllowed(ip) {
  const now = Date.now();
  let bucket = rateBuckets.get(ip);
  if (!bucket || now > bucket.resetAt) { bucket = { count: 0, resetAt: now + 60000 }; rateBuckets.set(ip, bucket); }
  bucket.count += 1;
  if (rateBuckets.size > 5000) { for (const [k, v] of rateBuckets) if (now > v.resetAt) rateBuckets.delete(k); }
  return bucket.count <= RATE_LIMIT;
}

function readBody(req) {
  return new Promise((resolve) => {
    let size = 0; const chunks = [];
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > MAX_BODY) { resolve(null); req.destroy(); return; }
      chunks.push(chunk);
    });
    req.on('end', () => {
      try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8'))); }
      catch { resolve(null); }
    });
    req.on('error', () => resolve(null));
  });
}

function sendJson(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
  res.end(body);
}

const MIME = { '.html': 'text/html; charset=utf-8' };
function serveStatic(pathname, res) {
  let rel = decodeURIComponent(pathname);
  if (rel === '/' || rel === '\\') rel = '/index.html';
  if (rel !== '/index.html') { res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' }); res.end('找不到頁面。'); return; } // 只公開 index.html
  const filePath = path.join(__dirname, 'index.html');
  fs.readFile(filePath, (err, buf) => {
    if (err) { res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' }); res.end('找不到頁面。'); return; }
    res.writeHead(200, { 'content-type': MIME[path.extname(filePath).toLowerCase()] || 'application/octet-stream', 'cache-control': 'no-store' });
    res.end(buf);
  });
}

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://localhost');
    const pathname = url.pathname;
    const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown').split(',')[0].trim();

    if (pathname === '/api/health' && req.method === 'GET') {
      return sendJson(res, 200, { ok: true, version, clients: sseClients.size });
    }

    if (pathname === '/api/teacher/verify' && req.method === 'POST') {
      const body = await readBody(req);
      const ok = teacherOk(body && body.password);
      return sendJson(res, 200, { ok });
    }

    if (pathname === '/api/events' && req.method === 'GET') {
      const instanceId = String(url.searchParams.get('instanceId') || 'default').slice(0, 64);
      const filtered = items.filter((item) => (item.instanceId || 'default') === instanceId);
      return sendJson(res, 200, { version, items: filtered });
    }

    if (pathname === '/api/events' && req.method === 'POST') {
      if (!rateAllowed(ip)) return sendJson(res, 429, { error: '提交太頻繁，請稍候再試。' });
      const body = await readBody(req);
      if (!body || typeof body !== 'object') return sendJson(res, 400, { error: '請求格式錯誤。' });
      const instanceId = String(body.instanceId || 'default').slice(0, 64);
      const data = body.data;
      if (!data || typeof data !== 'object' || Array.isArray(data) || typeof data.eventType !== 'string' || !data.eventType) {
        return sendJson(res, 400, { error: '資料格式錯誤。' });
      }
      if (PROTECTED.has(data.eventType) && !teacherOk(String(req.headers['x-teacher-key'] || ''))) {
        return sendJson(res, 403, { error: '教師驗證失效，請重新登入控制台。' });
      }
      const item = { id: crypto.randomUUID(), createdAt: Date.now(), instanceId, data };
      items.push(item);
      if (items.length > MAX_ITEMS) items = items.slice(items.length - MAX_ITEMS);
      await persist();
      version += 1;
      broadcast(instanceId);
      return sendJson(res, 200, { ok: true, version });
    }

    if (pathname === '/api/stream' && req.method === 'GET') {
      const instanceId = String(url.searchParams.get('instanceId') || 'default').slice(0, 64);
      res.writeHead(200, {
        'content-type': 'text/event-stream; charset=utf-8',
        'cache-control': 'no-store',
        connection: 'keep-alive',
        'x-accel-buffering': 'no',
      });
      res.write(': connected\n\n');
      const client = { res, instanceId };
      sseClients.add(client);
      req.on('close', () => sseClients.delete(client));
      return;
    }

    if (req.method === 'GET' || req.method === 'HEAD') return serveStatic(pathname, res);
    sendJson(res, 405, { error: 'Method Not Allowed' });
  } catch (err) {
    console.error('[server] 未處理錯誤：', err);
    if (!res.headersSent) sendJson(res, 500, { error: '伺服器內部錯誤，請重試。' });
  }
});

server.listen(PORT, () => {
  console.log(`市場均衡課堂互動已啟動：http://localhost:${PORT}`);
  console.log(`資料檔案：${DATA_FILE}`);
  if (!TEACHER_PASSWORD) console.warn('⚠️  未設定 TEACHER_PASSWORD，教師控制功能已鎖定。請在 Render → Environment 設定後重新部署。');
});
