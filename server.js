// Servidor do Stick Duelo: salas multiplayer (WebSocket) + ranking (HTTP)
// Rodar:  npm install && npm start
const http = require('http');
const fs = require('fs');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_PEERS = 6;          // máximo de conexões por sala
const MAX_MSG = 8 * 1024;     // tamanho máximo de uma mensagem (bytes)
const MAX_MSGS_PER_SEC = 60;  // limite anti-spam por jogador
const RANK_FILE = process.env.RANK_FILE || './rank.json';

// ======================= 1) RANKING (HTTP) =======================
// players[id] = { id, tk, nk, coins, kills }
// "id" é público (aparece no ranking); "tk" é um segredo que prova que o dono é você.
let players = {};
try { players = JSON.parse(fs.readFileSync(RANK_FILE, 'utf8')); } catch (e) {}

let saveTimer = null;
function saveSoon() {                       // grava no disco no máximo 1x a cada 5s
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    fs.writeFile(RANK_FILE, JSON.stringify(players), () => {});
  }, 5000);
}

const int = (v, max) => Math.max(0, Math.min(max, Math.floor(Number(v)) || 0));
const str = (v, max) => (typeof v === 'string' ? v.trim().slice(0, max) : '');

function top(by, myId) {
  const sorted = Object.values(players).sort((a, b) => b[by] - a[by]);
  const pub = (p, pos) => ({ id: p.id, nk: p.nk, coins: p.coins, kills: p.kills, pos });
  const list = sorted.slice(0, 100).map((p, i) => pub(p, i + 1));
  const idx = sorted.findIndex(p => p.id === myId);
  return { list, me: idx >= 0 ? pub(sorted[idx], idx + 1) : null };
}

function postScore(b) {
  const id = str(b.id, 64), tk = str(b.tk, 64), nk = str(b.nk, 20);
  if (!id || !tk || !nk) return [400, { error: 'dados inválidos' }];
  const cur = players[id];
  if (cur && cur.tk !== tk) return [403, { error: 'token incorreto' }];
  // apelido único (ignora maiúsculas/minúsculas)
  const taken = Object.values(players).some(p => p.id !== id && p.nk.toLowerCase() === nk.toLowerCase());
  if (taken) return [409, { error: 'apelido em uso' }];
  players[id] = { id, tk, nk, coins: int(b.coins, 1e9), kills: int(b.kills, 1e9) };
  saveSoon();
  return [200, { ok: true }];
}

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};
function send(res, code, obj) {
  res.writeHead(code, { ...CORS, 'Content-Type': 'application/json' });
  res.end(JSON.stringify(obj));
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  if (req.method === 'OPTIONS') { res.writeHead(204, CORS); return res.end(); }

  if (req.method === 'GET' && url.pathname === '/') return send(res, 200, { ok: true, rooms: rooms.size });

  if (req.method === 'GET' && url.pathname === '/rank/top') {
    const by = url.searchParams.get('by') === 'kills' ? 'kills' : 'coins';
    return send(res, 200, top(by, url.searchParams.get('id') || ''));
  }

  if (req.method === 'POST' && url.pathname === '/rank/score') {
    let body = '';
    req.on('data', c => { body += c; if (body.length > 2048) req.destroy(); });
    req.on('end', () => {
      let b; try { b = JSON.parse(body); } catch (e) { return send(res, 400, { error: 'json inválido' }); }
      const [code, out] = postScore(b || {});
      send(res, code, out);
    });
    return;
  }
  send(res, 404, { error: 'não encontrado' });
});

// ======================= 2) SALAS (WebSocket) =======================
// O servidor NÃO entende o jogo. Ele só guarda o "presence" (estado) de cada
// jogador e repassa as mudanças para os outros da mesma sala.
//
// Cliente -> servidor:  {t:'j', r:'sd-ABCD'}   entrar na sala
//                       {t:'p', d:{...}}       atualizar meu estado (null apaga a chave)
// Servidor -> cliente:  {t:'w', id}            bem-vindo, este é o seu id
//                       {t:'s', id, p}         estado atual de quem já estava na sala
//                       {t:'u', id, d}         alguém atualizou o estado
//                       {t:'l', id}            alguém saiu
//                       {t:'x'}                entrada recusada (sala cheia/nome inválido)
const rooms = new Map();   // nome da sala -> Map(id -> { ws, presence })

const wss = new WebSocketServer({ server, maxPayload: MAX_MSG });
const tx = (ws, obj) => { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); };

function merge(target, delta) {
  for (const k in delta) {
    if (delta[k] === null) delete target[k]; else target[k] = delta[k];
  }
}

wss.on('connection', ws => {
  let room = null, id = null;
  let count = 0;
  const rl = setInterval(() => { count = 0; }, 1000);
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', raw => {
    if (++count > MAX_MSGS_PER_SEC) return;       // ignora spam
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;

    // --- entrar numa sala ---
    if (m.t === 'j' && !room) {
      const name = str(m.r, 40);
      if (!/^[\w-]{1,40}$/.test(name)) return tx(ws, { t: 'x' });
      let peers = rooms.get(name);
      if (!peers) rooms.set(name, peers = new Map());
      if (peers.size >= MAX_PEERS) return tx(ws, { t: 'x' });

      id = crypto.randomBytes(4).toString('hex');
      room = name;
      tx(ws, { t: 'w', id });
      peers.forEach((p, pid) => tx(ws, { t: 's', id: pid, p: p.presence }));
      peers.set(id, { ws, presence: {} });
      return;
    }

    // --- atualizar meu estado e avisar os outros ---
    if (m.t === 'p' && room && m.d && typeof m.d === 'object') {
      const peers = rooms.get(room);
      const me = peers && peers.get(id);
      if (!me) return;
      merge(me.presence, m.d);
      peers.forEach((p, pid) => { if (pid !== id) tx(p.ws, { t: 'u', id, d: m.d }); });
    }
  });

  ws.on('close', () => {
    clearInterval(rl);
    if (!room) return;
    const peers = rooms.get(room);
    if (!peers) return;
    peers.delete(id);
    peers.forEach(p => tx(p.ws, { t: 'l', id }));
    if (peers.size === 0) rooms.delete(room);     // sala vazia some
  });
  ws.on('error', () => {});
});

// derruba conexões "mortas" (celular que perdeu sinal sem fechar a conexão)
setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 20000);

server.listen(PORT, () => console.log('Stick Duelo server na porta ' + PORT));
