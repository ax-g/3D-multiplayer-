// ============================================================================
// Authoritative multiplayer server for the Three.js game.
//
// Responsibilities (mirrors client's index.html protocol comment block):
//   - Room creation/join by 4-character code, 2-8 players.
//   - Relay position/rotation state at a fixed tick rate.
//   - Validate and resolve combat: computes damage itself from its own
//     weapon table + reported body part, checks attacker/target distance
//     and per-weapon cooldown. NEVER trusts a client-sent damage number
//     (the client protocol doesn't even have a field for one).
//   - Track health/death/respawn per room.
//   - Arbitrate pickup collection races; broadcast weapon drops.
//
// Run:  npm install && npm start        (defaults to PORT env var or 8080)
// ============================================================================

const { WebSocketServer } = require('ws');
const http = require('http');

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = 8;
const TICK_HZ = 15;
const STALE_CONNECTION_MS = 20000; // kill sockets that go silent this long
const RESPAWN_DELAY_MS = 3000;
const ATTACK_RANGE_LENIENCY = 1.6; // meters of slack for latency/interpolation

// Mirrors WEAPON_DATABASE in index.html. Keep these two in sync.
const WEAPON_BASE = { damage: 20, headMultiplier: 2.0, limbMultiplier: 0.7, attackRate: 500, range: 2.2 };
const WEAPON_DATABASE = {
  sword: { ...WEAPON_BASE, damage: 28, attackRate: 450, range: 2.2 },
  axe: { ...WEAPON_BASE, damage: 40, attackRate: 700, range: 2.0 },
  spear: { ...WEAPON_BASE, damage: 22, attackRate: 380, range: 2.8 },
  dagger: { ...WEAPON_BASE, damage: 16, attackRate: 260, range: 1.6 },
  hammer: { ...WEAPON_BASE, damage: 50, attackRate: 900, range: 2.0 },
};
const UNARMED = { damage: 8, headMultiplier: 2, limbMultiplier: 0.7, attackRate: 550, range: 1.6 };

const SPAWN_POINTS = [
  [0, 2, 6], [10, 2, -10], [-10, 2, 10], [15, 2, 5],
  [-15, 2, -5], [0, 2, -20], [-6, 2, -12], [8, 2, 12],
];

/** @type {Map<string, Room>} */
const rooms = new Map();

function makeRoomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // no 0/O/1/I
  let code;
  do {
    code = Array.from({ length: 4 }, () => chars[Math.floor(Math.random() * chars.length)]).join('');
  } while (rooms.has(code));
  return code;
}

function randomSpawn() {
  return SPAWN_POINTS[Math.floor(Math.random() * SPAWN_POINTS.length)];
}

class Room {
  constructor(code) {
    this.code = code;
    this.players = new Map(); // id -> PlayerState
    this.collected = new Set(); // pickup ids already granted to someone
  }

  broadcast(msg, exceptId) {
    const data = JSON.stringify(msg);
    for (const p of this.players.values()) {
      if (p.id === exceptId) continue;
      if (p.ws.readyState === p.ws.OPEN) p.ws.send(data);
    }
  }

  send(id, msg) {
    const p = this.players.get(id);
    if (p && p.ws.readyState === p.ws.OPEN) p.ws.send(JSON.stringify(msg));
  }

  publicState(p) {
    return {
      id: p.id, name: p.name, x: p.x, y: p.y, z: p.z, yaw: p.yaw,
      health: p.health, maxHealth: p.maxHealth, alive: p.alive, weaponId: p.weaponId,
    };
  }
}

let idCounter = 1;
function nextId() { return 'p' + (idCounter++) + '_' + Math.random().toString(36).slice(2, 6); }

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Multiplayer game server OK. Rooms active: ' + rooms.size);
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  const conn = { id: null, room: null, lastSeen: Date.now() };
  ws.isAlive = true;

  ws.on('message', (raw) => {
    conn.lastSeen = Date.now();
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    handleMessage(ws, conn, msg);
  });

  ws.on('close', () => onDisconnect(conn));
  ws.on('error', () => {});
});

// Kill sockets that stopped sending anything (covers mobile backgrounding /
// dead connections that never fire a clean 'close').
setInterval(() => {
  const now = Date.now();
  for (const client of wss.clients) {
    if (client._conn && now - client._conn.lastSeen > STALE_CONNECTION_MS) {
      client.terminate();
    }
  }
}, 5000);

function handleMessage(ws, conn, msg) {
  if (!ws._conn) ws._conn = conn;

  switch (msg.t) {
    case 'hello': return onHello(ws, conn, msg);
    case 'state': return onState(conn, msg);
    case 'attack': return onAttack(conn, msg);
    case 'pickup': return onPickup(conn, msg);
    case 'drop': return onDrop(conn, msg);
    case 'ping': return ws.readyState === ws.OPEN && ws.send(JSON.stringify({ t: 'pong', time: msg.time }));
  }
}

function onHello(ws, conn, msg) {
  if (conn.id) return; // already joined

  let room;
  const requested = (msg.room || '').toUpperCase().trim();
  if (requested) {
    room = rooms.get(requested);
    if (!room) return ws.send(JSON.stringify({ t: 'room-error', message: `Room ${requested} not found.` }));
    if (room.players.size >= MAX_PLAYERS) return ws.send(JSON.stringify({ t: 'room-error', message: 'Room is full (8/8).' }));
  } else {
    room = new Room(makeRoomCode());
    rooms.set(room.code, room);
  }

  const id = nextId();
  const [sx, sy, sz] = randomSpawn();
  const player = {
    id, ws, room, name: String(msg.name || 'Player').slice(0, 16).trim() || 'Player',
    x: sx, y: sy, z: sz, yaw: 0,
    health: 100, maxHealth: 100, alive: true, weaponId: null,
    lastAttack: new Map(), // weaponKey -> timestamp
    lastStateTime: Date.now(),
  };
  room.players.set(id, player);
  conn.id = id;
  conn.room = room;

  ws.send(JSON.stringify({
    t: 'welcome',
    id,
    room: room.code,
    players: [...room.players.values()].filter((p) => p.id !== id).map(room.publicState.bind(room)),
    collected: [...room.collected],
  }));

  room.broadcast({ t: 'player-joined', player: room.publicState(player) }, id);
}

function onState(conn, msg) {
  const player = getPlayer(conn);
  if (!player || !player.alive) return;

  // Light anti-teleport check: clamp how far a single update can move the
  // player based on elapsed time and the fastest legitimate speed (dash).
  const now = Date.now();
  const dt = Math.max(0.001, (now - player.lastStateTime) / 1000);
  player.lastStateTime = now;
  const maxDist = 11 * dt + 2; // dashSpeed * dt, plus latency slack
  const dx = msg.x - player.x, dz = msg.z - player.z;
  const dist = Math.hypot(dx, dz);
  if (Number.isFinite(msg.x) && Number.isFinite(msg.y) && Number.isFinite(msg.z) && Number.isFinite(msg.yaw)) {
    if (dist <= maxDist) {
      player.x = msg.x; player.y = msg.y; player.z = msg.z;
    } else {
      // Suspicious jump — accept direction but cap the distance instead of
      // outright teleporting the player back (keeps motion feeling smooth
      // under normal packet loss/jitter while still bounding cheating).
      const scale = maxDist / dist;
      player.x += dx * scale; player.z += dz * scale;
    }
    player.yaw = msg.yaw;
  }
  if (typeof msg.weaponId !== 'undefined') player.weaponId = msg.weaponId;
}

function weaponTable(weaponId) {
  return weaponId && WEAPON_DATABASE[weaponId] ? WEAPON_DATABASE[weaponId] : UNARMED;
}

function onAttack(conn, msg) {
  const attacker = getPlayer(conn);
  if (!attacker || !attacker.alive) return;
  const room = conn.room;
  const target = room.players.get(msg.targetId);
  if (!target || !target.alive || target.id === attacker.id) return;

  const weapon = weaponTable(msg.weaponId);
  const weaponKey = msg.weaponId || 'unarmed';
  const now = Date.now();
  const last = attacker.lastAttack.get(weaponKey) || 0;
  if (now - last < weapon.attackRate * 0.9) return; // cooldown not respected — drop it
  attacker.lastAttack.set(weaponKey, now);

  const dist = Math.hypot(attacker.x - target.x, attacker.y - target.y, attacker.z - target.z);
  if (dist > weapon.range + ATTACK_RANGE_LENIENCY) return; // out of range — ignore claimed hit

  const bodyPart = msg.bodyPart === 'head' ? 'head' : msg.bodyPart === 'limb' ? 'limb' : 'body';
  const multiplier = bodyPart === 'head' ? weapon.headMultiplier : bodyPart === 'limb' ? weapon.limbMultiplier : 1;
  const damage = Math.round(weapon.damage * multiplier);

  target.health = Math.max(0, target.health - damage);
  room.broadcast({
    t: 'attack-result', attackerId: attacker.id, targetId: target.id,
    damage, targetHealth: target.health, headshot: bodyPart === 'head',
  });

  if (target.health <= 0) {
    target.alive = false;
    room.broadcast({ t: 'death', id: target.id, killerId: attacker.id, respawnDelay: RESPAWN_DELAY_MS });
    setTimeout(() => {
      if (!room.players.has(target.id)) return; // left before respawn
      const [rx, ry, rz] = randomSpawn();
      target.x = rx; target.y = ry; target.z = rz;
      target.health = target.maxHealth; target.alive = true;
      room.broadcast({ t: 'respawn', id: target.id, x: rx, y: ry, z: rz });
    }, RESPAWN_DELAY_MS);
  }
}

function onPickup(conn, msg) {
  const player = getPlayer(conn);
  if (!player) return;
  const room = conn.room;
  const pickupId = msg.pickupId;
  if (!pickupId || room.collected.has(pickupId)) {
    return room.send(player.id, { t: 'pickup-denied', pickupId });
  }
  room.collected.add(pickupId);
  room.broadcast({ t: 'pickup-collected', pickupId, by: player.id });
}

function onDrop(conn, msg) {
  const player = getPlayer(conn);
  if (!player) return;
  const room = conn.room;
  // The dropper already spawned this locally; broadcast it to everyone else.
  room.broadcast({
    t: 'weapon-dropped',
    pickup: { id: msg.id, weaponId: msg.weaponId, label: msg.label, position: msg.position },
  }, player.id);
}

function getPlayer(conn) {
  if (!conn.id || !conn.room) return null;
  return conn.room.players.get(conn.id) || null;
}

function onDisconnect(conn) {
  if (!conn.id || !conn.room) return;
  const room = conn.room;
  room.players.delete(conn.id);
  room.broadcast({ t: 'player-left', id: conn.id });
  if (room.players.size === 0) rooms.delete(room.code);
}

// Periodic state broadcast, independent of how often clients send updates.
setInterval(() => {
  for (const room of rooms.values()) {
    if (room.players.size === 0) continue;
    const players = [...room.players.values()].map(room.publicState.bind(room));
    room.broadcast({ t: 'state-batch', players });
  }
}, 1000 / TICK_HZ);

server.listen(PORT, () => console.log(`Multiplayer server listening on :${PORT}`));