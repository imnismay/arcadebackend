const express = require("express");
const http = require("http");
const crypto = require("crypto");
const cors = require("cors");
const { Server } = require("socket.io");

const GAME_META = {
  "ludo":           { min: 2, max: 4 },
  "ashta-chamma":   { min: 2, max: 4 },
  "snakes-ladders": { min: 2, max: 4 },
  "chess":          { min: 2, max: 2 },
  "mancala":        { min: 2, max: 2 },
  "box-battle":     { min: 2, max: 2 },
  "connect-four":   { min: 2, max: 2 },
  "tic-tac-toe":    { min: 2, max: 2 },
  "memory-match":   { min: 1, max: 8, solo: true },
  "2048":           { min: 1, max: 8, solo: true },
};
const MAX_PLAYERS = 8;
const GRACE_MS = 90_000;
const HOST_GRACE_MS = 15_000;

// Comma-separated list, e.g. "https://nismay-arcade.pages.dev,https://arcade.yourdomain.com"
// Leave unset locally => allows everything (dev only).
const ORIGINS = (process.env.ALLOWED_ORIGINS || "")
  .split(",").map((s) => s.trim().replace(/\/$/, "")).filter(Boolean);
const corsOrigin = ORIGINS.length ? ORIGINS : "*";

const rooms = new Map();

const app = express();
app.use(cors({ origin: corsOrigin }));
app.get("/", (_req, res) => res.send("NISMAY Arcade server is running"));
app.get("/health", (_req, res) => res.json({ ok: true, rooms: rooms.size }));

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: corsOrigin, methods: ["GET", "POST"] },
  maxHttpBufferSize: 1e6,
  pingInterval: 10000,
  pingTimeout: 15000,
});

/* ───────── helpers ───────── */
const clean = (v, n) => String(v ?? "").replace(/[\u0000-\u001f<>]/g, "").trim().slice(0, n);
const ack = (cb, payload) => typeof cb === "function" && cb(payload);

function hashPw(pw) {
  const salt = crypto.randomBytes(16);
  return { salt, hash: crypto.scryptSync(String(pw), salt, 32) };
}
function checkPw(room, pw) {
  const h = crypto.scryptSync(String(pw ?? ""), room.pw.salt, 32);
  return crypto.timingSafeEqual(h, room.pw.hash);
}

const publicRoom = (room) => ({
  code: room.code,
  hostId: room.hostId,
  selected: room.selected,
  catalog: GAME_META,
  players: room.players.map((p) => ({ id: p.id, name: p.name, connected: p.connected })),
  game: room.game
    ? { id: room.game.id, key: room.game.key, solo: room.game.solo, seats: room.game.seats }
    : null,
});
const broadcast = (room) => io.to(room.code).emit("room:update", publicRoom(room));

function removePlayer(room, id) {
  const idx = room.players.findIndex((p) => p.id === id);
  if (idx < 0) return;
  const [p] = room.players.splice(idx, 1);
  clearTimeout(p.timer);
  clearTimeout(p.hostTimer);
  if (!room.players.length) {
    rooms.delete(room.code);
    return;
  }
  if (room.hostId === id) {
    room.hostId = (room.players.find((x) => x.connected) || room.players[0]).id;
  }
  broadcast(room);
}

function attach(socket, room, player) {
  if (player.socketId && player.socketId !== socket.id) {
    const old = io.sockets.sockets.get(player.socketId);
    if (old) {
      old.leave(room.code);
      old.data.code = null;
      old.emit("room:kicked", { reason: "You joined from another tab." });
    }
  }
  player.socketId = socket.id;
  player.connected = true;
  clearTimeout(player.timer);
  clearTimeout(player.hostTimer);
  socket.join(room.code);
  socket.data.code = room.code;
}

function leaveCurrent(socket) {
  const code = socket.data.code;
  if (!code) return;
  socket.leave(code);
  socket.data.code = null;
  const room = rooms.get(code);
  if (room) removePlayer(room, socket.data.clientId);
}

/* ───────── auth ───────── */
io.use((socket, next) => {
  const id = socket.handshake.auth?.clientId;
  if (typeof id !== "string" || !/^[a-zA-Z0-9-]{8,64}$/.test(id)) return next(new Error("bad client id"));
  socket.data.clientId = id;
  next();
});

io.on("connection", (socket) => {
  const clientId = socket.data.clientId;

  const ctx = () => {
    const room = rooms.get(socket.data.code);
    if (!room) return {};
    return { room, player: room.players.find((p) => p.id === clientId) };
  };
  const needHost = (cb) => {
    const { room, player } = ctx();
    if (!room || !player) { ack(cb, { ok: false, error: "You are not in a room." }); return null; }
    if (room.hostId !== clientId) { ack(cb, { ok: false, error: "Only the host can do that." }); return null; }
    return room;
  };

  socket.on("room:create", (data, cb) => {
    const name = clean(data?.name, 14) || "Player";
    const pw = String(data?.password ?? "");
    if (pw.length < 4 || pw.length > 32) return ack(cb, { ok: false, error: "Password must be 4–32 characters." });

    leaveCurrent(socket);
    let code;
    do { code = String(crypto.randomInt(100000, 1000000)); } while (rooms.has(code));

    const player = {
      id: clientId, name, socketId: null, connected: true,
      token: crypto.randomBytes(16).toString("hex"), timer: null, hostTimer: null,
    };
    const room = { code, pw: hashPw(pw), hostId: clientId, players: [player], selected: "ludo", game: null };
    rooms.set(code, room);
    attach(socket, room, player);
    ack(cb, { ok: true, token: player.token, room: publicRoom(room) });
  });

  socket.on("room:join", (data, cb) => {
    const now = Date.now();
    if ((socket.data.lockUntil || 0) > now) return ack(cb, { ok: false, error: "Too many attempts. Wait a few seconds." });

    const code = clean(data?.code, 8);
    const room = rooms.get(code);
    if (!room || !checkPw(room, data?.password)) {
      socket.data.fails = (socket.data.fails || 0) + 1;
      if (socket.data.fails >= 5) { socket.data.lockUntil = now + 30_000; socket.data.fails = 0; }
      return ack(cb, { ok: false, error: "Wrong room number or password." });
    }
    socket.data.fails = 0;

    if (socket.data.code && socket.data.code !== code) leaveCurrent(socket);

    let player = room.players.find((p) => p.id === clientId);
    if (!player) {
      if (room.players.length >= MAX_PLAYERS) return ack(cb, { ok: false, error: "Room is full." });
      player = {
        id: clientId, name: clean(data?.name, 14) || "Player", socketId: null, connected: true,
        token: crypto.randomBytes(16).toString("hex"), timer: null, hostTimer: null,
      };
      room.players.push(player);
    }
    attach(socket, room, player);
    broadcast(room);
    ack(cb, { ok: true, token: player.token, room: publicRoom(room) });
  });

  socket.on("room:resume", (data, cb) => {
    const room = rooms.get(clean(data?.code, 8));
    const player = room?.players.find((p) => p.id === clientId);
    if (!room || !player || player.token !== data?.token) {
      return ack(cb, { ok: false, error: "That room no longer exists." });
    }
    attach(socket, room, player);
    broadcast(room);
    ack(cb, { ok: true, room: publicRoom(room) });
  });

  socket.on("room:leave", (_d, cb) => { leaveCurrent(socket); ack(cb, { ok: true }); });

  socket.on("room:kick", (data, cb) => {
    const room = needHost(cb); if (!room) return;
    const target = room.players.find((p) => p.id === data?.id);
    if (!target || target.id === clientId) return ack(cb, { ok: false, error: "Invalid player." });
    const s = target.socketId && io.sockets.sockets.get(target.socketId);
    if (s) { s.leave(room.code); s.data.code = null; s.emit("room:kicked", { reason: "The host removed you from the room." }); }
    removePlayer(room, target.id);
    ack(cb, { ok: true });
  });

  socket.on("room:transfer_host", (data, cb) => {
    const room = needHost(cb); if (!room) return;
    const target = room.players.find((p) => p.id === data?.id && p.connected);
    if (!target) return ack(cb, { ok: false, error: "Player unavailable." });
    room.hostId = target.id;
    broadcast(room);
    ack(cb, { ok: true });
  });

  socket.on("room:set_order", (data, cb) => {
    const room = needHost(cb); if (!room) return;
    if (room.game) return ack(cb, { ok: false, error: "Can't reorder during a game." });
    const ids = Array.isArray(data?.order) ? data.order : [];
    const cur = room.players.map((p) => p.id);
    if (ids.length !== cur.length || !cur.every((id) => ids.includes(id))) return ack(cb, { ok: false, error: "Bad order." });
    room.players = ids.map((id) => room.players.find((p) => p.id === id));
    broadcast(room);
    ack(cb, { ok: true });
  });

  socket.on("room:select", (data, cb) => {
    const room = needHost(cb); if (!room) return;
    if (room.game || !GAME_META[data?.gameId]) return ack(cb, { ok: false, error: "Invalid game." });
    room.selected = data.gameId;
    broadcast(room);
    ack(cb, { ok: true });
  });

  socket.on("game:start", (data, cb) => {
    const room = needHost(cb); if (!room) return;
    const meta = GAME_META[data?.gameId];
    if (!meta) return ack(cb, { ok: false, error: "Unknown game." });
    if (room.game) return ack(cb, { ok: false, error: "A game is already running." });
    const online = room.players.filter((p) => p.connected);
    if (online.length < meta.min) return ack(cb, { ok: false, error: `Need at least ${meta.min} players for this game.` });

    const seatPlayers = meta.solo ? online : online.slice(0, meta.max);
    room.game = {
      id: data.gameId, solo: !!meta.solo, key: crypto.randomBytes(5).toString("hex"),
      seats: seatPlayers.map((p) => ({ id: p.id, name: p.name })), state: null, v: 0,
    };
    room.selected = data.gameId;
    broadcast(room);
    ack(cb, { ok: true });
  });

  socket.on("game:end", (_d, cb) => {
    const room = needHost(cb); if (!room) return;
    room.game = null;
    broadcast(room);
    ack(cb, { ok: true });
  });

  socket.on("game:state", (msg) => {
    const { room } = ctx();
    if (!room?.game || room.game.solo) return;
    if (!msg || msg.key !== room.game.key || msg.state == null) return;
    const seated = room.game.seats.some((s) => s.id === clientId);
    if (!seated && room.hostId !== clientId) return;
    let size;
    try { size = JSON.stringify(msg.state).length; } catch { return; }
    if (size > 200_000) return;
    room.game.state = msg.state;
    room.game.v++;
    socket.to(room.code).emit("game:state", { key: room.game.key, state: msg.state, v: room.game.v });
  });

  socket.on("game:sync", (_d, cb) => {
    const { room } = ctx();
    if (!room?.game) return ack(cb, { ok: false });
    ack(cb, { ok: true, key: room.game.key, state: room.game.state });
  });

  socket.on("disconnect", () => {
    const { room, player } = ctx();
    if (!room || !player || player.socketId !== socket.id) return;
    player.connected = false;
    player.socketId = null;
    player.timer = setTimeout(() => removePlayer(room, clientId), GRACE_MS);
    if (room.hostId === clientId) {
      player.hostTimer = setTimeout(() => {
        if (!player.connected && room.hostId === clientId) {
          const next = room.players.find((p) => p.connected);
          if (next) { room.hostId = next.id; broadcast(room); }
        }
      }, HOST_GRACE_MS);
    }
    broadcast(room);
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, "0.0.0.0", () => console.log(`🚀 Arcade server on :${PORT}`));