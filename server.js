const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const cors = require("cors");
const crypto = require("crypto");

const app = express();
app.use(express.json());

const allowedOrigins = (process.env.CLIENT_ORIGIN || "*")
  .split(",")
  .map((origin) => origin.trim())
  .filter(Boolean);

app.use(cors({
  origin: (origin, callback) => {
    // No Origin is normal for health checks and server-to-server requests.
    if (!origin || allowedOrigins.includes("*") || allowedOrigins.includes(origin)) {
      return callback(null, true);
    }
    return callback(new Error("Origin not allowed by CLIENT_ORIGIN"));
  }
}));

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: allowedOrigins.includes("*") ? "*" : allowedOrigins,
    methods: ["GET", "POST"]
  },
  transports: ["websocket", "polling"]
});

const PORT = Number(process.env.PORT || 3000);
const MAX_PLAYERS_PER_ROOM = 10;
const TEAM_MODES = new Set([1, 2, 3, 4, 5]);
const TICK_RATE_MS = 50; // 20 state updates per second
const MAX_MOVE_PER_PACKET = 3;
const MAX_HEALTH = 100;

const rooms = new Map();

function newRoom(roomId) {
  return {
    id: roomId,
    mode: 5,
    lobbyName: "Private Match",
    createdAt: Date.now(),
    players: new Map(),
    round: 1,
    score: { attackers: 0, defenders: 0 },
    phase: "warmup",
    bomb: { planted: false, plantedAt: null, fuseMs: 45000, defuseProgress: 0, defusingBy: null },
    roundEndsAt: null
  };
}

function getRoom(roomId) {
  if (!rooms.has(roomId)) rooms.set(roomId, newRoom(roomId));
  return rooms.get(roomId);
}

function safePlayer(player) {
  return {
    id: player.id,
    name: player.name,
    team: player.team,
    x: player.x,
    y: player.y,
    z: player.z,
    yaw: player.yaw,
    pitch: player.pitch,
    health: player.health,
    alive: player.alive,
    kills: player.kills,
    deaths: player.deaths,
    weapon: player.weapon,
    reloading: player.reloading
  };
}

function roomSnapshot(room) {
  return {
    roomId: room.id,
    mode: room.mode || 5,
    lobbyName: room.lobbyName || "Private Match",
    round: room.round,
    phase: room.phase,
    score: room.score,
    bomb: {
      planted: room.bomb.planted,
      plantedAt: room.bomb.plantedAt,
      fuseMs: room.bomb.fuseMs,
      defuseProgress: room.bomb.defuseProgress,
      defusingBy: room.bomb.defusingBy
    },
    players: [...room.players.values()].map(safePlayer)
  };
}

function broadcastRoom(room) {
  io.to(room.id).emit("state", roomSnapshot(room));
}

function endRound(room, winner, reason) {
  if (room.phase === "results") return;
  room.phase = "results";
  room.score[winner] += 1;
  room.roundEndsAt = Date.now() + 5000;
  room.bomb.defusingBy = null;
  io.to(room.id).emit("roundEnd", {
    winner,
    announcement: winner === "attackers" ? "ATTACKERS WIN" : "DEFENDERS WIN",
    reason,
    round: room.round,
    score: room.score,
    players: [...room.players.values()].map(safePlayer),
    nextRoundInMs: 5000
  });
  broadcastRoom(room);
}

function startNextRound(room) {
  room.round += 1;
  room.phase = "live";
  room.bomb = { planted: false, plantedAt: null, fuseMs: 45000, defuseProgress: 0, defusingBy: null };
  room.roundEndsAt = null;
  for (const p of room.players.values()) {
    p.health = MAX_HEALTH;
    p.alive = true;
    p.reloading = false;
    p.x = p.team === "attackers" ? -4 : 4;
    p.y = 1.7;
    p.z = 0;
  }
  io.to(room.id).emit("roundStart", { round: room.round, score: room.score });
  broadcastRoom(room);
}

app.get("/", (_req, res) => {
  res.json({ service: "WILDFROST FPS realtime server", status: "ok", rooms: rooms.size });
});
app.get("/health", (_req, res) => res.status(200).json({ ok: true }));

io.on("connection", (socket) => {
  socket.emit("connected", { id: socket.id, message: "Connected to WILDFROST realtime server" });

  // Invite codes are generated server-side and reserve a fixed 1v1–5v5 format.
  socket.on("createLobby", (payload = {}, ack = () => {}) => {
    const mode = Number(payload.mode);
    if (!TEAM_MODES.has(mode)) return ack({ ok: false, error: "Choose a format from 1v1 to 5v5" });
    let code;
    do { code = crypto.randomBytes(3).toString("hex").toUpperCase(); } while (rooms.has(code));
    const room = newRoom(code);
    room.mode = mode;
    room.createdByInvite = true;
    room.lobbyName = String(payload.name || "Private Match").trim().slice(0, 28) || "Private Match";
    rooms.set(code, room);
    ack({ ok: true, code, mode, capacity: mode * 2 });
  });

  socket.on("joinLobby", (payload = {}, ack = () => {}) => {
    const code = String(payload.code || "").trim().toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 8);
    const room = rooms.get(code);
    if (!code || !room) return ack({ ok: false, error: "Invite code not found. Ask the host to create a lobby and share its code." });
    if (Number(payload.mode) !== Number(room.mode)) return ack({ ok: false, error: `This code is for ${room.mode}v${room.mode}. Select that format to join.` });
    if (room.players.size >= room.mode * 2) return ack({ ok: false, error: "Lobby is full" });
    ack({ ok: true, code, mode: room.mode, capacity: room.mode * 2, lobbyName: room.lobbyName });
  });

  socket.on("joinRoom", (payload = {}, ack = () => {}) => {
    const roomId = String(payload.roomId || "public").trim().slice(0, 32);
    const name = String(payload.name || "Player").trim().slice(0, 20) || "Player";
    const requestedTeam = payload.team === "defenders" ? "defenders" : "attackers";

    // Remove the socket from its previous game room first.
    for (const oldRoom of rooms.values()) {
      if (oldRoom.players.has(socket.id)) {
        oldRoom.players.delete(socket.id);
        socket.leave(oldRoom.id);
        broadcastRoom(oldRoom);
      }
    }

    const room = getRoom(roomId);
    const requestedMode = Number(payload.mode);
    if (TEAM_MODES.has(requestedMode) && room.players.size === 0 && !room.createdByInvite) room.mode = requestedMode;
    const capacity = Math.min(MAX_PLAYERS_PER_ROOM, (room.mode || 5) * 2);
    if (TEAM_MODES.has(requestedMode) && requestedMode !== room.mode) {
      ack({ ok: false, error: `This lobby is configured for ${room.mode}v${room.mode}` });
      return;
    }
    if (room.players.size >= capacity) {
      ack({ ok: false, error: "Lobby is full" });
      return;
    }

    // Assign teams evenly so RED and BLUE never differ by more than one player.
    const attackers = [...room.players.values()].filter((p) => p.team === "attackers").length;
    const defenders = [...room.players.values()].filter((p) => p.team === "defenders").length;
    let team = requestedTeam;
    if (attackers > defenders) team = "defenders";
    else if (defenders > attackers) team = "attackers";

    const player = {
      id: socket.id,
      name,
      team,
      x: team === "attackers" ? -4 : 4,
      y: 1.7,
      z: 0,
      yaw: 0,
      pitch: 0,
      health: MAX_HEALTH,
      alive: true,
      kills: 0,
      deaths: 0,
      weapon: "rifle",
      reloading: false,
      lastMoveAt: 0
    };

    room.players.set(socket.id, player);
    socket.join(roomId);
    if (room.phase === "warmup") room.phase = "live";
    ack({ ok: true, player: safePlayer(player), state: roomSnapshot(room) });
    broadcastRoom(room);
  });

  socket.on("move", (payload = {}) => {
    const room = [...rooms.values()].find((r) => r.players.has(socket.id));
    if (!room || room.phase !== "live") return;
    const p = room.players.get(socket.id);
    if (!p.alive) return;

    const now = Date.now();
    if (now - p.lastMoveAt < 20) return; // limit spam
    p.lastMoveAt = now;

    for (const axis of ["x", "y", "z"]) {
      const value = Number(payload[axis]);
      if (Number.isFinite(value) && Math.abs(value - p[axis]) <= MAX_MOVE_PER_PACKET) {
        p[axis] = value;
      }
    }
    if (Number.isFinite(payload.yaw)) p.yaw = payload.yaw;
    if (Number.isFinite(payload.pitch)) p.pitch = Math.max(-1.5, Math.min(1.5, payload.pitch));
    if (typeof payload.weapon === "string" && ["rifle", "pistol", "knife"].includes(payload.weapon)) {
      p.weapon = payload.weapon;
    }
    broadcastRoom(room);
  });

  socket.on("plantBomb", () => {
    const room = [...rooms.values()].find((r) => r.players.has(socket.id));
    if (!room || room.phase !== "live" || room.bomb.planted) return;
    const p = room.players.get(socket.id);
    if (!p || !p.alive || p.team !== "attackers") return;
    room.bomb.planted = true;
    room.bomb.plantedAt = Date.now();
    room.bomb.defuseProgress = 0;
    room.bomb.defusingBy = null;
    io.to(room.id).emit("bombPlanted", { plantedAt: room.bomb.plantedAt, fuseMs: room.bomb.fuseMs });
    broadcastRoom(room);
  });

  socket.on("defuseProgress", (payload = {}) => {
    const room = [...rooms.values()].find((r) => r.players.has(socket.id));
    if (!room || room.phase !== "live" || !room.bomb.planted) return;
    const p = room.players.get(socket.id);
    if (!p || !p.alive || p.team !== "defenders") return;
    const progress = Number(payload.progress);
    if (!Number.isFinite(progress)) return;
    room.bomb.defuseProgress = Math.max(0, Math.min(1, progress));
    room.bomb.defusingBy = room.bomb.defuseProgress > 0 && room.bomb.defuseProgress < 1 ? socket.id : null;
    if (room.bomb.defuseProgress >= 1) endRound(room, "defenders", "bomb_defused");
    else broadcastRoom(room);
  });

  // Client requests a hit; server validates target/team/weapon basics.
  // For production, add server-side raycasts, rate limits, map collision and authoritative ammo.
  socket.on("hitRequest", (payload = {}, ack = () => {}) => {
    const room = [...rooms.values()].find((r) => r.players.has(socket.id));
    if (!room || room.phase !== "live") return ack({ ok: false, error: "Round is not live" });
    const attacker = room.players.get(socket.id);
    const target = room.players.get(String(payload.targetId || ""));
    if (!attacker || !target || !attacker.alive || !target.alive) return ack({ ok: false, error: "Invalid target" });
    if (attacker.team === target.team) return ack({ ok: false, error: "Friendly fire disabled" });

    const damage = Math.max(1, Math.min(100, Math.floor(Number(payload.damage) || 20)));
    target.health = Math.max(0, target.health - damage);
    if (target.health === 0) {
      target.alive = false;
      target.deaths += 1;
      attacker.kills += 1;
      io.to(room.id).emit("playerDied", { victimId: target.id, killerId: attacker.id });
    }
    ack({ ok: true, target: safePlayer(target) });
    broadcastRoom(room);
  });

  socket.on("respawn", () => {
    const room = [...rooms.values()].find((r) => r.players.has(socket.id));
    if (!room) return;
    const p = room.players.get(socket.id);
    if (!p || p.alive || room.phase !== "live") return;
    p.health = MAX_HEALTH;
    p.alive = true;
    p.x = p.team === "attackers" ? -4 : 4;
    p.y = 1.7;
    p.z = 0;
    broadcastRoom(room);
  });

  socket.on("disconnect", () => {
    for (const room of rooms.values()) {
      if (room.players.delete(socket.id)) {
        broadcastRoom(room);
      }
    }
  });
});

setInterval(() => {
  const now = Date.now();
  for (const room of rooms.values()) {
    if (room.phase === "live" && room.bomb.planted && room.bomb.plantedAt &&
        now >= room.bomb.plantedAt + room.bomb.fuseMs) {
      endRound(room, "attackers", "bomb_detonated");
    }
    if (room.phase === "results" && room.roundEndsAt && now >= room.roundEndsAt) {
      startNextRound(room);
    }
    // Broadcast authoritative state at a fixed cadence.
    if (room.players.size > 0) broadcastRoom(room);
    if (room.players.size === 0 && room.phase === "warmup") rooms.delete(room.id);
  }
}, TICK_RATE_MS);

server.listen(PORT, "0.0.0.0", () => {
  console.log(`WILDFROST realtime server listening on port ${PORT}`);
});
