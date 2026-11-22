
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  transports: ['websocket', 'polling'],
  pingInterval: 25000,
  pingTimeout: 20000
});

const PORT = process.env.PORT || 3000;
const rooms = new Map();
const players = new Map();

app.get('/', (_req, res) => res.json({
  game: 'Terra Incognita Network Server',
  status: 'online',
  rooms: publicRooms().length,
  stage: 'room discovery and player position sync'
}));
app.get('/health', (_req, res) => res.json({ ok: true }));

function makeCode() {
  let code;
  do { code = 'TI-' + crypto.randomBytes(3).toString('hex').toUpperCase(); }
  while (rooms.has(code));
  return code;
}

function publicRooms() {
  return [...rooms.values()].map(r => ({
    id: r.id,
    name: r.name,
    players: r.players.size,
    max: r.max,
    hasPassword: !!r.password,
    seed: r.seed,
    mode: r.mode
  }));
}

function sendRoomList() {
  io.emit('server:list', publicRooms());
}

function roomPlayers(room) {
  return [...room.players].map(id => {
    const p = players.get(id);
    return p ? {
      id,
      name: p.name,
      x: p.x,
      y: p.y,
      face: p.face,
      held: p.held || null
    } : null;
  }).filter(Boolean);
}

function leaveRoom(socket) {
  const oldId = socket.data.roomId;
  if (!oldId) return;

  const room = rooms.get(oldId);
  socket.leave(oldId);
  socket.data.roomId = null;

  if (!room) return;

  room.players.delete(socket.id);

  if (room.owner === socket.id) {
    io.to(oldId).emit('room:closed');

    for (const id of room.players) {
      const peer = io.sockets.sockets.get(id);
      if (peer) peer.data.roomId = null;
    }

    rooms.delete(oldId);
  } else {
    io.to(oldId).emit('player:left', socket.id);
  }

  sendRoomList();
}

io.on('connection', socket => {
  players.set(socket.id, {
    name: 'Игрок',
    x: 0,
    y: 0,
    face: { x: 1, y: 0 },
    held: null
  });

  socket.emit('server:list', publicRooms());
  socket.on('server:list', () => socket.emit('server:list', publicRooms()));

  socket.on('room:create', (data = {}, reply = () => {}) => {
    leaveRoom(socket);

    const max = Math.max(2, Math.min(20, Number(data.max) || 8));

    const room = {
      id: makeCode(),
      name: String(data.name || 'Мой сервер').slice(0, 28),
      password: String(data.password || '').slice(0, 32),
      max,
      seed: String(data.seed || '').slice(0, 32),
      mode: data.mode === 'creative' ? 'creative' : 'survival',
      owner: socket.id,
      players: new Set([socket.id]),
      messages: [],
      created: Date.now()
    };

    rooms.set(room.id, room);
    socket.join(room.id);
    socket.data.roomId = room.id;

    reply({
      ok: true,
      room: {
        id: room.id,
        name: room.name,
        max: room.max,
        seed: room.seed,
        mode: room.mode
      }
    });

    socket.emit('room:players', roomPlayers(room));
    socket.emit('chat:history', room.messages);
    sendRoomList();
  });

  socket.on('room:join', (data = {}, reply = () => {}) => {
    const id = String(data.id || '').toUpperCase();
    const room = rooms.get(id);

    if (!room) {
      return reply({
        ok: false,
        error: 'Сервер не найден или уже закрыт.'
      });
    }

    if (room.password && String(data.password || '') !== room.password) {
      return reply({ ok: false, error: 'Неверный пароль.' });
    }

    if (room.players.size >= room.max) {
      return reply({
        ok: false,
        error: 'На сервере нет свободных мест.'
      });
    }

    leaveRoom(socket);

    room.players.add(socket.id);
    socket.join(id);
    socket.data.roomId = id;

    reply({
      ok: true,
      room: {
        id: room.id,
        name: room.name,
        max: room.max,
        seed: room.seed,
        mode: room.mode
      },
      players: roomPlayers(room)
    });

    socket.emit('chat:history', room.messages || []);

    socket.to(id).emit(
      'player:update',
      Object.assign({ id: socket.id }, players.get(socket.id))
    );

    io.to(id).emit('room:players', roomPlayers(room));
    sendRoomList();
  });

  socket.on('player:update', data => {
    const roomId = socket.data.roomId;
    const room = rooms.get(roomId);
    const p = players.get(socket.id);

    if (!room || !p || !data) return;

    const x = Number(data.x);
    const y = Number(data.y);

    if (
      !Number.isFinite(x) ||
      !Number.isFinite(y) ||
      Math.abs(x) > 10000000 ||
      Math.abs(y) > 10000000
    ) return;

    p.x = x;
    p.y = y;
    p.name = String(data.name || 'Игрок').slice(0, 20);

    if (
      data.face &&
      Number.isFinite(Number(data.face.x)) &&
      Number.isFinite(Number(data.face.y))
    ) {
      p.face = {
        x: Number(data.face.x),
        y: Number(data.face.y)
      };
    }

    p.held = data.held && typeof data.held.key === 'string'
      ? { key: data.held.key.slice(0, 64) }
      : null;

    // Надёжная отправка обновлений игрока.
    socket.to(roomId).emit(
      'player:update',
      Object.assign({ id: socket.id }, p)
    );
  });

  // Отправка сообщения в чат.
  socket.on('chat:send', raw => {
    const roomId = socket.data.roomId;
    const room = rooms.get(roomId);
    const player = players.get(socket.id);

    if (!room || !player || typeof raw !== 'string') return;

    const now = Date.now();

    // Ограничение частоты отправки сообщений.
    if (now - (socket.data.lastChatAt || 0) < 700) return;

    const message = raw
      .replace(/[\u0000-\u001f\u007f]/g, ' ')
      .trim()
      .slice(0, 240);

    if (!message) return;

    socket.data.lastChatAt = now;

    const entry = {
      id: crypto.randomBytes(6).toString('hex'),
      name: String(player.name || 'Игрок').slice(0, 20),
      text: message,
      time: now
    };

    room.messages = room.messages || [];
    room.messages.push(entry);

    // Храним последние 50 сообщений.
    if (room.messages.length > 50) {
      room.messages.shift();
    }

    io.to(roomId).emit('chat:message', entry);
  });

  socket.on('room:leave', () => leaveRoom(socket));

  socket.on('disconnect', () => {
    leaveRoom(socket);
    players.delete(socket.id);
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Terra Incognita network server listening on ${PORT}`);
});
