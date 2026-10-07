const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 3001;

// rooms: Map<roomId, { state: {}, players: {}, clients: Set<ws> }>
const rooms = new Map();

function getRoom(roomId) {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, {
      state: { phase: 'lobby', qIndex: -1, deadline: 0, semiFinalists: [], winnerId: null, revealStage: 0 },
      players: {},
      clients: new Set(),
    });
  }
  return rooms.get(roomId);
}

function broadcast(room) {
  const payload = JSON.stringify({ type: 'sync', state: room.state, players: room.players });
  for (const client of room.clients) {
    if (client.readyState === WebSocket.OPEN) client.send(payload);
  }
}

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('claude-ambassador-quest relay ok');
});

const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  if (url.pathname !== '/ws') {
    socket.destroy();
    return;
  }
  const roomId = (url.searchParams.get('room') || 'DEFAULT').toUpperCase();

  wss.handleUpgrade(req, socket, head, (ws) => {
    const room = getRoom(roomId);
    room.clients.add(ws);
    console.log(`[relay] client joined room ${roomId} (now ${room.clients.size})`);

    ws.send(JSON.stringify({ type: 'sync', state: room.state, players: room.players }));

    ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw.toString()); } catch (e) { return; }

      if (msg.type === 'patchState' && msg.patch && typeof msg.patch === 'object') {
        Object.assign(room.state, msg.patch);
        broadcast(room);
      } else if (msg.type === 'patchPlayer' && msg.id && msg.patch && typeof msg.patch === 'object') {
        room.players[msg.id] = Object.assign({}, room.players[msg.id], msg.patch);
        broadcast(room);
      }
    });

    ws.on('close', () => {
      room.clients.delete(ws);
      console.log(`[relay] client left room ${roomId} (now ${room.clients.size})`);
      if (room.clients.size === 0) {
        // keep room state briefly in case of reconnects; simplest is to just drop it
        rooms.delete(roomId);
      }
    });

    ws.on('error', (err) => {
      console.error(`[relay] error in room ${roomId}:`, err.message);
    });
  });
});

// Keepalive ping to prevent Render/proxy idle disconnect
const PING_INTERVAL = 25000;
setInterval(() => {
  for (const [, room] of rooms) {
    for (const client of room.clients) {
      if (client.readyState === WebSocket.OPEN) client.ping();
    }
  }
}, PING_INTERVAL);

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[relay] listening on port ${PORT}`);
});
