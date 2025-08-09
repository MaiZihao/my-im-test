import WebSocket, { WebSocketServer } from 'ws';
import Redis from 'ioredis';
import axios from 'axios';

const port = process.env.PORT || 8080;
const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
const authUrl = process.env.AUTH_SERVICE_URL || 'http://localhost:3000';

const redis = new Redis(redisUrl);
const sub = new Redis(redisUrl);

const wss = new WebSocketServer({ port });

// userId -> ws
const userConnections = new Map();

sub.subscribe('message:deliver');
sub.on('message', (channel, message) => {
  if (channel !== 'message:deliver') return;
  try {
    const payload = JSON.parse(message);
    const ws = userConnections.get(payload.toUserId);
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'message', message: payload }));
    }
  } catch (e) {
    console.error('Failed to deliver message', e);
  }
});

wss.on('connection', async (ws, req) => {
  const url = new URL(req.url, 'http://localhost');
  const token = url.searchParams.get('token');
  if (!token) {
    ws.close(1008, 'Unauthorized');
    return;
  }

  try {
    const { data } = await axios.get(`${authUrl}/me`, { headers: { Authorization: `Bearer ${token}` } });
    const userId = data.user.id;
    userConnections.set(userId, ws);
    ws.send(JSON.stringify({ type: 'init', user: data.user }));

    ws.on('message', async (raw) => {
      try {
        const msg = JSON.parse(raw.toString());
        if (msg.type === 'send') {
          await axios.post(`${authUrl}/messages`, { toUserId: msg.toUserId, text: msg.text }, { headers: { Authorization: `Bearer ${token}` } });
        }
      } catch (e) {
        ws.send(JSON.stringify({ type: 'error', error: 'bad_message' }));
      }
    });

    ws.on('close', () => {
      userConnections.delete(userId);
    });
  } catch (e) {
    ws.close(1008, 'Unauthorized');
  }
});

console.log(`Gateway listening on ${port}`);
