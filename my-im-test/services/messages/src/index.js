import express from 'express';
import bodyParser from 'body-parser';
import cors from 'cors';
import pkg from 'pg';
import Redis from 'ioredis';

const { Pool } = pkg;

const port = process.env.PORT || 3000;
const databaseUrl = process.env.DATABASE_URL || 'postgresql://im:im@localhost:5432/im';
const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';

const pool = new Pool({ connectionString: databaseUrl });
const redis = new Redis(redisUrl);

async function waitForDb(maxAttempts = 30, delayMs = 1000) {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      await pool.query('SELECT 1');
      return;
    } catch (e) {
      if (attempt === maxAttempts) throw e;
      await new Promise(r => setTimeout(r, delayMs));
    }
  }
}

async function ensureSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      username TEXT UNIQUE NOT NULL,
      token TEXT UNIQUE NOT NULL
    );
    CREATE TABLE IF NOT EXISTS messages (
      id SERIAL PRIMARY KEY,
      from_user_id INTEGER NOT NULL,
      to_user_id INTEGER NOT NULL,
      text TEXT NOT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
  `);
}

const app = express();
app.use(cors());
app.use(bodyParser.json());

app.post('/register', async (req, res) => {
  const { username } = req.body;
  if (!username) return res.status(400).json({ error: 'username_required' });
  const token = Buffer.from(username + ':' + Date.now()).toString('base64');
  try {
    const result = await pool.query(
      'INSERT INTO users (username, token) VALUES ($1, $2) RETURNING id, username, token',
      [username, token]
    );
    res.json({ user: result.rows[0] });
  } catch (e) {
    res.status(400).json({ error: 'username_taken' });
  }
});

app.get('/me', async (req, res) => {
  const auth = req.headers['authorization'] || '';
  const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
  if (!token) return res.status(401).json({ error: 'unauthorized' });
  const result = await pool.query('SELECT id, username FROM users WHERE token=$1', [token]);
  if (result.rowCount === 0) return res.status(401).json({ error: 'unauthorized' });
  res.json({ user: result.rows[0] });
});

app.post('/messages', async (req, res) => {
  const auth = req.headers['authorization'] || '';
  const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
  if (!token) return res.status(401).json({ error: 'unauthorized' });
  const me = await pool.query('SELECT id FROM users WHERE token=$1', [token]);
  if (me.rowCount === 0) return res.status(401).json({ error: 'unauthorized' });
  const fromUserId = me.rows[0].id;

  const { toUserId, text } = req.body;
  if (!toUserId || !text) return res.status(400).json({ error: 'bad_request' });

  const saved = await pool.query(
    'INSERT INTO messages (from_user_id, to_user_id, text) VALUES ($1,$2,$3) RETURNING id, from_user_id, to_user_id, text, created_at',
    [fromUserId, toUserId, text]
  );

  await redis.publish('message:deliver', JSON.stringify({
    id: saved.rows[0].id,
    fromUserId,
    toUserId,
    text,
    createdAt: saved.rows[0].created_at
  }));

  res.json({ message: saved.rows[0] });
});

app.get('/messages', async (req, res) => {
  const auth = req.headers['authorization'] || '';
  const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
  if (!token) return res.status(401).json({ error: 'unauthorized' });
  const me = await pool.query('SELECT id FROM users WHERE token=$1', [token]);
  if (me.rowCount === 0) return res.status(401).json({ error: 'unauthorized' });
  const userId = me.rows[0].id;

  const result = await pool.query(
    'SELECT id, from_user_id, to_user_id, text, created_at FROM messages WHERE from_user_id=$1 OR to_user_id=$1 ORDER BY id DESC LIMIT 50',
    [userId]
  );
  res.json({ messages: result.rows });
});

waitForDb()
  .then(() => ensureSchema())
  .then(() => {
    app.listen(port, () => console.log(`Messages service listening on ${port}`));
  })
  .catch((e) => {
    console.error('Failed to start messages service', e);
    process.exit(1);
  });