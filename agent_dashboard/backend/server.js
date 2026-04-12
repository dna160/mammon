/**
 * Mammon V2 Dashboard Backend
 * Express REST API + WebSocket server.
 *
 * Data sources:
 *   PostgreSQL — trade_telemetry, agent_q_memory, execution_log
 *   Redis       — live engine state: pipeline, orders, last_fill,
 *                 regime, live_params, state_vector
 *
 * WebSocket (ws://host/ws) pushes the full dashboard snapshot every 1 second.
 * REST endpoints serve the same data on demand.
 */

'use strict';

const express    = require('express');
const cors       = require('cors');
const http       = require('http');
const { Pool }   = require('pg');
const Redis      = require('ioredis');
const WebSocket  = require('ws');

const app    = express();
const server = http.createServer(app);
const PORT   = process.env.PORT    || 4000;
const DB_DSN = process.env.DB_DSN  || 'postgresql://mammon:mammon@localhost:5432/mammon';
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

const SYMBOLS = ['ADAFDUSD', 'DOTFDUSD', 'DOGEFDUSD', 'XRPFDUSD'];

// ── Infrastructure ────────────────────────────────────────────────────────────

const pool  = new Pool({ connectionString: DB_DSN });
const redis = new Redis(REDIS_URL, { lazyConnect: true, maxRetriesPerRequest: 2 });

redis.on('error', (err) => console.error('[Redis] Connection error:', err.message));

app.use(cors({ origin: '*' }));
app.use(express.json());

// Disable caching for all API responses
app.use((_req, res, next) => {
  res.set('Cache-Control', 'no-store');
  next();
});
app.set('etag', false);

// ── WebSocket server ──────────────────────────────────────────────────────────

const wss = new WebSocket.Server({ server, path: '/ws' });

wss.on('connection', (ws) => {
  console.log('[WS] Client connected. Total:', wss.clients.size);
  ws.on('close', () => console.log('[WS] Client disconnected. Total:', wss.clients.size));
  ws.on('error', (e) => console.error('[WS] Client error:', e.message));
});

function broadcast(data) {
  const json = JSON.stringify(data);
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      try { client.send(json); } catch (_) {}
    }
  });
}

// ── Data collectors ───────────────────────────────────────────────────────────

async function getHoldings() {
  const holdings = {};
  await Promise.all(
    SYMBOLS.map(async (sym) => {
      try {
        const raw = await redis.get(`engine_d:${sym}:pipeline`);
        if (raw) {
          const d = JSON.parse(raw);
          holdings[sym] = {
            inventory_coin: parseFloat(d.inventory_coin ?? 0),
            pnl_usd:        parseFloat(d.pnl_usd       ?? 0),
            micro_price:    parseFloat(d.micro_price    ?? 0),
            total_trades:   parseInt(d.total_trades     ?? 0, 10),
            decision:       d.decision ?? 'WARMING',
            obi:            parseFloat(d.obi            ?? 0),
            tfi:            parseFloat(d.tfi            ?? 0),
            variance:       parseFloat(d.variance       ?? 0),
            optimal_bid:    parseFloat(d.optimal_bid    ?? 0),
            optimal_ask:    parseFloat(d.optimal_ask    ?? 0),
            reservation:    parseFloat(d.reservation    ?? 0),
            warm_ticks:     parseInt(d.warm_ticks       ?? 0, 10),
            ts:             d.ts ?? null,
          };
        } else {
          holdings[sym] = null;
        }
      } catch (e) {
        holdings[sym] = null;
      }
    })
  );
  return holdings;
}

async function getOrders() {
  const orders = {};
  await Promise.all(
    SYMBOLS.map(async (sym) => {
      try {
        const raw = await redis.get(`engine_d:${sym}:orders`);
        if (raw) {
          const d = JSON.parse(raw);
          orders[sym] = {
            bid_id:        d.bid_id   ?? null,
            ask_id:        d.ask_id   ?? null,
            bid_price:     parseFloat(d.bid_price      ?? 0),
            ask_price:     parseFloat(d.ask_price      ?? 0),
            real_pnl_usd:  parseFloat(d.real_pnl_usd   ?? 0),
            real_inventory: parseFloat(d.real_inventory ?? 0),
            real_trades:   parseInt(d.real_trades      ?? 0, 10),
            ts:            d.ts ?? null,
            // ping-pong state: 0 = bidding (State 0/Empty), 1 = asking (State 1/Loaded)
            ping_pong: d.bid_id && !d.ask_id ? 0 :
                       d.ask_id && !d.bid_id ? 1 : null,
          };
        } else {
          orders[sym] = null;
        }
      } catch (e) {
        orders[sym] = null;
      }
    })
  );
  return orders;
}

async function getLastFills() {
  const fills = {};
  await Promise.all(
    SYMBOLS.map(async (sym) => {
      try {
        const raw = await redis.get(`engine_d:${sym}:last_fill`);
        fills[sym] = raw ? JSON.parse(raw) : null;
      } catch (e) {
        fills[sym] = null;
      }
    })
  );
  return fills;
}

async function getAgentQ() {
  const agentQ = {};
  await Promise.all(
    SYMBOLS.map(async (sym) => {
      const symL = sym.toLowerCase();
      try {
        const [rawRegime, rawParams, rawSV] = await Promise.all([
          redis.get(`hft:regime:${symL}:latest`),
          redis.get(`hft:live_params:${symL}:latest`),
          redis.get(`cognitive:state_vector:${symL}`),
        ]);
        agentQ[sym] = {
          regime:       rawRegime ? JSON.parse(rawRegime) : null,
          params:       rawParams ? JSON.parse(rawParams) : null,
          state_vector: rawSV     ? JSON.parse(rawSV)     : null,
        };
      } catch (e) {
        agentQ[sym] = { regime: null, params: null, state_vector: null };
      }
    })
  );
  return agentQ;
}

async function getKpis() {
  try {
    const result = await pool.query(`
      SELECT
        COUNT(*)::int                      AS daily_trades,
        COALESCE(SUM(trade_roe_pct), 0)   AS daily_roe,
        COALESCE(SUM(net_pnl), 0)         AS total_net_pnl,
        COALESCE(AVG(net_pnl), 0)         AS avg_pnl_per_trade,
        COALESCE(
          100.0 * SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END)::float
          / NULLIF(COUNT(*), 0),
          0
        )                                  AS win_rate_pct
      FROM trade_telemetry
      WHERE engine_id = 'D'
        AND timestamp > NOW() - INTERVAL '24 hours'
    `);
    const r = result.rows[0] || {};
    return {
      daily_trades:      r.daily_trades      || 0,
      daily_roe:         parseFloat(r.daily_roe)         || 0,
      total_net_pnl:     parseFloat(r.total_net_pnl)     || 0,
      avg_pnl_per_trade: parseFloat(r.avg_pnl_per_trade) || 0,
      win_rate_pct:      parseFloat(r.win_rate_pct)      || 0,
    };
  } catch (e) {
    console.error('[KPIs] query error:', e.message);
    return { daily_trades: 0, daily_roe: 0, total_net_pnl: 0, avg_pnl_per_trade: 0, win_rate_pct: 0 };
  }
}

async function getTrades(limit = 100) {
  try {
    const result = await pool.query(`
      SELECT
        trade_id::text,
        timestamp,
        engine_id,
        asset_pair,
        trade_size_idr,
        entry_signal_value,
        gross_pnl,
        fees_paid,
        net_pnl,
        trade_roe_pct
      FROM trade_telemetry
      WHERE engine_id = 'D'
      ORDER BY timestamp DESC
      LIMIT $1
    `, [limit]);
    return result.rows;
  } catch (e) {
    console.error('[Trades] query error:', e.message);
    return [];
  }
}

async function getChart() {
  try {
    const result = await pool.query(`
      SELECT
        date_trunc('minute', timestamp)  AS bucket,
        COALESCE(SUM(net_pnl), 0)        AS period_pnl,
        SUM(SUM(net_pnl)) OVER (
          ORDER BY date_trunc('minute', timestamp)
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                AS cumulative_pnl
      FROM trade_telemetry
      WHERE engine_id = 'D'
        AND timestamp > NOW() - INTERVAL '24 hours'
      GROUP BY bucket
      ORDER BY bucket ASC
    `);
    return result.rows.map((r) => ({
      time:           r.bucket.toISOString(),
      period_pnl:     parseFloat(r.period_pnl)     || 0,
      cumulative_pnl: parseFloat(r.cumulative_pnl) || 0,
    }));
  } catch (e) {
    console.error('[Chart] query error:', e.message);
    return [];
  }
}

async function getRewardHistory(limit = 50) {
  try {
    const result = await pool.query(`
      SELECT
        id, timestamp, evaluated_at, symbol, regime,
        proposed_gamma, proposed_min_spread, tfi_threshold,
        vol_bps, tfi_zscore, drift_bps, native_spread,
        net_pnl, adverse_selection, reward_score,
        alpha_reasoning, cro_reasoning, override_applied
      FROM agent_q_memory
      ORDER BY timestamp DESC
      LIMIT $1
    `, [limit]);
    return result.rows;
  } catch (e) {
    console.error('[RewardHistory] query error:', e.message);
    return [];
  }
}

async function getFullSnapshot() {
  const [holdings, orders, lastFills, agentQ, kpis, trades, chart] = await Promise.all([
    getHoldings(),
    getOrders(),
    getLastFills(),
    getAgentQ(),
    getKpis(),
    getTrades(100),
    getChart(),
  ]);

  // Compute per-symbol cumulative PnL from holdings (live engine data)
  const livePnl = {};
  SYMBOLS.forEach((sym) => {
    if (holdings[sym]) livePnl[sym] = holdings[sym].pnl_usd;
  });

  return {
    ts:        Date.now(),
    kpis,
    trades,
    chart,
    holdings,
    orders,
    last_fills: lastFills,
    agent_q:   agentQ,
    live_pnl:  livePnl,
    symbols:   SYMBOLS,
  };
}

// ── WebSocket broadcast loop (1 Hz) ──────────────────────────────────────────

let _lastSnapshot = null;

async function broadcastLoop() {
  try {
    const snapshot  = await getFullSnapshot();
    _lastSnapshot   = snapshot;
    if (wss.clients.size > 0) broadcast(snapshot);
  } catch (e) {
    console.error('[Broadcast] error:', e.message);
  }
}

setInterval(broadcastLoop, 1000);

// ── REST endpoints ────────────────────────────────────────────────────────────

app.get('/health', (_req, res) => res.json({ status: 'ok', ts: new Date().toISOString() }));

app.get('/api/snapshot', async (_req, res) => {
  try {
    res.json(_lastSnapshot ?? await getFullSnapshot());
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/kpis', async (_req, res) => {
  try { res.json({ D: await getKpis() }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/trades', async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit ?? '100', 10), 500);
  try { res.json(await getTrades(limit)); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/chart', async (_req, res) => {
  try { res.json(await getChart()); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/holdings', async (_req, res) => {
  try { res.json(await getHoldings()); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/orders', async (_req, res) => {
  try { res.json(await getOrders()); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/agent-q', async (_req, res) => {
  try { res.json(await getAgentQ()); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/reward-history', async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit ?? '50', 10), 200);
  try { res.json(await getRewardHistory(limit)); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Start ─────────────────────────────────────────────────────────────────────

server.listen(PORT, () => {
  console.log(`Mammon V2 dashboard backend on port ${PORT}`);
  console.log(`WebSocket path: ws://localhost:${PORT}/ws`);
  // Warm up first snapshot
  broadcastLoop();
});
