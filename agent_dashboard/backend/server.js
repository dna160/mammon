/**
 * Mammon Dashboard Backend
 * Express REST API — serves Engine D trade telemetry from PostgreSQL to the React frontend.
 */

const express = require('express');
const cors    = require('cors');
const { Pool } = require('pg');

const app  = express();
const PORT = process.env.PORT || 4000;
const DB_DSN = process.env.DB_DSN || 'postgresql://mammon:mammon@localhost:5432/mammon';

// PostgreSQL connection pool
const pool = new Pool({ connectionString: DB_DSN });

// Allow all origins (dashboard_frontend is on the same Docker network, proxied through nginx)
app.use(cors({ origin: '*' }));
app.use(express.json());

// Disable HTTP caching for all API responses — we need live data on every poll
app.use((req, res, next) => {
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.set('Pragma', 'no-cache');
  res.set('Expires', '0');
  next();
});

// Disable Express ETags so browsers never get 304 Not Modified
app.set('etag', false);

// ── Health ────────────────────────────────────────────────────────────────────

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// ── GET /api/kpis ─────────────────────────────────────────────────────────────
// Returns daily trade count and cumulative ROE% for Engine D (24h window).
// Response: { D: { daily_trades: N, daily_roe: X } }

app.get('/api/kpis', async (_req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        COUNT(*)::int                    AS daily_trades,
        COALESCE(SUM(trade_roe_pct), 0) AS daily_roe
      FROM trade_telemetry
      WHERE engine_id = 'D'
        AND timestamp > NOW() - INTERVAL '24 hours'
    `);

    const row = result.rows[0] || { daily_trades: 0, daily_roe: 0 };
    res.json({
      D: {
        daily_trades: row.daily_trades,
        daily_roe:    parseFloat(row.daily_roe),
      },
    });
  } catch (err) {
    console.error('KPIs query error:', err.message);
    res.status(500).json({ error: 'Database error' });
  }
});

// ── GET /api/trades ───────────────────────────────────────────────────────────
// Returns latest 50 Engine D trade records across all Binance pairs, newest first.

app.get('/api/trades', async (_req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        trade_id,
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
      LIMIT 50
    `);
    res.json(result.rows);
  } catch (err) {
    console.error('Trades query error:', err.message);
    res.status(500).json({ error: 'Database error' });
  }
});

// ── GET /api/chart ────────────────────────────────────────────────────────────
// Returns cumulative ROE% over time for Engine D.
// Response: [{ time: ISO, D: cumROE }, ...]

app.get('/api/chart', async (_req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        date_trunc('minute', timestamp) AS bucket,
        SUM(trade_roe_pct) OVER (
          ORDER BY date_trunc('minute', timestamp)
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_roe
      FROM trade_telemetry
      WHERE engine_id = 'D'
        AND timestamp > NOW() - INTERVAL '24 hours'
      ORDER BY bucket ASC
    `);

    const chartData = result.rows.map((row) => ({
      time: row.bucket.toISOString(),
      D:    parseFloat(row.cumulative_roe),
    }));

    res.json(chartData);
  } catch (err) {
    console.error('Chart query error:', err.message);
    res.status(500).json({ error: 'Database error' });
  }
});

// ── Start ─────────────────────────────────────────────────────────────────────

app.listen(PORT, () => {
  console.log(`Mammon dashboard backend running on port ${PORT}`);
});
