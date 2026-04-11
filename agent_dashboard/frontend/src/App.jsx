import { useState, useEffect, useCallback } from 'react';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine,
} from 'recharts';

// ── Design tokens ─────────────────────────────────────────────────────────────

const D_COLOR  = '#58A6FF';
const D_BG     = 'rgba(88,166,255,0.08)';
const D_BORDER = 'rgba(88,166,255,0.3)';

// ── KPI Card ──────────────────────────────────────────────────────────────────

function KpiCard({ data }) {
  const roe    = parseFloat(data?.daily_roe ?? 0);
  const trades = data?.daily_trades ?? 0;

  return (
    <div
      className="flex-1 min-w-[220px] rounded-lg p-5 border transition-all"
      style={{ background: D_BG, borderColor: D_BORDER }}
    >
      <div className="flex items-center justify-between mb-3">
        <span className="text-xs font-semibold uppercase tracking-widest" style={{ color: '#8B949E' }}>
          Engine D
        </span>
        <span
          className="text-xs font-bold px-2 py-0.5 rounded-full"
          style={{ background: `${D_COLOR}22`, color: D_COLOR }}
        >
          HFT Market Maker
        </span>
      </div>
      <div className="flex items-end gap-6">
        <div>
          <div className="text-3xl font-bold" style={{ color: D_COLOR }}>{trades}</div>
          <div className="text-xs mt-1" style={{ color: '#8B949E' }}>Trades (24h)</div>
        </div>
        <div>
          <div
            className="text-2xl font-bold"
            style={{ color: roe >= 0 ? '#3FB950' : '#F85149' }}
          >
            {roe >= 0 ? '+' : ''}{roe.toFixed(4)}%
          </div>
          <div className="text-xs mt-1" style={{ color: '#8B949E' }}>ROE (24h)</div>
        </div>
      </div>
    </div>
  );
}

// ── Chart Tooltip ─────────────────────────────────────────────────────────────

function ChartTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null;
  return (
    <div
      className="rounded border p-3 text-xs"
      style={{ background: '#161B22', borderColor: '#30363D' }}
    >
      <div className="mb-2" style={{ color: '#8B949E' }}>
        {new Date(label).toLocaleTimeString()}
      </div>
      {payload.map((p) => (
        <div key={p.dataKey} className="flex items-center gap-2 mb-1">
          <span className="w-2 h-2 rounded-full inline-block" style={{ background: p.color }} />
          <span style={{ color: p.color }}>Engine D:</span>
          <span style={{ color: '#C9D1D9' }}>{parseFloat(p.value).toFixed(4)}%</span>
        </div>
      ))}
    </div>
  );
}

// ── Trade Row ─────────────────────────────────────────────────────────────────

function TradeRow({ trade }) {
  const netPnl = parseFloat(trade.net_pnl ?? 0);
  const roe    = parseFloat(trade.trade_roe_pct ?? 0);
  const signal = parseFloat(trade.entry_signal_value ?? 0);
  const size   = parseFloat(trade.trade_size_idr ?? 0);

  return (
    <tr className="border-b" style={{ background: D_BG, borderColor: '#30363D' }}>
      <td className="px-4 py-2">
        <span
          className="text-xs font-bold px-2 py-0.5 rounded"
          style={{ background: `${D_COLOR}22`, color: D_COLOR }}
        >
          D
        </span>
      </td>
      <td className="px-4 py-2 text-xs" style={{ color: '#C9D1D9' }}>
        {trade.asset_pair || '—'}
      </td>
      <td className="px-4 py-2 text-xs font-mono" style={{ color: '#8B949E' }}>
        {signal.toFixed(4)}
      </td>
      <td className="px-4 py-2 text-xs" style={{ color: '#C9D1D9' }}>
        {size.toLocaleString()}
      </td>
      <td
        className="px-4 py-2 text-xs font-bold"
        style={{ color: netPnl >= 0 ? '#3FB950' : '#F85149' }}
      >
        {netPnl >= 0 ? '+' : ''}{netPnl.toFixed(4)}
      </td>
      <td
        className="px-4 py-2 text-xs font-bold"
        style={{ color: roe >= 0 ? '#3FB950' : '#F85149' }}
      >
        {roe >= 0 ? '+' : ''}{roe.toFixed(4)}%
      </td>
      <td className="px-4 py-2 text-xs" style={{ color: '#8B949E' }}>
        {new Date(trade.timestamp).toLocaleTimeString()}
      </td>
    </tr>
  );
}

// ── App ───────────────────────────────────────────────────────────────────────

export default function App() {
  const [kpis,       setKpis]       = useState({ D: null });
  const [trades,     setTrades]     = useState([]);
  const [chartData,  setChartData]  = useState([]);
  const [lastUpdate, setLastUpdate] = useState(null);
  const [error,      setError]      = useState(null);

  const fetchAll = useCallback(async () => {
    try {
      const [kpisRes, tradesRes, chartRes] = await Promise.all([
        fetch('/api/kpis'),
        fetch('/api/trades'),
        fetch('/api/chart'),
      ]);

      if (!kpisRes.ok || !tradesRes.ok || !chartRes.ok) {
        throw new Error(`API error (${kpisRes.status})`);
      }

      const [kpisData, tradesData, chartRaw] = await Promise.all([
        kpisRes.json(),
        tradesRes.json(),
        chartRes.json(),
      ]);

      setKpis(kpisData);
      setTrades(tradesData);
      setChartData(chartRaw);
      setLastUpdate(new Date());
      setError(null);
    } catch (e) {
      setError(e.message);
    }
  }, []);

  useEffect(() => {
    fetchAll();
    const id = setInterval(fetchAll, 5000);
    return () => clearInterval(id);
  }, [fetchAll]);

  return (
    <div className="min-h-screen p-6" style={{ background: '#0D1117' }}>

      {/* ── Header ── */}
      <div className="flex flex-wrap items-center justify-between gap-4 mb-8">
        <div>
          <h1
            className="text-2xl font-bold tracking-widest uppercase"
            style={{ color: D_COLOR }}
          >
            Project Mammon
          </h1>
          <p className="text-xs mt-1" style={{ color: '#8B949E' }}>
            Engine D &mdash; Avellaneda-Stoikov HFT Market Maker
          </p>
        </div>
        <div className="flex items-center gap-3">
          {error ? (
            <span
              className="text-xs px-3 py-1 rounded-full"
              style={{ background: 'rgba(248,81,73,0.15)', color: '#F85149' }}
            >
              &#9888; {error}
            </span>
          ) : (
            <span
              className="text-xs flex items-center gap-2 px-3 py-1 rounded-full"
              style={{ background: 'rgba(63,185,80,0.1)', color: '#3FB950' }}
            >
              <span
                className="w-2 h-2 rounded-full inline-block animate-pulse"
                style={{ background: '#3FB950' }}
              />
              LIVE
            </span>
          )}
          {lastUpdate && (
            <span className="text-xs" style={{ color: '#8B949E' }}>
              {lastUpdate.toLocaleTimeString()}
            </span>
          )}
        </div>
      </div>

      {/* ── KPI Card ── */}
      <div className="flex flex-wrap gap-4 mb-8">
        <KpiCard data={kpis.D} />
      </div>

      {/* ── ROE Chart ── */}
      <div
        className="rounded-lg border p-6 mb-8"
        style={{ background: '#161B22', borderColor: '#30363D' }}
      >
        <h2
          className="text-xs font-semibold uppercase tracking-widest mb-4"
          style={{ color: '#8B949E' }}
        >
          Cumulative ROE % &mdash; Engine D (24h)
        </h2>
        {chartData.length === 0 ? (
          <div
            className="h-48 flex items-center justify-center text-sm"
            style={{ color: '#8B949E' }}
          >
            Awaiting trade data&hellip;
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={chartData} margin={{ top: 5, right: 20, left: 0, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#21262D" />
              <XAxis
                dataKey="time"
                tickFormatter={(t) =>
                  new Date(t).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
                }
                tick={{ fill: '#8B949E', fontSize: 10 }}
                axisLine={{ stroke: '#30363D' }}
                tickLine={false}
              />
              <YAxis
                tickFormatter={(v) => `${parseFloat(v).toFixed(2)}%`}
                tick={{ fill: '#8B949E', fontSize: 10 }}
                axisLine={{ stroke: '#30363D' }}
                tickLine={false}
              />
              <Tooltip content={<ChartTooltip />} />
              <ReferenceLine y={0} stroke="#30363D" strokeDasharray="4 4" />
              <Line
                type="monotone"
                dataKey="D"
                stroke={D_COLOR}
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 4, fill: D_COLOR }}
              />
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* ── Trades Table ── */}
      <div
        className="rounded-lg border overflow-hidden"
        style={{ background: '#161B22', borderColor: '#30363D' }}
      >
        <div className="px-6 py-4 border-b" style={{ borderColor: '#30363D' }}>
          <h2
            className="text-xs font-semibold uppercase tracking-widest"
            style={{ color: '#8B949E' }}
          >
            Latest Executions (50)
          </h2>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-left" style={{ minWidth: '640px' }}>
            <thead>
              <tr className="border-b" style={{ borderColor: '#30363D' }}>
                {['Engine', 'Pair', 'Signal', 'Size', 'Net PnL', 'ROE %', 'Time'].map((h) => (
                  <th
                    key={h}
                    className="px-4 py-3 text-xs font-semibold uppercase tracking-wider"
                    style={{ color: '#8B949E' }}
                  >
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {trades.length === 0 ? (
                <tr>
                  <td
                    colSpan={7}
                    className="px-4 py-10 text-center text-sm"
                    style={{ color: '#8B949E' }}
                  >
                    No trades logged yet. Engine D is warming up&hellip;
                  </td>
                </tr>
              ) : (
                trades.map((trade) => (
                  <TradeRow key={trade.trade_id} trade={trade} />
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* ── Footer ── */}
      <div className="mt-8 text-center text-xs" style={{ color: '#30363D' }}>
        Project Mammon &mdash; Engine D HFT &mdash; Agent Q v3 Hybrid Optimizer
      </div>
    </div>
  );
}
