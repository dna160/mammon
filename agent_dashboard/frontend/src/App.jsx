/**
 * Project Mammon V2 — Live HFT Dashboard
 *
 * Panels:
 *   1. Header          — system status, LIVE indicator, WS connection
 *   2. Symbol Strips   — per-coin: regime badge, ping-pong state, micro-price
 *   3. Agent Q Panel   — state vector, latest params, RAG memory
 *   4. Holdings Panel  — inventory per coin with notional + live PnL
 *   5. Order Queue     — active bid/ask orders per symbol
 *   6. PnL Chart       — cumulative USD PnL over 24h
 *   7. Trade Log       — scrolling execution table (last 100 fills)
 *   8. Reward History  — agent_q_memory table (self-learning scorecard)
 *
 * Data flow: WebSocket push from backend every 1 second.
 * Fallback:  REST polling every 5 seconds if WS drops.
 */

import { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine, AreaChart, Area,
} from 'recharts';

// ── Design tokens ─────────────────────────────────────────────────────────────
const C = {
  bg:         '#0D1117',
  surface:    '#161B22',
  surface2:   '#1C2128',
  border:     '#30363D',
  text:       '#C9D1D9',
  muted:      '#8B949E',
  blue:       '#58A6FF',
  green:      '#3FB950',
  red:        '#F85149',
  amber:      '#E3B341',
  purple:     '#BC8CFF',
  teal:       '#39D353',
  orange:     '#F0883E',
};

const REGIME_META = {
  MEAN_REVERTING:               { color: C.blue,   label: 'MEAN REV',  short: 'MR' },
  RETAIL_FRENZY_UP:             { color: C.green,  label: 'FRENZY UP', short: 'RFU' },
  INSTITUTIONAL_ABSORPTION_DOWN:{ color: C.red,    label: 'INST↓',     short: 'IAD' },
  DEAD_ZONE:                    { color: C.muted,  label: 'DEAD ZONE', short: 'DZ' },
  TOXIC_LIQUIDATION_CASCADE:    { color: '#FF0000',label: '⚠ TOXIC',   short: 'TLC' },
  UNKNOWN:                      { color: C.muted,  label: 'NO DATA',   short: '?' },
};

const COIN_META = {
  SOLFDUSD:  { coin: 'SOL',  color: '#9945FF' },
  XRPFDUSD:  { coin: 'XRP',  color: '#00AAE4' },
  DOGEFDUSD: { coin: 'DOGE', color: '#C3A634' },
  ETHFDUSD:  { coin: 'ETH',  color: '#627EEA' },
  BNBFDUSD:  { coin: 'BNB',  color: '#F3BA2F' },
};

const pp = (n, d = 4) => (isNaN(n) ? '—' : parseFloat(n).toFixed(d));
const ppm = (n, d = 2) => {
  const v = parseFloat(n);
  if (isNaN(v)) return '—';
  return (v >= 0 ? '+' : '') + v.toFixed(d);
};
const fmtPrice = (n) => {
  const v = parseFloat(n);
  if (isNaN(v) || v === 0) return '—';
  if (v >= 1)   return v.toFixed(4);
  if (v >= 0.01) return v.toFixed(5);
  return v.toFixed(6);
};
const fmtTime = (ts) => {
  if (!ts) return '—';
  return new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
};
const fmtMs = (ms) => {
  if (!ms) return '—';
  return new Date(ms).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
};

// ── Reusable components ───────────────────────────────────────────────────────

function Panel({ title, children, className = '', action }) {
  return (
    <div
      className={`rounded-lg border flex flex-col ${className}`}
      style={{ background: C.surface, borderColor: C.border }}
    >
      <div
        className="flex items-center justify-between px-4 py-3 border-b shrink-0"
        style={{ borderColor: C.border }}
      >
        <span className="text-xs font-semibold uppercase tracking-widest" style={{ color: C.muted }}>
          {title}
        </span>
        {action}
      </div>
      <div className="flex-1 overflow-hidden">
        {children}
      </div>
    </div>
  );
}

function Badge({ label, color, size = 'sm' }) {
  const sz = size === 'xs' ? 'text-[9px] px-1.5 py-0.5' : 'text-xs px-2 py-0.5';
  return (
    <span
      className={`font-bold rounded-sm ${sz} inline-block`}
      style={{ background: `${color}22`, color, border: `1px solid ${color}44` }}
    >
      {label}
    </span>
  );
}

function PingPongBadge({ state }) {
  // state: 0 = Empty/Bidding, 1 = Loaded/Asking, null = unknown
  if (state === 0)  return <Badge label="STATE 0 · BIDDING" color={C.blue}  size="xs" />;
  if (state === 1)  return <Badge label="STATE 1 · LOADED"  color={C.amber} size="xs" />;
  return <Badge label="WARMING UP" color={C.muted} size="xs" />;
}

function Stat({ label, value, color }) {
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-[10px] uppercase tracking-wider" style={{ color: C.muted }}>{label}</span>
      <span className="text-sm font-mono font-bold" style={{ color: color ?? C.text }}>{value}</span>
    </div>
  );
}

// ── Chart Tooltip ─────────────────────────────────────────────────────────────

function PnlTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null;
  const v = parseFloat(payload[0]?.value ?? 0);
  return (
    <div className="rounded border p-3 text-xs" style={{ background: C.surface2, borderColor: C.border }}>
      <div className="mb-1" style={{ color: C.muted }}>{fmtTime(label)}</div>
      <div style={{ color: v >= 0 ? C.green : C.red }}>
        Cumulative PnL: {ppm(v, 4)} USD
      </div>
    </div>
  );
}

// ── Symbol Strip ─────────────────────────────────────────────────────────────

function SymbolStrip({ symbol, holding, order, agentQ }) {
  const meta    = COIN_META[symbol]   ?? { coin: symbol, color: C.blue };
  const regime  = agentQ?.regime?.regime ?? 'UNKNOWN';
  const regMeta = REGIME_META[regime] ?? REGIME_META.UNKNOWN;
  const params  = agentQ?.params;
  const status  = params?.system_status ?? '—';
  const isLive  = status === 'LIVE';
  const isSafe  = status === 'SAFE_MODE_LOCKDOWN';

  const price   = holding?.micro_price ?? 0;
  const inv     = holding?.inventory_coin ?? 0;
  const pnl     = order?.real_pnl_usd ?? 0;
  const pp_st   = order?.ping_pong ?? null;

  // Determine ping-pong state from order data
  const ppState = order
    ? (order.bid_id && !order.ask_id ? 0 : order.ask_id && !order.bid_id ? 1 : null)
    : null;

  return (
    <div
      className="flex flex-wrap items-center gap-3 px-4 py-2 border-b"
      style={{ borderColor: C.border, background: C.surface }}
    >
      {/* Coin identity */}
      <div className="flex items-center gap-2 w-24">
        <span
          className="w-7 h-7 rounded-full flex items-center justify-center text-xs font-black shrink-0"
          style={{ background: meta.color + '22', color: meta.color, border: `1px solid ${meta.color}44` }}
        >
          {meta.coin[0]}
        </span>
        <div>
          <div className="text-xs font-bold" style={{ color: C.text }}>{meta.coin}</div>
          <div className="text-[10px]" style={{ color: C.muted }}>{symbol.replace('FDUSD', '')}/FDUSD</div>
        </div>
      </div>

      {/* Price */}
      <div className="flex flex-col w-24">
        <span className="text-[10px]" style={{ color: C.muted }}>MICRO PRICE</span>
        <span className="text-sm font-mono font-bold" style={{ color: C.amber }}>
          ${fmtPrice(price)}
        </span>
      </div>

      {/* Inventory */}
      <div className="flex flex-col w-24">
        <span className="text-[10px]" style={{ color: C.muted }}>INVENTORY</span>
        <span className="text-sm font-mono font-bold" style={{ color: Math.abs(inv) > 0 ? C.blue : C.muted }}>
          {inv !== 0 ? pp(inv, 4) : '—'} {meta.coin}
        </span>
      </div>

      {/* Live PnL */}
      <div className="flex flex-col w-24">
        <span className="text-[10px]" style={{ color: C.muted }}>ENGINE PnL</span>
        <span className="text-sm font-mono font-bold" style={{ color: pnl >= 0 ? C.green : C.red }}>
          {ppm(pnl, 2)} USD
        </span>
      </div>

      {/* Regime */}
      <div className="flex items-center gap-1">
        <Badge label={regMeta.label} color={regMeta.color} size="xs" />
        {params && <span className="text-[10px]" style={{ color: C.muted }}>
          γ={pp(params.gamma, 2)} s={pp(params.min_spread_ticks, 1)}
        </span>}
      </div>

      {/* Ping-pong state */}
      <PingPongBadge state={ppState} />

      {/* System status */}
      <div className="ml-auto">
        {isSafe ? (
          <Badge label="⚠ SAFE MODE" color={C.red} size="xs" />
        ) : isLive ? (
          <Badge label="● LIVE" color={C.green} size="xs" />
        ) : (
          <Badge label={status} color={C.muted} size="xs" />
        )}
      </div>
    </div>
  );
}

// ── Agent Q Panel ─────────────────────────────────────────────────────────────

function AgentQPanel({ agentQ, rewardHistory }) {
  const [activeTab, setActiveTab] = useState(Object.keys(COIN_META)[0]);

  const data  = agentQ?.[activeTab];
  const sv    = data?.state_vector;
  const params = data?.params;
  const regime = data?.regime?.regime ?? 'UNKNOWN';
  const regMeta = REGIME_META[regime] ?? REGIME_META.UNKNOWN;

  const symHistory = useMemo(
    () => (rewardHistory ?? []).filter((r) => r.symbol === activeTab).slice(0, 8),
    [rewardHistory, activeTab],
  );

  // T-1 memory: most recent evaluated row (has reward_score set)
  const lastEvaluated = useMemo(
    () => symHistory.find((r) => r.evaluated_at != null && r.reward_score != null),
    [symHistory],
  );

  const bestAction  = symHistory.filter((r) => r.reward_score > 0).sort((a,b) => b.reward_score - a.reward_score)[0];
  const worstAction = symHistory.filter((r) => r.reward_score < 0).sort((a,b) => a.reward_score - b.reward_score)[0];

  const SvRow = ({ label, value, label2, color }) => (
    <div className="flex items-center justify-between py-1.5 border-b" style={{ borderColor: C.border }}>
      <span className="text-xs" style={{ color: C.muted }}>{label}</span>
      <div className="flex items-center gap-2">
        <span className="text-xs font-mono font-semibold" style={{ color: C.text }}>{value}</span>
        {label2 && (
          <span
            className="text-[10px] font-bold px-1.5 py-0.5 rounded"
            style={{
              color,
              background: `${color}22`,
              border: `1px solid ${color}44`,
            }}
          >
            {label2}
          </span>
        )}
      </div>
    </div>
  );

  return (
    <Panel title="Agent Q — Intelligence">
      {/* Tabs */}
      <div className="flex border-b overflow-x-auto shrink-0" style={{ borderColor: C.border }}>
        {Object.entries(COIN_META).map(([sym, m]) => (
          <button
            key={sym}
            onClick={() => setActiveTab(sym)}
            className="px-3 py-2 text-xs font-semibold whitespace-nowrap transition-colors"
            style={{
              color:       activeTab === sym ? m.color : C.muted,
              borderBottom: activeTab === sym ? `2px solid ${m.color}` : '2px solid transparent',
              background:   activeTab === sym ? `${m.color}11` : 'transparent',
            }}
          >
            {m.coin}
          </button>
        ))}
      </div>

      <div className="overflow-y-auto p-4 flex flex-col gap-4" style={{ maxHeight: 480 }}>

        {/* Regime + Params header */}
        <div className="flex items-center justify-between">
          <Badge label={regMeta.label} color={regMeta.color} />
          {data?.regime?.confidence != null && (
            <span className="text-[10px]" style={{ color: C.muted }}>
              conf {(data.regime.confidence * 100).toFixed(0)}%
            </span>
          )}
        </div>

        {/* Live params */}
        {params ? (
          <div className="grid grid-cols-3 gap-3">
            <Stat label="γ (Gamma)"    value={pp(params.gamma, 3)}            color={C.blue} />
            <Stat label="Min Spread"   value={`${pp(params.min_spread_ticks, 1)} tks`} color={C.amber} />
            <Stat label="TFI Threshold" value={`$${parseFloat(params.tfi_threshold ?? 0).toLocaleString()}`} color={C.purple} />
          </div>
        ) : (
          <div className="text-xs" style={{ color: C.muted }}>No params published yet.</div>
        )}

        {/* T-1 RL Memory */}
        <div
          className="rounded p-3 border"
          style={{ background: C.surface2, borderColor: C.border }}
        >
          <div className="text-[10px] uppercase tracking-widest mb-2" style={{ color: C.muted }}>
            T-1 RL Memory — Cadence Feedback
          </div>
          {lastEvaluated ? (
            <div className="flex flex-wrap gap-4">
              <div className="flex flex-col gap-0.5">
                <span className="text-[10px]" style={{ color: C.muted }}>REWARD</span>
                <span className="text-sm font-mono font-bold"
                  style={{ color: parseFloat(lastEvaluated.reward_score) >= 0 ? C.green : C.red }}>
                  {ppm(lastEvaluated.reward_score, 2)}
                </span>
              </div>
              <div className="flex flex-col gap-0.5">
                <span className="text-[10px]" style={{ color: C.muted }}>ROUND TRIPS</span>
                <span className="text-sm font-mono font-bold"
                  style={{ color: (lastEvaluated.total_round_trips ?? 0) >= 100 ? C.green : C.red }}>
                  {lastEvaluated.total_round_trips ?? 0}
                  <span className="text-[10px] font-normal" style={{ color: C.muted }}>/100</span>
                </span>
              </div>
              <div className="flex flex-col gap-0.5">
                <span className="text-[10px]" style={{ color: C.muted }}>WIN RATE</span>
                <span className="text-sm font-mono font-bold"
                  style={{ color: (lastEvaluated.win_rate_pct ?? 0) >= 50 ? C.green : C.amber }}>
                  {pp(lastEvaluated.win_rate_pct, 1)}%
                </span>
              </div>
              <div className="flex flex-col gap-0.5">
                <span className="text-[10px]" style={{ color: C.muted }}>NET PnL</span>
                <span className="text-sm font-mono font-bold"
                  style={{ color: parseFloat(lastEvaluated.net_pnl ?? 0) >= 0 ? C.green : C.red }}>
                  {ppm(lastEvaluated.net_pnl, 4)}
                </span>
              </div>
            </div>
          ) : (
            <div className="text-xs" style={{ color: C.muted }}>
              No evaluated cycles yet — first reward computed at T+15min.
            </div>
          )}
          {params?.cro_reasoning && (
            <div className="mt-2 text-[10px] italic border-t pt-2" style={{ color: C.muted, borderColor: C.border }}>
              CRO: {params.cro_reasoning}
            </div>
          )}
        </div>

        {/* State Vector */}
        {sv ? (
          <div>
            <div className="text-[10px] uppercase tracking-widest mb-2" style={{ color: C.muted }}>
              5M State Vector · {new Date(sv.ts ?? 0).toLocaleTimeString()}
            </div>
            <SvRow
              label="1. Micro-Volatility"
              value={`${pp(sv.vol_bps, 2)} bps/s`}
              label2={sv.vol_label}
              color={sv.vol_label === 'EXTREME' ? C.red : sv.vol_label === 'HIGH' ? C.amber : C.green}
            />
            <SvRow
              label="2. Order Flow Toxicity"
              value={`${ppm(sv.tfi_zscore, 2)}σ`}
              label2={sv.tfi_label}
              color={sv.tfi_label?.includes('TOXIC') ? C.red : C.green}
            />
            <SvRow
              label="3. Market Drift"
              value={`${ppm(sv.drift_bps, 2)} bps/5m`}
              label2={sv.drift_label}
              color={sv.drift_label?.includes('UP') ? C.green : sv.drift_label?.includes('DOWN') ? C.red : C.muted}
            />
            <SvRow
              label="4. Native LOB Spread"
              value={`${pp(sv.native_spread, 1)} ticks`}
              label2={null}
              color={C.text}
            />
            <SvRow
              label="5. Adverse Selection"
              value={`${pp(sv.adverse_pct, 1)}%`}
              label2={sv.adverse_label?.includes('DANGER') ? 'DANGER' : 'SAFE'}
              color={sv.adverse_pct > 60 ? C.red : sv.adverse_pct > 40 ? C.amber : C.green}
            />
          </div>
        ) : (
          <div className="text-xs" style={{ color: C.muted }}>
            State vector not yet computed. Watcher warming up…
          </div>
        )}

        {/* RAG Memory */}
        {symHistory.length > 0 && (
          <div>
            <div className="text-[10px] uppercase tracking-widest mb-2" style={{ color: C.muted }}>
              RAG Memory — {regime}
            </div>
            {bestAction && (
              <div
                className="text-xs rounded p-2 mb-2 border"
                style={{ background: `${C.green}11`, borderColor: `${C.green}44` }}
              >
                <div className="font-bold mb-0.5" style={{ color: C.green }}>
                  REWARDED: γ={pp(bestAction.proposed_gamma, 2)} spread={pp(bestAction.proposed_min_spread, 1)}
                  {' '}→ {ppm(bestAction.net_pnl, 2)} USD (AQ {pp(bestAction.adverse_selection, 0)}%)
                </div>
                {bestAction.alpha_reasoning && (
                  <div className="italic" style={{ color: C.muted }}>{bestAction.alpha_reasoning}</div>
                )}
              </div>
            )}
            {worstAction && (
              <div
                className="text-xs rounded p-2 border"
                style={{ background: `${C.red}11`, borderColor: `${C.red}44` }}
              >
                <div className="font-bold mb-0.5" style={{ color: C.red }}>
                  PUNISHED: γ={pp(worstAction.proposed_gamma, 2)} spread={pp(worstAction.proposed_min_spread, 1)}
                  {' '}→ {ppm(worstAction.net_pnl, 2)} USD (AQ {pp(worstAction.adverse_selection, 0)}%)
                </div>
                {worstAction.alpha_reasoning && (
                  <div className="italic" style={{ color: C.muted }}>{worstAction.alpha_reasoning}</div>
                )}
              </div>
            )}
          </div>
        )}
      </div>
    </Panel>
  );
}

// ── Holdings Panel ────────────────────────────────────────────────────────────

function HoldingsPanel({ holdings }) {
  const rows = Object.entries(COIN_META).map(([sym, meta]) => {
    const h        = holdings?.[sym];
    // Use real_inventory (exchange-reconciled) when available, fall back to shadow ledger
    const realInv  = h?.real_inventory;
    const shadowInv = h?.inventory_coin ?? 0;
    const inv      = realInv !== null && realInv !== undefined ? realInv : shadowInv;
    const isReal   = realInv !== null && realInv !== undefined;
    const price    = h?.micro_price ?? 0;
    const notional = inv * price;
    return { sym, meta, h, inv, shadowInv, isReal, price, notional };
  });

  const totalNotional = rows.reduce((s, r) => s + Math.abs(r.notional), 0);

  return (
    <Panel title="Holdings — Live Inventory">
      <div className="overflow-y-auto" style={{ maxHeight: 300 }}>
        {rows.map(({ sym, meta, h, inv, shadowInv, isReal, price, notional }) => (
          <div
            key={sym}
            className="flex items-center gap-3 px-4 py-3 border-b"
            style={{ borderColor: C.border }}
          >
            <div
              className="w-8 h-8 rounded-full flex items-center justify-center text-xs font-black shrink-0"
              style={{ background: meta.color + '22', color: meta.color }}
            >
              {meta.coin[0]}
            </div>
            <div className="flex-1">
              <div className="flex items-center justify-between mb-0.5">
                <div className="flex items-center gap-1.5">
                  <span className="text-xs font-bold" style={{ color: C.text }}>{meta.coin}</span>
                  {h?.decision && (
                    <Badge
                      label={h.decision}
                      color={h.decision === 'SKEW-ASK' ? C.amber : h.decision === 'SKEW-BID' ? C.green : h.decision === 'REQUOTE' ? C.blue : C.muted}
                      size="xs"
                    />
                  )}
                </div>
                <div className="text-right">
                  <div
                    className="text-sm font-mono font-bold"
                    style={{ color: Math.abs(inv) > 0 ? C.blue : C.muted }}
                  >
                    {Math.abs(inv) > 0 ? pp(inv, 6) : '—'} {meta.coin}
                  </div>
                  {!isReal && Math.abs(shadowInv) > 0 && (
                    <div className="text-[9px]" style={{ color: C.amber }}>shadow ledger</div>
                  )}
                  {isReal && (
                    <div className="text-[9px]" style={{ color: C.green }}>exchange ✓</div>
                  )}
                </div>
              </div>
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <span className="text-[10px] font-mono" style={{ color: C.muted }}>
                    mid ${fmtPrice(price)}
                  </span>
                  {h?.obi != null && (
                    <span className="text-[10px] font-mono" style={{ color: h.obi > 0 ? C.green : C.red }}>
                      OBI {ppm(h.obi, 3)}
                    </span>
                  )}
                </div>
                <span
                  className="text-[11px] font-mono font-semibold"
                  style={{ color: Math.abs(notional) > 0 ? C.amber : C.muted }}
                >
                  {Math.abs(notional) > 0 ? `$${Math.abs(notional).toFixed(3)}` : '—'}
                </span>
              </div>
              {/* Engine PnL row */}
              {h && (
                <div className="flex items-center justify-between mt-0.5">
                  <span className="text-[10px]" style={{ color: C.muted }}>
                    σ² {h.variance?.toExponential(2) ?? '—'}
                  </span>
                  <span className="text-[10px] font-mono" style={{ color: (h.real_pnl_usd ?? h.pnl_usd) >= 0 ? C.green : C.red }}>
                    PnL {ppm(h.real_pnl_usd ?? h.pnl_usd, 4)} USD
                  </span>
                </div>
              )}
            </div>
          </div>
        ))}
        {/* Totals row */}
        <div className="flex items-center justify-between px-4 py-3" style={{ background: C.surface2 }}>
          <span className="text-xs font-semibold" style={{ color: C.muted }}>TOTAL NOTIONAL</span>
          <span className="text-sm font-mono font-bold" style={{ color: C.amber }}>
            ${totalNotional.toFixed(3)}
          </span>
        </div>
      </div>
    </Panel>
  );
}

// ── Order Queue Panel ─────────────────────────────────────────────────────────

function OrderQueuePanel({ orders, holdings }) {
  const rows = Object.entries(COIN_META).map(([sym, meta]) => {
    // Orders are now derived from holdings pipeline data (open_bid/open_ask)
    const h = holdings?.[sym];
    const openBid = h?.open_bid   ?? null;
    const openAsk = h?.open_ask   ?? null;
    const optBid  = h?.optimal_bid ?? null;
    const optAsk  = h?.optimal_ask ?? null;
    const hasBid  = openBid !== null;
    const hasAsk  = openAsk !== null;
    const ppState = h?.ping_pong ?? null;
    const decision = h?.decision ?? '—';
    return { sym, meta, h, openBid, openAsk, optBid, optAsk, hasBid, hasAsk, ppState, decision };
  });

  return (
    <Panel title="Order Queue — Active Bids / Asks">
      <div className="overflow-y-auto" style={{ maxHeight: 300 }}>
        {rows.map(({ sym, meta, h, openBid, openAsk, optBid, optAsk, hasBid, hasAsk, ppState, decision }) => (
          <div
            key={sym}
            className="px-4 py-3 border-b"
            style={{ borderColor: C.border }}
          >
            <div className="flex items-center justify-between mb-2">
              <div className="flex items-center gap-2">
                <span className="text-xs font-bold" style={{ color: meta.color }}>{meta.coin}</span>
                <PingPongBadge state={ppState} />
              </div>
              <span className="text-[10px] font-mono" style={{ color: C.muted }}>
                {h?.total_trades ? `${h.total_trades} fills` : '0 fills'}
              </span>
            </div>

            <div className="grid grid-cols-2 gap-3">
              {/* BID side */}
              <div
                className="rounded p-2"
                style={{ background: hasBid ? `${C.green}11` : C.surface2, border: `1px solid ${hasBid ? C.green + '44' : C.border}` }}
              >
                <div className="text-[10px] font-semibold mb-1" style={{ color: hasBid ? C.green : C.muted }}>
                  {hasBid ? '● BID (LIVE)' : '○ BID (NONE)'}
                </div>
                <div className="text-sm font-mono font-bold" style={{ color: hasBid ? C.green : C.muted }}>
                  {hasBid ? `$${fmtPrice(openBid)}` : '—'}
                </div>
                {optBid && (
                  <div className="text-[9px] font-mono mt-0.5" style={{ color: C.muted }}>
                    AS model: ${fmtPrice(optBid)}
                  </div>
                )}
              </div>

              {/* ASK side */}
              <div
                className="rounded p-2"
                style={{ background: hasAsk ? `${C.red}11` : C.surface2, border: `1px solid ${hasAsk ? C.red + '44' : C.border}` }}
              >
                <div className="text-[10px] font-semibold mb-1" style={{ color: hasAsk ? C.red : C.muted }}>
                  {hasAsk ? '● ASK (LIVE)' : '○ ASK (NONE)'}
                </div>
                <div className="text-sm font-mono font-bold" style={{ color: hasAsk ? C.red : C.muted }}>
                  {hasAsk ? `$${fmtPrice(openAsk)}` : '—'}
                </div>
                {optAsk && (
                  <div className="text-[9px] font-mono mt-0.5" style={{ color: C.muted }}>
                    AS model: ${fmtPrice(optAsk)}
                  </div>
                )}
              </div>
            </div>

            {/* Spread row */}
            {hasBid && hasAsk && (
              <div className="mt-2 text-[10px] font-mono" style={{ color: C.purple }}>
                spread: {fmtPrice(Math.abs((openAsk ?? 0) - (openBid ?? 0)))}
              </div>
            )}
          </div>
        ))}
      </div>
    </Panel>
  );
}

// ── PnL Chart ─────────────────────────────────────────────────────────────────

function PnlChart({ chart }) {
  if (!chart?.length) {
    return (
      <Panel title="Cumulative PnL — 24h (USD)">
        <div className="h-48 flex items-center justify-center text-sm" style={{ color: C.muted }}>
          Awaiting trade data…
        </div>
      </Panel>
    );
  }

  const latest = chart[chart.length - 1]?.cumulative_pnl ?? 0;
  const isPos  = latest >= 0;

  return (
    <Panel
      title="Cumulative PnL — 24h (USD)"
      action={
        <span className="text-sm font-bold font-mono" style={{ color: isPos ? C.green : C.red }}>
          {ppm(latest, 4)} USD
        </span>
      }
    >
      <div className="px-4 py-4">
        <ResponsiveContainer width="100%" height={200}>
          <AreaChart data={chart} margin={{ top: 5, right: 10, left: 0, bottom: 5 }}>
            <defs>
              <linearGradient id="pnlGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%"  stopColor={isPos ? C.green : C.red} stopOpacity={0.3} />
                <stop offset="95%" stopColor={isPos ? C.green : C.red} stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#21262D" />
            <XAxis
              dataKey="time"
              tickFormatter={(t) => new Date(t).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
              tick={{ fill: C.muted, fontSize: 9 }}
              axisLine={{ stroke: C.border }}
              tickLine={false}
            />
            <YAxis
              tickFormatter={(v) => `$${parseFloat(v).toFixed(2)}`}
              tick={{ fill: C.muted, fontSize: 9 }}
              axisLine={{ stroke: C.border }}
              tickLine={false}
              width={60}
            />
            <Tooltip content={<PnlTooltip />} />
            <ReferenceLine y={0} stroke={C.border} strokeDasharray="4 4" />
            <Area
              type="monotone"
              dataKey="cumulative_pnl"
              stroke={isPos ? C.green : C.red}
              strokeWidth={2}
              fill="url(#pnlGrad)"
              dot={false}
              activeDot={{ r: 4, fill: isPos ? C.green : C.red }}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </Panel>
  );
}

// ── KPI Bar ───────────────────────────────────────────────────────────────────

function KpiBar({ kpis }) {
  const k = kpis ?? {};
  return (
    <div
      className="flex flex-wrap gap-6 px-6 py-3 border-b"
      style={{ background: C.surface2, borderColor: C.border }}
    >
      <Stat label="Trades (24h)"      value={k.daily_trades ?? '—'} />
      <Stat label="Total Net PnL"     value={`${ppm(k.total_net_pnl, 4)} USD`}
            color={(k.total_net_pnl ?? 0) >= 0 ? C.green : C.red} />
      <Stat label="Avg PnL / Trade"   value={`${ppm(k.avg_pnl_per_trade, 4)} USD`}
            color={(k.avg_pnl_per_trade ?? 0) >= 0 ? C.green : C.red} />
      <Stat label="Win Rate (24h)"    value={`${pp(k.win_rate_pct, 1)}%`}
            color={(k.win_rate_pct ?? 0) >= 50 ? C.green : C.red} />
      <Stat label="Daily ROE"         value={`${ppm(k.daily_roe, 4)}%`}
            color={(k.daily_roe ?? 0) >= 0 ? C.green : C.red} />
    </div>
  );
}

// ── Trade Log ─────────────────────────────────────────────────────────────────

const TH = ['Time', 'Pair', 'Side', 'Fill Price', 'Qty', 'Notional', 'Fee', 'Net PnL', 'ROE %'];

function TradeRow({ trade }) {
  const net      = parseFloat(trade.net_pnl_usd  ?? 0);
  const notional = parseFloat(trade.notional_usd  ?? 0);
  const fee      = parseFloat(trade.fee_usd        ?? 0);
  const price    = parseFloat(trade.fill_price     ?? 0);
  const qty      = parseFloat(trade.fill_qty       ?? 0);
  const roe      = notional > 0 ? (net / notional) * 100 : 0;
  const meta     = COIN_META[trade.symbol] ?? { coin: trade.symbol, color: C.blue };
  const isBuy    = trade.side === 'BUY';

  return (
    <tr
      className="border-b hover:opacity-90 transition-opacity"
      style={{ borderColor: C.border }}
    >
      <td className="px-3 py-2 text-xs font-mono whitespace-nowrap" style={{ color: C.muted }}>
        {fmtTime(trade.timestamp)}
      </td>
      <td className="px-3 py-2">
        <span className="text-xs font-bold" style={{ color: meta.color }}>
          {meta.coin ?? trade.symbol}
        </span>
      </td>
      <td className="px-3 py-2">
        <span className="text-xs font-bold px-1.5 py-0.5 rounded"
          style={{
            background: isBuy ? `${C.green}22` : `${C.red}22`,
            color:      isBuy ? C.green         : C.red,
          }}>
          {trade.side ?? '—'}
        </span>
      </td>
      <td className="px-3 py-2 text-xs font-mono" style={{ color: C.text }}>
        {pp(price, 5)}
      </td>
      <td className="px-3 py-2 text-xs font-mono" style={{ color: C.muted }}>
        {pp(qty, 4)}
      </td>
      <td className="px-3 py-2 text-xs font-mono" style={{ color: C.text }}>
        {pp(notional, 4)}
      </td>
      <td className="px-3 py-2 text-xs font-mono" style={{ color: C.muted }}>
        {pp(fee, 4)}
      </td>
      <td className="px-3 py-2 text-xs font-mono font-bold"
        style={{ color: net >= 0 ? C.green : C.red }}>
        {isBuy ? '—' : ppm(net, 4)}
      </td>
      <td className="px-3 py-2 text-xs font-mono font-bold"
        style={{ color: roe >= 0 ? C.green : C.red }}>
        {isBuy ? '—' : `${ppm(roe, 3)}%`}
      </td>
    </tr>
  );
}

function TradeLog({ trades }) {
  return (
    <Panel title={`Live Trade Log — Engine D (${trades?.length ?? 0})`}>
      <div className="overflow-auto" style={{ maxHeight: 340 }}>
        <table className="w-full text-left" style={{ minWidth: 780 }}>
          <thead style={{ position: 'sticky', top: 0, background: C.surface2, zIndex: 1 }}>
            <tr>
              {TH.map((h) => (
                <th key={h} className="px-3 py-2 text-[10px] font-semibold uppercase tracking-wider"
                  style={{ color: C.muted, borderBottom: `1px solid ${C.border}` }}>
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {!trades?.length ? (
              <tr>
                <td colSpan={TH.length} className="px-3 py-10 text-center text-sm" style={{ color: C.muted }}>
                  No trades logged yet. Engine warming up…
                </td>
              </tr>
            ) : (
              trades.map((t) => <TradeRow key={t.trade_id} trade={t} />)
            )}
          </tbody>
        </table>
      </div>
    </Panel>
  );
}

// ── Reward History ────────────────────────────────────────────────────────────

function RewardHistory({ data }) {
  const RH = ['Time', 'Symbol', 'Regime', 'γ', 'Spread', 'TFI', 'Trips', 'Win%', 'Net PnL', 'AQ%', 'Score', 'Override'];

  return (
    <Panel title="Agent Q Memory — RAG Reward Scorecard">
      <div className="overflow-auto" style={{ maxHeight: 300 }}>
        <table className="w-full text-left" style={{ minWidth: 860 }}>
          <thead style={{ position: 'sticky', top: 0, background: C.surface2, zIndex: 1 }}>
            <tr>
              {RH.map((h) => (
                <th key={h} className="px-3 py-2 text-[10px] font-semibold uppercase tracking-wider"
                  style={{ color: C.muted, borderBottom: `1px solid ${C.border}` }}>
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {!data?.length ? (
              <tr>
                <td colSpan={RH.length} className="px-3 py-10 text-center text-sm" style={{ color: C.muted }}>
                  No agent_q_memory entries yet. First tactical cycle runs in 15 min…
                </td>
              </tr>
            ) : (
              data.map((r) => {
                const score   = parseFloat(r.reward_score ?? 0);
                const pnl     = parseFloat(r.net_pnl      ?? 0);
                const aq      = parseFloat(r.adverse_selection ?? 0);
                const pending = !r.evaluated_at;
                const regMeta = REGIME_META[r.regime] ?? REGIME_META.UNKNOWN;
                return (
                  <tr key={r.id} className="border-b" style={{ borderColor: C.border }}>
                    <td className="px-3 py-2 text-xs font-mono whitespace-nowrap" style={{ color: C.muted }}>
                      {fmtTime(r.timestamp)}
                    </td>
                    <td className="px-3 py-2 text-xs font-bold" style={{ color: COIN_META[r.symbol]?.color ?? C.blue }}>
                      {COIN_META[r.symbol]?.coin ?? r.symbol}
                    </td>
                    <td className="px-3 py-2">
                      <Badge label={regMeta.short} color={regMeta.color} size="xs" />
                    </td>
                    <td className="px-3 py-2 text-xs font-mono" style={{ color: C.text }}>
                      {pp(r.proposed_gamma, 2)}
                    </td>
                    <td className="px-3 py-2 text-xs font-mono" style={{ color: C.text }}>
                      {pp(r.proposed_min_spread, 1)}
                    </td>
                    <td className="px-3 py-2 text-xs font-mono" style={{ color: C.text }}>
                      {parseFloat(r.tfi_threshold ?? 0).toLocaleString()}
                    </td>
                    <td className="px-3 py-2 text-xs font-mono font-bold"
                      style={{ color: pending ? C.muted : (r.total_round_trips ?? 0) >= 100 ? C.green : C.red }}>
                      {pending ? '—' : (r.total_round_trips ?? 0)}
                    </td>
                    <td className="px-3 py-2 text-xs font-mono font-bold"
                      style={{ color: pending ? C.muted : (r.win_rate_pct ?? 0) >= 50 ? C.green : C.amber }}>
                      {pending ? '—' : `${pp(r.win_rate_pct, 1)}%`}
                    </td>
                    <td className="px-3 py-2 text-xs font-mono font-bold"
                      style={{ color: pending ? C.muted : pnl >= 0 ? C.green : C.red }}>
                      {pending ? 'PENDING…' : ppm(pnl, 4)}
                    </td>
                    <td className="px-3 py-2 text-xs font-mono"
                      style={{ color: pending ? C.muted : aq > 60 ? C.red : aq > 40 ? C.amber : C.green }}>
                      {pending ? '—' : `${pp(aq, 1)}%`}
                    </td>
                    <td className="px-3 py-2 text-xs font-mono font-bold"
                      style={{ color: pending ? C.muted : score >= 0 ? C.green : C.red }}>
                      {pending ? 'PENDING…' : ppm(score, 4)}
                    </td>
                    <td className="px-3 py-2">
                      {r.override_applied
                        ? <Badge label="OVERRIDDEN" color={C.amber} size="xs" />
                        : <span className="text-[10px]" style={{ color: C.muted }}>—</span>}
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </Panel>
  );
}

// ── App ───────────────────────────────────────────────────────────────────────

export default function App() {
  const [snapshot,    setSnapshot]    = useState(null);
  const [rewardHist,  setRewardHist]  = useState([]);
  const [wsStatus,    setWsStatus]    = useState('connecting'); // 'connecting' | 'live' | 'error'
  const [lastUpdate,  setLastUpdate]  = useState(null);
  const wsRef = useRef(null);

  // Reward history — low-frequency REST poll (every 30s)
  const fetchRewardHistory = useCallback(async () => {
    try {
      const res = await fetch('/api/reward-history?limit=50');
      if (res.ok) setRewardHist(await res.json());
    } catch (_) {}
  }, []);

  // REST fallback when WS is down
  const fetchSnapshot = useCallback(async () => {
    try {
      const res = await fetch('/api/snapshot');
      if (res.ok) {
        setSnapshot(await res.json());
        setLastUpdate(Date.now());
      }
    } catch (_) {}
  }, []);

  // WebSocket connection
  useEffect(() => {
    let reconnectTimer;

    function connect() {
      const proto = window.location.protocol === 'https:' ? 'wss' : 'ws';
      const ws    = new WebSocket(`${proto}://${window.location.host}/ws`);
      wsRef.current = ws;

      ws.onopen    = () => { setWsStatus('live'); console.log('[WS] Connected.'); };
      ws.onmessage = (e) => {
        try {
          const parsed = JSON.parse(e.data);
          setSnapshot(parsed);
          setLastUpdate(Date.now());
          // Reward history is now included in the snapshot push — update inline
          if (parsed.reward_history?.length) setRewardHist(parsed.reward_history);
        } catch (_) {}
      };
      ws.onerror = () => setWsStatus('error');
      ws.onclose = () => {
        setWsStatus('error');
        reconnectTimer = setTimeout(connect, 5000);
      };
    }

    connect();
    return () => {
      clearTimeout(reconnectTimer);
      wsRef.current?.close();
    };
  }, []);

  // REST fallback when WS is errored
  useEffect(() => {
    if (wsStatus !== 'error') return;
    fetchSnapshot();
    const id = setInterval(fetchSnapshot, 5000);
    return () => clearInterval(id);
  }, [wsStatus, fetchSnapshot]);

  // Reward history — independent slow poll
  useEffect(() => {
    fetchRewardHistory();
    const id = setInterval(fetchRewardHistory, 30_000);
    return () => clearInterval(id);
  }, [fetchRewardHistory]);

  const d = snapshot ?? {};

  return (
    <div className="min-h-screen flex flex-col" style={{ background: C.bg, color: C.text, fontFamily: "'JetBrains Mono', 'Fira Code', monospace" }}>

      {/* ── Header ── */}
      <div
        className="flex items-center justify-between px-6 py-4 border-b shrink-0"
        style={{ background: C.surface, borderColor: C.border }}
      >
        <div>
          <div className="flex items-center gap-3">
            <h1 className="text-lg font-black tracking-[0.2em] uppercase" style={{ color: C.amber }}>
              PROJECT MAMMON V2
            </h1>
            <Badge label="SELF-HEALING AGENTIC HFT" color={C.amber} size="xs" />
          </div>
          <p className="text-[10px] mt-0.5" style={{ color: C.muted }}>
            Avellaneda-Stoikov · Ping-Pong State Machine · RAG Memory · Agent Q
          </p>
        </div>

        <div className="flex items-center gap-4">
          {/* WS status */}
          {wsStatus === 'live' ? (
            <div className="flex items-center gap-2 px-3 py-1.5 rounded-full"
              style={{ background: `${C.green}15`, border: `1px solid ${C.green}44` }}>
              <span className="w-2 h-2 rounded-full animate-pulse" style={{ background: C.green }} />
              <span className="text-xs font-bold" style={{ color: C.green }}>LIVE</span>
            </div>
          ) : wsStatus === 'connecting' ? (
            <div className="flex items-center gap-2 px-3 py-1.5 rounded-full"
              style={{ background: `${C.amber}15`, border: `1px solid ${C.amber}44` }}>
              <span className="w-2 h-2 rounded-full animate-pulse" style={{ background: C.amber }} />
              <span className="text-xs font-bold" style={{ color: C.amber }}>CONNECTING</span>
            </div>
          ) : (
            <div className="flex items-center gap-2 px-3 py-1.5 rounded-full"
              style={{ background: `${C.red}15`, border: `1px solid ${C.red}44` }}>
              <span className="w-2 h-2 rounded-full" style={{ background: C.red }} />
              <span className="text-xs font-bold" style={{ color: C.red }}>REST FALLBACK</span>
            </div>
          )}

          {/* Last update */}
          {lastUpdate && (
            <span className="text-[10px]" style={{ color: C.muted }}>
              {fmtMs(lastUpdate)}
            </span>
          )}
        </div>
      </div>

      {/* ── KPI Bar ── */}
      <KpiBar kpis={d.kpis} />

      {/* ── Symbol Strips ── */}
      <div className="shrink-0 border-b" style={{ borderColor: C.border }}>
        {(d.symbols ?? Object.keys(COIN_META)).map((sym) => (
          <SymbolStrip
            key={sym}
            symbol={sym}
            holding={d.holdings?.[sym]}
            order={d.orders?.[sym]}
            agentQ={d.agent_q?.[sym]}
          />
        ))}
      </div>

      {/* ── Main grid ── */}
      <div className="flex-1 grid gap-4 p-4" style={{ gridTemplateColumns: '1fr 1fr 1fr', gridTemplateRows: 'auto auto' }}>

        {/* Agent Q Intelligence — col 1 */}
        <div style={{ gridColumn: '1', gridRow: '1 / 3' }}>
          <AgentQPanel agentQ={d.agent_q} rewardHistory={rewardHist} />
        </div>

        {/* Holdings — col 2 */}
        <div style={{ gridColumn: '2', gridRow: '1' }}>
          <HoldingsPanel holdings={d.holdings} />
        </div>

        {/* Order Queue — col 3 */}
        <div style={{ gridColumn: '3', gridRow: '1' }}>
          <OrderQueuePanel orders={d.orders} holdings={d.holdings} />
        </div>

        {/* PnL Chart — col 2-3 */}
        <div style={{ gridColumn: '2 / 4', gridRow: '2' }}>
          <PnlChart chart={d.chart} />
        </div>

      </div>

      {/* ── Trade Log ── */}
      <div className="px-4 pb-4">
        <TradeLog trades={d.trades} />
      </div>

      {/* ── Reward History ── */}
      <div className="px-4 pb-6">
        <RewardHistory data={rewardHist} />
      </div>

      {/* ── Footer ── */}
      <div className="text-center py-4 text-[10px] border-t" style={{ color: C.border, borderColor: C.border }}>
        Project Mammon V2 · Engine D · Agent Q · Self-Healing Agentic HFT
      </div>

    </div>
  );
}
