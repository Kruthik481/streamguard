'use client';

import { useEffect, useState } from 'react';
import { fetchAlerts } from '@/lib/api';
import { money, ruleLabel } from '@/lib/format';
import type { Alert } from '@/lib/types';

const RULES = ['', 'velocity', 'card_testing', 'impossible_travel', 'amount_spike', 'new_merchant_high_amount'];

// live: alerts pushed over SSE for the unfiltered first page; refreshKey changes when a run ends.
export function AlertHistory({ live, refreshKey }: { live: Alert[]; refreshKey: string }) {
  const [rule, setRule] = useState('');
  const [pages, setPages] = useState<Alert[]>([]);
  const [cursor, setCursor] = useState(0);
  const [error, setError] = useState('');

  useEffect(() => {
    let cancelled = false;
    fetchAlerts(0, rule)
      .then((r) => { if (!cancelled) { setPages(r.alerts); setCursor(r.next_cursor); setError(''); } })
      .catch((e: Error) => { if (!cancelled) setError(e.message); });
    return () => { cancelled = true; };
  }, [rule, refreshKey]);

  const loadMore = () =>
    fetchAlerts(cursor, rule).then((r) => { setPages((p) => [...p, ...r.alerts]); setCursor(r.next_cursor); })
      .catch((e: Error) => setError(e.message));

  const seen = new Set<string>();
  const rows = [...(rule ? [] : live), ...pages].filter((a) => !seen.has(a.tx_id) && seen.add(a.tx_id));

  return (
    <section className="panel" aria-label="Alert history">
      <div className="flex items-center justify-between">
        <h2 className="panel-title">Alerts</h2>
        <label className="text-sm">
          <span className="sr-only">Filter by rule</span>
          <select value={rule} onChange={(e) => setRule(e.target.value)} className="select">
            {RULES.map((r) => <option key={r} value={r}>{r ? ruleLabel(r) : 'All rules'}</option>)}
          </select>
        </label>
      </div>
      {error && <p role="alert" className="text-sm">{error}</p>}
      <div className="max-h-[28rem] overflow-y-auto [overflow-anchor:none]">
      <table className="w-full text-base">
        <thead className="sticky top-0 bg-[var(--panel)]"><tr><th>Card</th><th>Amount</th><th>Rules</th><th>Severity</th></tr></thead>
        <tbody>
          {rows.map((a) => (
            <tr key={a.tx_id} data-testid="alert-row" className={`sev-${a.severity}`}>
              <td className="font-mono text-sm">{a.card_id}</td>
              <td className="font-mono text-sm">{money(a.amount_cents)}</td>
              <td title={a.details.map((d) => d.detail).join(' · ')}>{a.rules.map(ruleLabel).join(', ')}</td>
              <td className={`sev-text-${a.severity}`}>{a.severity}</td>
            </tr>
          ))}
          {rows.length === 0 && <tr><td colSpan={4} className="text-muted">No alerts yet.</td></tr>}
        </tbody>
      </table>
      </div>
      {cursor > 0 && <button type="button" className="btn mt-3" onClick={loadMore}>Load more</button>}
    </section>
  );
}
