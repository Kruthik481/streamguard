import type { FeedItem } from '@/lib/reducer';
import { money, ruleLabel, shortId } from '@/lib/format';

export function Feed({ items }: { items: FeedItem[] }) {
  return (
    <section className="panel" aria-label="Live transactions">
      <h2 className="panel-title">Live transactions</h2>
      <ol data-testid="feed" className="max-h-96 overflow-y-auto font-mono text-xs">
        {items.length === 0 && <li className="text-muted">Start a run to see transactions stream in.</li>}
        {items.map((t) => (
          <li key={t.id} className={`feed-row ${t.rules.length ? `sev-${t.severity}` : ''}`}>
            <span>{shortId(t.id)}</span>
            <span>{t.card}</span>
            <span>{t.country}</span>
            <span className="text-right">{money(t.amountCents)}</span>
            <span className="text-muted">P{t.partition}·w{t.worker}</span>
            <span>{t.rules.map(ruleLabel).join(', ')}</span>
          </li>
        ))}
      </ol>
    </section>
  );
}
