import type { Counters } from '@/lib/types';

const ROWS: { key: keyof Counters; label: string }[] = [
  { key: 'scored', label: 'Scored exactly once' },
  { key: 'alerts', label: 'Alerts raised' },
  { key: 'redelivered', label: 'Redelivered after a crash' },
  { key: 'deduplicated', label: 'Duplicates suppressed' },
  { key: 'dead_lettered', label: 'Dead-lettered' },
];

export function Idempotency({ counters }: { counters: Counters | null }) {
  return (
    <section data-testid="idempotency" className="panel" aria-label="Delivery guarantees">
      <h2 className="panel-title">At-least-once in, exactly-once out</h2>
      <dl className="grid grid-cols-2 gap-x-4 gap-y-2 text-sm">
        {ROWS.map(({ key, label }) => (
          <div key={key} className="contents">
            <dt className="text-muted">{label}</dt>
            <dd className="text-right font-mono">{(counters?.[key] ?? 0).toLocaleString()}</dd>
          </div>
        ))}
      </dl>
    </section>
  );
}
