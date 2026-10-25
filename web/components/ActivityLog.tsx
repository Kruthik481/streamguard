import type { Activity } from '@/lib/reducer';

export function ActivityLog({ items }: { items: Activity[] }) {
  return (
    <section className="panel" aria-label="Activity">
      <h2 className="panel-title">Activity</h2>
      <ol data-testid="activity" className="max-h-64 space-y-1.5 overflow-y-auto text-[15px]" aria-live="polite">
        {items.length === 0 && <li className="text-muted">Kills, rebalances and redeliveries show up here.</li>}
        {items.map((a) => (
          <li key={a.id} className={`activity activity-${a.kind}`}>{a.text}</li>
        ))}
      </ol>
    </section>
  );
}
