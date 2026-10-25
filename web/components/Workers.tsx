'use client';

import { AnimatePresence, motion } from 'motion/react';
import type { WorkerView } from '@/lib/types';

type Props = { workers: WorkerView[]; lag: number[]; running: boolean; onKill: (id: number) => void };

export function Workers({ workers, lag, running, onKill }: Props) {
  const liveCount = workers.filter((w) => w.up).length;
  return (
    <section className="panel" aria-label="Workers">
      <h2 className="panel-title">Consumer group · 4 partitions</h2>
      <ul className="grid grid-cols-2 gap-3 md:grid-cols-4">
        {workers.map((w) => (
          <li key={w.id} data-testid={`worker-${w.id}`} data-state={w.up ? 'up' : 'down'}
            className={`worker ${w.up ? '' : 'worker-down'}`}>
            <div className="flex flex-wrap items-baseline justify-between gap-x-2">
              <span className="font-mono">worker {w.id}</span>
              <span className="text-xs">{w.up ? 'up' : 'restarting'}</span>
            </div>
            <ul className="mt-2 flex min-h-7 gap-1" aria-label={`Partitions owned by worker ${w.id}`}>
              <AnimatePresence initial={false}>
                {w.partitions.map((p) => (
                  <motion.li key={p} layoutId={`partition-${p}`} className="chip font-mono"
                    initial={{ opacity: 0, scale: 0.8 }} animate={{ opacity: 1, scale: 1 }} exit={{ opacity: 0, scale: 0.8 }}>
                    P{p} <span className="text-muted">lag {lag[p] ?? 0}</span>
                  </motion.li>
                ))}
              </AnimatePresence>
            </ul>
            <div className="mt-2 flex items-center justify-between text-xs text-muted">
              <span>{w.scored.toLocaleString()} scored</span>
              <button type="button" data-testid={`kill-worker-${w.id}`} className="btn-danger"
                disabled={!running || !w.up || liveCount <= 1} onClick={() => onKill(w.id)}>
                Kill
              </button>
            </div>
          </li>
        ))}
      </ul>
    </section>
  );
}
