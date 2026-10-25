'use client';

import type { RunView } from '@/lib/types';
import { countdown, scenarioLabel } from '@/lib/format';

const SCENARIOS = ['card_testing', 'impossible_travel', 'account_takeover'] as const;

type Props = {
  run: RunView | null;
  now: number;
  connected: boolean;
  busy: boolean;
  onStart: () => void;
  onInject: (name: string) => void;
};

export function RunBar({ run, now, connected, busy, onStart, onInject }: Props) {
  const running = run?.status === 'running';
  return (
    <section className="panel flex flex-wrap items-center gap-3" aria-label="Run controls">
      <button type="button" data-testid="start-run" className="btn-primary" disabled={busy || running} onClick={onStart}>
        {running ? 'Run in progress' : 'Start a 3-minute run'}
      </button>
      <p data-testid="run-status" className="text-sm text-muted" aria-live="polite">
        {!connected && 'Connecting to the stream…'}
        {connected && running && `Run #${run.id} · ${countdown(run.ends_at, now)} left`}
        {connected && !running && (run ? `Last run #${run.id} ${run.status}` : 'No run yet')}
      </p>
      <div className="ml-auto flex flex-wrap gap-2" role="group" aria-label="Inject an attack">
        {SCENARIOS.map((name) => (
          <button key={name} type="button" data-testid={`scenario-${name}`} className="btn" disabled={busy || !running}
            onClick={() => onInject(name)}>
            {scenarioLabel(name)}
          </button>
        ))}
      </div>
    </section>
  );
}
