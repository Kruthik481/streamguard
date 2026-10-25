import { ruleLabel, scenarioLabel, shortId } from './format';
import type { Alert, MinuteStat, RunView, Scored, ServerEvent, WorkerView } from './types';

export const FEED_LIMIT = 200;
export const ALERT_LIMIT = 50;
export const ACTIVITY_LIMIT = 40;
export const THROUGHPUT_SECONDS = 60;

export type FeedItem = {
  id: string;
  card: string;
  amountCents: number;
  country: string;
  worker: number;
  partition: number;
  severity: string;
  rules: string[];
};

export type Activity = {
  id: number;
  kind: 'kill' | 'restart' | 'rebalance' | 'redelivered' | 'deduplicated' | 'dlq' | 'scenario' | 'run';
  text: string;
};

export type UIState = {
  connected: boolean;
  run: RunView | null;
  feed: FeedItem[];
  alerts: Alert[];
  stats: MinuteStat[];
  throughput: number[]; // tx per second, oldest first; last entry is the current second
  throughputSecond: number;
  activity: Activity[];
  seq: number;
};

export type Action =
  | { kind: 'event'; event: ServerEvent; now: number }
  | { kind: 'connected'; value: boolean }
  | { kind: 'tick'; now: number };

export const initialState: UIState = {
  connected: false,
  run: null,
  feed: [],
  alerts: [],
  stats: [],
  throughput: Array(THROUGHPUT_SECONDS).fill(0),
  throughputSecond: 0,
  activity: [],
  seq: 0,
};

function roll(state: UIState, now: number): Pick<UIState, 'throughput' | 'throughputSecond'> {
  const second = Math.floor(now / 1000);
  if (state.throughputSecond === 0) return { throughput: state.throughput, throughputSecond: second };
  const gap = Math.min(THROUGHPUT_SECONDS, Math.max(0, second - state.throughputSecond));
  if (gap === 0) return { throughput: state.throughput, throughputSecond: second };
  return { throughput: [...state.throughput.slice(gap), ...Array(gap).fill(0)], throughputSecond: second };
}

function log(state: UIState, kind: Activity['kind'], text: string): Pick<UIState, 'activity' | 'seq'> {
  const seq = state.seq + 1;
  return { seq, activity: [{ id: seq, kind, text }, ...state.activity].slice(0, ACTIVITY_LIMIT) };
}

function bump(run: RunView | null, key: keyof RunView['counters'], by = 1): RunView | null {
  return run && { ...run, counters: { ...run.counters, [key]: run.counters[key] + by } };
}

function patchWorkers(run: RunView | null, fn: (w: WorkerView) => WorkerView): RunView | null {
  return run && { ...run, workers: run.workers.map(fn) };
}

function onScored(state: UIState, s: Scored, now: number): UIState {
  const rules = (s.hits ?? []).map((h) => h.rule);
  const item: FeedItem = {
    id: s.tx.id, card: s.tx.card_id, amountCents: s.tx.amount_cents, country: s.tx.country,
    worker: s.worker, partition: s.partition, severity: s.severity, rules,
  };
  const rolled = roll(state, now);
  const throughput = [...rolled.throughput.slice(0, -1), (rolled.throughput.at(-1) ?? 0) + 1];
  let run = bump(state.run, 'scored');
  run = patchWorkers(run, (w) => (w.id === s.worker ? { ...w, scored: w.scored + 1 } : w));
  let alerts = state.alerts;
  if (rules.length > 0) {
    run = bump(run, 'alerts');
    const alert: Alert = {
      id: -(state.seq + 1), tx_id: s.tx.id, card_id: s.tx.card_id, amount_cents: s.tx.amount_cents,
      score: s.score, severity: s.severity, rules, details: s.hits ?? [], event_time: s.tx.event_time,
      created_at: new Date(now).toISOString(),
    };
    alerts = [alert, ...alerts].slice(0, ALERT_LIMIT);
  }
  return { ...state, ...rolled, throughput, run, alerts, seq: state.seq + 1, feed: [item, ...state.feed].slice(0, FEED_LIMIT) };
}

function onEvent(state: UIState, e: ServerEvent, now: number): UIState {
  switch (e.type) {
    case 'snapshot':
      return { ...state, run: e.data.run, alerts: e.data.alerts, stats: e.data.stats };
    case 'stats':
      return { ...state, run: e.data };
    case 'run': {
      const text = e.data.status === 'running' ? `Run #${e.data.id} started` : `Run #${e.data.id} ${e.data.status}`;
      return { ...state, run: e.data, ...log(state, 'run', text) };
    }
    case 'scored':
      return onScored(state, e.data, now);
    case 'deduplicated':
      return { ...state, run: bump(state.run, 'deduplicated'),
        ...log(state, 'deduplicated', `Duplicate suppressed: ${shortId(e.data.tx_id)} was already scored`) };
    case 'redelivered':
      return { ...state, run: bump(state.run, 'redelivered'),
        ...log(state, 'redelivered', `Redelivered ${e.data.delivery_id} on P${e.data.partition} to worker ${e.data.worker}`) };
    case 'dlq':
      return { ...state, run: bump(state.run, 'dead_lettered'),
        ...log(state, 'dlq', `Dead-lettered ${e.data.delivery_id} from P${e.data.partition}`) };
    case 'worker': {
      const { id, up } = e.data;
      const run = patchWorkers(state.run, (w) => (w.id === id ? { ...w, up, partitions: up ? w.partitions : [] } : w));
      return { ...state, run, ...log(state, up ? 'restart' : 'kill', up ? `Worker ${id} restarted` : `Worker ${id} killed`) };
    }
    case 'rebalance': {
      const run = patchWorkers(state.run, (w) => {
        const kept = w.partitions.filter((p) => !e.data.some((m) => m.partition === p && m.to !== w.id));
        const gained = e.data.filter((m) => m.to === w.id).map((m) => m.partition);
        return { ...w, partitions: [...new Set([...kept, ...gained])].sort((a, b) => a - b) };
      });
      const text = e.data.map((m) => `Partition ${m.partition}: worker ${m.from} → worker ${m.to}`).join('; ');
      return { ...state, run, ...log(state, 'rebalance', text) };
    }
    case 'scenario':
      return { ...state, ...log(state, 'scenario', `Injected ${scenarioLabel(e.data.name)} on ${e.data.cards.join(', ')}`) };
  }
}

export function uiReducer(state: UIState, action: Action): UIState {
  switch (action.kind) {
    case 'connected':
      return { ...state, connected: action.value };
    case 'tick':
      return { ...state, ...roll(state, action.now) };
    case 'event':
      return onEvent(state, action.event, action.now);
  }
}

export { ruleLabel };
