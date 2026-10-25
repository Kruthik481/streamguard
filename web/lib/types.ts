export type Tx = {
  id: string;
  card_id: string;
  amount_cents: number;
  merchant_id: string;
  country: string;
  event_time: string;
};

export type Hit = { rule: string; weight: number; detail: string };

export type WorkerView = { id: number; up: boolean; partitions: number[]; scored: number };

export type Counters = {
  scored: number;
  alerts: number;
  deduplicated: number;
  redelivered: number;
  dead_lettered: number;
};

export type RunStatus = 'running' | 'finished' | 'error' | 'interrupted';

export type RunView = {
  id: number;
  status: RunStatus;
  started_at: string;
  ends_at: string;
  workers: WorkerView[];
  counters: Counters;
  lag: number[];
};

export type Alert = {
  id: number;
  tx_id: string;
  card_id: string;
  amount_cents: number;
  score: number;
  severity: string;
  rules: string[];
  details: Hit[];
  event_time: string;
  created_at: string;
};

export type MinuteStat = { minute: string; scored: number; alerts: number; deduplicated: number };

export type Scored = {
  tx: Tx;
  worker: number;
  partition: number;
  score: number;
  severity: string;
  hits: Hit[] | null;
};

export type Move = { partition: number; from: number; to: number };

export type ServerEvent =
  | { type: 'snapshot'; data: { run: RunView | null; alerts: Alert[]; stats: MinuteStat[] } }
  | { type: 'run'; data: RunView }
  | { type: 'stats'; data: RunView }
  | { type: 'scored'; data: Scored }
  | { type: 'deduplicated'; data: { tx_id: string; worker: number; partition: number } }
  | { type: 'redelivered'; data: { delivery_id: string; worker: number; partition: number } }
  | { type: 'dlq'; data: { delivery_id: string; partition: number } }
  | { type: 'worker'; data: { id: number; up: boolean } }
  | { type: 'rebalance'; data: Move[] }
  | { type: 'scenario'; data: { name: string; cards: string[] } };

export const EVENT_TYPES = [
  'snapshot', 'run', 'stats', 'scored', 'deduplicated', 'redelivered', 'dlq', 'worker', 'rebalance', 'scenario',
] as const;
