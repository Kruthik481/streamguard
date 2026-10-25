import { describe, expect, it } from 'vitest';
import { ACTIVITY_LIMIT, FEED_LIMIT, initialState, uiReducer, type UIState } from './reducer';
import type { RunView, Scored, ServerEvent } from './types';

const t0 = Date.parse('2026-10-10T12:00:00Z');

const run: RunView = {
  id: 1,
  status: 'running',
  started_at: '2026-10-10T12:00:00Z',
  ends_at: '2026-10-10T12:03:00Z',
  workers: [0, 1, 2, 3].map((id) => ({ id, up: true, partitions: [id], scored: 0 })),
  counters: { scored: 0, alerts: 0, deduplicated: 0, redelivered: 0, dead_lettered: 0 },
  lag: [0, 0, 0, 0],
};

function scored(id: string, hits: Scored['hits'] = null): ServerEvent {
  return {
    type: 'scored',
    data: {
      tx: { id, card_id: 'card-001', amount_cents: 1200, merchant_id: 'm-1', country: 'US', event_time: '2026-10-10T12:00:00Z' },
      worker: 2,
      partition: 2,
      score: hits ? 3 : 0,
      severity: hits ? 'medium' : '',
      hits,
    },
  };
}

function apply(state: UIState, ...events: ServerEvent[]): UIState {
  return events.reduce((s, event) => uiReducer(s, { kind: 'event', event, now: t0 }), state);
}

describe('uiReducer', () => {
  it('loads the snapshot', () => {
    const s = apply(initialState, { type: 'snapshot', data: { run, alerts: [], stats: [] } });
    expect(s.run?.id).toBe(1);
    expect(s.alerts).toEqual([]);
  });

  it('adds scored transactions to the feed, counts them and buckets throughput', () => {
    const s = apply(initialState, { type: 'snapshot', data: { run, alerts: [], stats: [] } }, scored('a'), scored('b'));
    expect(s.feed.map((f) => f.id)).toEqual(['b', 'a']);
    expect(s.run?.counters.scored).toBe(2);
    expect(s.throughput.at(-1)).toBe(2);
  });

  it('turns scored transactions with hits into alerts', () => {
    const hit = [{ rule: 'card_testing', weight: 3, detail: '3 charges' }];
    const s = apply(initialState, { type: 'snapshot', data: { run, alerts: [], stats: [] } }, scored('a', hit));
    expect(s.alerts[0]).toMatchObject({ tx_id: 'a', rules: ['card_testing'], severity: 'medium' });
    expect(s.feed[0].rules).toEqual(['card_testing']);
    expect(s.run?.counters.alerts).toBe(1);
  });

  it('caps the feed', () => {
    const events = Array.from({ length: FEED_LIMIT + 5 }, (_, i) => scored(`t${i}`));
    expect(apply(initialState, ...events).feed).toHaveLength(FEED_LIMIT);
  });

  it('marks a killed worker down and applies the rebalance', () => {
    const s = apply(
      initialState,
      { type: 'snapshot', data: { run, alerts: [], stats: [] } },
      { type: 'worker', data: { id: 0, up: false } },
      { type: 'rebalance', data: [{ partition: 0, from: 0, to: 1 }] },
    );
    expect(s.run?.workers[0]).toMatchObject({ up: false, partitions: [] });
    expect(s.run?.workers[1].partitions).toEqual([0, 1]);
    expect(s.activity[0].text).toContain('Partition 0: worker 0 → worker 1');
    expect(s.activity[1].kind).toBe('kill');
  });

  it('counts redeliveries and suppressed duplicates', () => {
    const s = apply(
      initialState,
      { type: 'snapshot', data: { run, alerts: [], stats: [] } },
      { type: 'redelivered', data: { delivery_id: '1-0', worker: 1, partition: 0 } },
      { type: 'deduplicated', data: { tx_id: '01JXYZ', worker: 1, partition: 0 } },
    );
    expect(s.run?.counters).toMatchObject({ redelivered: 1, deduplicated: 1 });
    expect(s.activity[0].kind).toBe('deduplicated');
  });

  it('replaces the run on stats and caps the activity log', () => {
    const later = { ...run, counters: { ...run.counters, scored: 99 } };
    const many = Array.from({ length: ACTIVITY_LIMIT + 3 }, (): ServerEvent => ({ type: 'dlq', data: { delivery_id: 'x', partition: 0 } }));
    const s = apply(initialState, ...many, { type: 'stats', data: later });
    expect(s.run?.counters.scored).toBe(99);
    expect(s.activity).toHaveLength(ACTIVITY_LIMIT);
  });

  it('rolls throughput buckets forward on tick', () => {
    const s1 = apply(initialState, scored('a'));
    const s2 = uiReducer(s1, { kind: 'tick', now: t0 + 3000 });
    expect(s2.throughput.at(-1)).toBe(0);
    expect(s2.throughput.at(-4)).toBe(1);
  });

  it('never mutates the previous state', () => {
    const before = apply(initialState, { type: 'snapshot', data: { run, alerts: [], stats: [] } });
    const frozen = JSON.stringify(before);
    apply(before, scored('a'), { type: 'worker', data: { id: 0, up: false } });
    expect(JSON.stringify(before)).toBe(frozen);
  });

  it('tracks the connection', () => {
    expect(uiReducer(initialState, { kind: 'connected', value: true }).connected).toBe(true);
  });
});
