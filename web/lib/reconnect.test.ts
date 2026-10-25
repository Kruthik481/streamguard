import { describe, expect, it } from 'vitest';
import { reconnectDelay } from './reconnect';

describe('reconnectDelay', () => {
  it('doubles from 2s and caps at 10s', () => {
    expect([0, 1, 2, 3, 10].map(reconnectDelay)).toEqual([2000, 4000, 8000, 10000, 10000]);
  });
});
