import { describe, expect, it } from 'vitest';
import { partitionOwners } from './streamMap';

describe('partitionOwners', () => {
  it('maps each partition to the worker that owns it', () => {
    const workers = [
      { id: 0, up: false, partitions: [], scored: 3 },
      { id: 1, up: true, partitions: [1, 0], scored: 9 },
      { id: 2, up: true, partitions: [2], scored: 4 },
      { id: 3, up: true, partitions: [3], scored: 5 },
    ];
    expect(partitionOwners(workers, 4)).toEqual([1, 1, 2, 3]);
  });

  it('leaves a partition unowned while nobody holds it', () => {
    expect(partitionOwners([{ id: 0, up: true, partitions: [0], scored: 0 }], 2)).toEqual([0, null]);
  });
});
