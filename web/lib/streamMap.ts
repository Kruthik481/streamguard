import type { WorkerView } from './types';

// Owner worker id per partition index, or null while a partition is between owners.
export function partitionOwners(workers: WorkerView[], partitions: number): (number | null)[] {
  const owners: (number | null)[] = Array(partitions).fill(null);
  for (const w of workers) {
    for (const p of w.partitions) {
      if (p >= 0 && p < partitions) owners[p] = w.id;
    }
  }
  return owners;
}
