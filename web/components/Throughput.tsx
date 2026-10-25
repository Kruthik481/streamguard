'use client';

import { Bar } from '@/components/charts/bar';
import { BarChart } from '@/components/charts/bar-chart';
import { ChartTooltip } from '@/components/charts/tooltip';

export function Throughput({ buckets }: { buckets: number[] }) {
  const data = buckets.map((tx, i) => ({ name: `${i - buckets.length + 1}s`, tx }));
  return (
    <figure className="panel">
      <figcaption className="panel-title">Throughput · last 60s (tx/s)</figcaption>
      <BarChart data={data} xDataKey="name" aspectRatio="4 / 1" barGap={0.25} margin={{ top: 8, right: 8, bottom: 8, left: 8 }}>
        <Bar dataKey="tx" fill="var(--accent)" lineCap={2} />
        <ChartTooltip />
      </BarChart>
    </figure>
  );
}
