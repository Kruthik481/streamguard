'use client';

import { useSyncExternalStore } from 'react';
import { motion } from 'motion/react';
import type { FeedItem } from '@/lib/reducer';
import { partitionOwners } from '@/lib/streamMap';
import type { WorkerView } from '@/lib/types';

const PARTITIONS = 4;
const ROW_Y = [70, 150, 230, 310];
const NARROW = '(max-width: 640px)';

// Wide screens get long lanes; phones get a compact map so its text stays readable.
const WIDE = { width: 960, laneStart: 120, bend: 420, nodeX: 700, nodeW: 240 };
const COMPACT = { width: 520, laneStart: 96, bend: 150, nodeX: 300, nodeW: 210 };
type Geometry = typeof WIDE;

function subscribe(onChange: () => void) {
  const mq = window.matchMedia(NARROW);
  mq.addEventListener('change', onChange);
  return () => mq.removeEventListener('change', onChange);
}
const PULSES_PER_LANE = 3;
const PULSE_SECONDS = 2.4;

type Props = { workers: WorkerView[]; lag: number[]; latest: FeedItem | null; running: boolean };

function lanePath(g: Geometry, p: number, owner: number | null): string {
  const y = ROW_Y[p];
  const ny = owner === null ? y : ROW_Y[owner];
  const mid = (g.bend + g.nodeX) / 2;
  return `M ${g.laneStart} ${y} L ${g.bend} ${y} C ${mid} ${y}, ${mid} ${ny}, ${g.nodeX} ${ny}`;
}

function nodeShape(w: number): string {
  return `0,0 ${w - 16},0 ${w},16 ${w},60 0,60`;
}

// The live picture of the consumer group: partition lanes feed whichever node owns them, so a
// kill shows up as a node going dark and its lane swinging over to the new owner.
export function StreamMap({ workers, lag, latest, running }: Props) {
  const compact = useSyncExternalStore(subscribe, () => window.matchMedia(NARROW).matches, () => false);
  const g = compact ? COMPACT : WIDE;
  const owners = partitionOwners(workers, PARTITIONS);
  const flagged = latest && latest.rules.length > 0 ? latest : null;
  const summary = owners.map((o, p) => `partition ${p} to ${o === null ? 'no worker' : `worker ${o}`}`).join(', ');
  return (
    <section className="panel stream-map" aria-label="Stream map">
      <svg viewBox={`0 25 ${g.width} 330`} role="img" className="w-full" aria-label={`Stream map: ${summary}`}>
        <defs>
          <pattern id="hazard" width="14" height="14" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
            <rect width="7" height="14" fill="var(--down)" opacity="0.35" />
          </pattern>
        </defs>
        {owners.map((owner, p) => {
          const d = lanePath(g, p, owner);
          return (
            <g key={p}>
              <motion.path id={`lane-${p}`} d={d} animate={{ d }} transition={{ duration: 0.9, ease: [0.65, 0, 0.35, 1] }}
                className="lane" />
              {flagged && flagged.partition === p && (
                <path key={flagged.id} d={d} className={`lane-flash sev-stroke-${flagged.severity}`} />
              )}
              {running && owner !== null && Array.from({ length: PULSES_PER_LANE }, (_, i) => (
                <circle key={i} r="4" className="pulse">
                  <animateMotion dur={`${PULSE_SECONDS}s`} begin={`${(i * PULSE_SECONDS) / PULSES_PER_LANE}s`}
                    repeatCount="indefinite">
                    <mpath href={`#lane-${p}`} />
                  </animateMotion>
                </circle>
              ))}
              <g transform={`translate(54 ${ROW_Y[p]})`}>
                <polygon points="0,-28 24,-14 24,14 0,28 -24,14 -24,-14" className="hex" />
                <text y="7" textAnchor="middle" className="hex-label">P{p}</text>
              </g>
              <text x={g.laneStart + 8} y={ROW_Y[p] - 12} className="lane-meta">lag {lag[p] ?? 0}</text>
            </g>
          );
        })}
        {workers.map((w) => (
          <g key={w.id} transform={`translate(${g.nodeX} ${ROW_Y[w.id] - 30})`}>
            <polygon points={nodeShape(g.nodeW)} className={w.up ? 'node-shape' : 'node-shape node-shape-down'} />
            {!w.up && <polygon points={nodeShape(g.nodeW)} fill="url(#hazard)" />}
            <text x="16" y="26" className={w.up ? 'node-name' : 'node-name node-name-down'}>NODE-0{w.id}</text>
            <text x="16" y="47" className="node-meta">{w.scored.toLocaleString()} scored</text>
            <text x={g.nodeW - 16} y="47" textAnchor="end" className={w.up ? 'node-state' : 'node-state node-state-down'}>
              {w.up ? 'ONLINE' : 'TERMINATED'}
            </text>
          </g>
        ))}
      </svg>
    </section>
  );
}
