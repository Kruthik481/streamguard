'use client';

import { useEffect, useReducer } from 'react';
import { API_URL } from './api';
import { reconnectDelay } from './reconnect';
import { initialState, uiReducer, type UIState } from './reducer';
import { EVENT_TYPES, type ServerEvent } from './types';

const TICK_MS = 1000;

// One EventSource for the page; the server re-sends a snapshot on every connect. The browser retries
// dropped connections itself, but gives up for good after a non-200 answer (a proxy 502 during a
// restart), so a closed stream is reopened here with backoff.
export function useEventStream(): UIState {
  const [state, dispatch] = useReducer(uiReducer, initialState);
  useEffect(() => {
    let es: EventSource | null = null;
    let retry: ReturnType<typeof setTimeout> | undefined;
    let attempt = 0;
    const open = () => {
      const source = new EventSource(`${API_URL}/events`);
      es = source;
      source.onopen = () => {
        attempt = 0;
        dispatch({ kind: 'connected', value: true });
      };
      source.onerror = () => {
        dispatch({ kind: 'connected', value: false });
        if (source.readyState === EventSource.CLOSED) {
          source.close();
          retry = setTimeout(open, reconnectDelay(attempt++));
        }
      };
      for (const type of EVENT_TYPES) {
        source.addEventListener(type, (msg) => {
          const event = { type, data: JSON.parse((msg as MessageEvent<string>).data) } as ServerEvent;
          dispatch({ kind: 'event', event, now: Date.now() });
        });
      }
    };
    open();
    const tick = setInterval(() => dispatch({ kind: 'tick', now: Date.now() }), TICK_MS);
    return () => {
      es?.close();
      clearTimeout(retry);
      clearInterval(tick);
    };
  }, []);
  return state;
}
