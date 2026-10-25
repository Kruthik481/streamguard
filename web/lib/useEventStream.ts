'use client';

import { useEffect, useReducer } from 'react';
import { API_URL } from './api';
import { initialState, uiReducer, type UIState } from './reducer';
import { EVENT_TYPES, type ServerEvent } from './types';

const TICK_MS = 1000;

// One EventSource for the page; the browser reconnects on its own and the server re-sends a snapshot.
export function useEventStream(): UIState {
  const [state, dispatch] = useReducer(uiReducer, initialState);
  useEffect(() => {
    const es = new EventSource(`${API_URL}/events`);
    es.onopen = () => dispatch({ kind: 'connected', value: true });
    es.onerror = () => dispatch({ kind: 'connected', value: false });
    for (const type of EVENT_TYPES) {
      es.addEventListener(type, (msg) => {
        const event = { type, data: JSON.parse((msg as MessageEvent<string>).data) } as ServerEvent;
        dispatch({ kind: 'event', event, now: Date.now() });
      });
    }
    const tick = setInterval(() => dispatch({ kind: 'tick', now: Date.now() }), TICK_MS);
    return () => {
      es.close();
      clearInterval(tick);
    };
  }, []);
  return state;
}
