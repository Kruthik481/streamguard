import { afterEach, describe, expect, it, vi } from 'vitest';
import { ApiError, killWorker, startRun } from './api';

afterEach(() => vi.unstubAllGlobals());

describe('api', () => {
  it('returns JSON on success', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({ id: 1 }), { status: 201 })));
    await expect(startRun()).resolves.toEqual({ id: 1 });
  });

  it('turns error bodies into ApiError', async () => {
    const body = { error: { code: 'last_worker', message: 'At least one worker must stay up.' } };
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify(body), { status: 409 })));
    await expect(killWorker(3)).rejects.toMatchObject({ status: 409, code: 'last_worker', message: body.error.message });
  });

  it('survives non-JSON failures', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('bad gateway', { status: 502 })));
    const err = await startRun().catch((e) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect(err.code).toBe('http_error');
  });
});
