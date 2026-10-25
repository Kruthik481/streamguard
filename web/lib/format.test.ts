import { describe, expect, it } from 'vitest';
import { countdown, money, ruleLabel, scenarioLabel, shortId } from './format';

describe('format', () => {
  it('formats cents as dollars', () => {
    expect(money(123456)).toBe('$1,234.56');
    expect(money(50)).toBe('$0.50');
  });
  it('labels rules and scenarios', () => {
    expect(ruleLabel('card_testing')).toBe('Card testing');
    expect(ruleLabel('something_new')).toBe('something_new');
    expect(scenarioLabel('impossible_travel')).toBe('Impossible travel');
  });
  it('counts down to the end of a run and never goes negative', () => {
    const now = Date.parse('2026-10-10T12:00:00Z');
    expect(countdown('2026-10-10T12:02:31Z', now)).toBe('2:31');
    expect(countdown('2026-10-10T11:59:00Z', now)).toBe('0:00');
  });
  it('shortens ids to their tail', () => {
    expect(shortId('01JABCDEFGHJKMNPQRSTVWXYZ0')).toBe('…XYZ0');
  });
});
