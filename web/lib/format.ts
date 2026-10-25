const RULE_LABELS: Record<string, string> = {
  velocity: 'Velocity',
  card_testing: 'Card testing',
  impossible_travel: 'Impossible travel',
  amount_spike: 'Amount spike',
  new_merchant_high_amount: 'New merchant, high amount',
};

const SCENARIO_LABELS: Record<string, string> = {
  card_testing: 'Card testing',
  impossible_travel: 'Impossible travel',
  account_takeover: 'Account takeover',
};

const usd = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' });

export const money = (cents: number) => usd.format(cents / 100);
export const ruleLabel = (rule: string) => RULE_LABELS[rule] ?? rule;
export const scenarioLabel = (name: string) => SCENARIO_LABELS[name] ?? name;
export const shortId = (id: string) => `…${id.slice(-4)}`;

export function countdown(endsAt: string, nowMs: number): string {
  const left = Math.max(0, Math.round((Date.parse(endsAt) - nowMs) / 1000));
  return `${Math.floor(left / 60)}:${String(left % 60).padStart(2, '0')}`;
}
