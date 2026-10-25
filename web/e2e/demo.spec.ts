import { expect, test } from '@playwright/test';

test('start a run, catch card testing, survive a killed worker', async ({ page }) => {
  await page.goto('/');
  await expect(page.getByTestId('run-status')).not.toHaveText(/Connecting/);
  await page.getByTestId('start-run').click();
  await expect(page.getByTestId('run-status')).toContainText('left');

  await page.getByTestId('scenario-card_testing').click();
  await expect(page.getByTestId('alert-row').filter({ hasText: 'Card testing' }).first()).toBeVisible({ timeout: 15_000 });

  await page.getByTestId('kill-worker-0').click();
  await expect(page.getByTestId('worker-0')).toHaveAttribute('data-state', 'down');
  await expect(page.getByTestId('activity')).toContainText('Partition 0: worker 0 → worker');
  await expect(page.getByTestId('worker-0')).toHaveAttribute('data-state', 'up', { timeout: 30_000 });
  await expect(page.getByTestId('feed')).not.toContainText('Start a run');
});
