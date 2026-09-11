import { describe, expect, it } from 'vitest';

import { PoolAbortedError, runPool } from './pool';

const tick = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));

describe('runPool', () => {
  it('동시 실행 수가 지정한 값을 넘지 않는다', async () => {
    let running = 0;
    let peak = 0;
    const tasks = Array.from({ length: 12 }, (_, i) => async () => {
      running += 1;
      peak = Math.max(peak, running);
      await tick();
      running -= 1;
      return i;
    });

    const results = await runPool(tasks, 3);

    expect(peak).toBe(3);
    expect(results).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
  });

  it('입력 순서대로 결과를 돌려준다', async () => {
    const delays = [30, 0, 10];
    const tasks = delays.map((ms, i) => async () => {
      await new Promise((resolve) => setTimeout(resolve, ms));
      return i;
    });

    await expect(runPool(tasks, 3)).resolves.toEqual([0, 1, 2]);
  });

  it('완료할 때마다 진행률을 보고한다', async () => {
    const seen: Array<[number, number]> = [];
    const tasks = Array.from({ length: 4 }, () => async () => 1);

    await runPool(tasks, 2, { onProgress: (done, total) => seen.push([done, total]) });

    expect(seen.map(([done]) => done).sort((a, b) => a - b)).toEqual([1, 2, 3, 4]);
    expect(seen.every(([, total]) => total === 4)).toBe(true);
  });

  it('중단 신호가 오면 PoolAbortedError를 던지고 남은 작업을 시작하지 않는다', async () => {
    const controller = new AbortController();
    let started = 0;
    const tasks = Array.from({ length: 10 }, () => async () => {
      started += 1;
      if (started === 2) controller.abort();
      await tick();
      return started;
    });

    await expect(runPool(tasks, 1, { signal: controller.signal })).rejects.toBeInstanceOf(
      PoolAbortedError,
    );
    expect(started).toBe(2);
  });

  it('작업이 실패하면 그 오류를 그대로 던진다', async () => {
    const boom = new Error('boom');
    const tasks = [async () => 1, async () => { throw boom; }, async () => 3];

    await expect(runPool(tasks, 1)).rejects.toBe(boom);
  });

  it('실패한 뒤로는 새 작업을 띄우지 않는다(H2)', async () => {
    const boom = new Error('rate limit');
    let started = 0;
    const tasks = Array.from({ length: 30 }, (_, i) => async () => {
      started += 1;
      await tick();
      if (i === 4) throw boom;
      return i;
    });

    await expect(runPool(tasks, 3)).rejects.toBe(boom);

    // 실패 시점에 떠 있던 최대 3개 + 그 사이 이미 집어간 것까지가 한계.
    // 30개를 끝까지 돌리던 것이 문제였다.
    expect(started).toBeLessThanOrEqual(7);
    expect(started).toBeGreaterThanOrEqual(5);

    // 실패 뒤에 뒤늦게 더 시작되는 작업이 없는지 확인한다.
    const settledAt = started;
    await tick();
    await tick();
    expect(started).toBe(settledAt);
  });

  it('여러 작업이 실패해도 첫 오류를 던진다', async () => {
    const first = new Error('first');
    const second = new Error('second');
    const tasks = [
      async () => { await tick(); throw first; },
      async () => { await tick(); await tick(); throw second; },
      async () => 3,
    ];

    await expect(runPool(tasks, 3)).rejects.toBe(first);
  });

  it('빈 목록은 빈 배열을 돌려준다', async () => {
    await expect(runPool([], 3)).resolves.toEqual([]);
  });
});
