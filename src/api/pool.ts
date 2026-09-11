/**
 * 프로미스 풀 — 데스크톱판 `ThreadPoolExecutor(max_workers=N)` 대응.
 *
 * 카카오 API는 미공개 rate limit이 있어 동시 실행 수를 3개로 묶어 쓴다.
 * (`app.py` 2·3단계, `optimizer._API_WORKERS` 참고)
 */

/** 중단 신호로 풀이 멈췄을 때 던지는 오류. */
export class PoolAbortedError extends Error {
  /** 중단 시점까지 끝난 작업 수 */
  readonly done: number;
  /** 전체 작업 수 */
  readonly total: number;

  constructor(done: number, total: number) {
    super(`작업이 중단되었습니다 (${done}/${total})`);
    this.name = 'PoolAbortedError';
    this.done = done;
    this.total = total;
  }
}

/** `PoolAbortedError` 여부. `instanceof`가 번들 경계에서 깨질 때를 대비한 이름 비교. */
export function isPoolAborted(err: unknown): err is PoolAbortedError {
  return err instanceof PoolAbortedError || (err as Error | null)?.name === 'PoolAbortedError';
}

export interface PoolOptions {
  /** 작업 하나가 끝날 때마다 (완료 수, 전체 수)로 호출된다. */
  onProgress?: (done: number, total: number) => void;
  /** 중단 신호. abort되면 남은 작업을 시작하지 않고 `PoolAbortedError`를 던진다. */
  signal?: AbortSignal;
}

/**
 * `tasks`를 최대 `concurrency`개까지 동시에 돌리고 입력 순서대로 결과를 돌려준다.
 *
 * - 작업 하나가 실패하면 **새 작업을 더 띄우지 않고** 이미 돌던 작업이 끝나기를
 *   기다린 뒤 첫 오류를 그대로 던진다. (`RateLimitExceededError`를 상위로
 *   전파하면서, 이미 한도를 넘긴 키로 남은 요청을 계속 쏘지 않기 위함 —
 *   데스크톱판 `executor.shutdown(wait=False, cancel_futures=True)` 대응)
 * - `signal`이 abort되면 `PoolAbortedError`를 던진다.
 *
 * 즉 실패 시점 이후로 시작되는 작업은 없고, 최대 `concurrency - 1`개가
 * 이미 떠 있던 상태로 마무리된다.
 */
export async function runPool<T>(
  tasks: ReadonlyArray<() => Promise<T>>,
  concurrency: number,
  options: PoolOptions = {},
): Promise<T[]> {
  const { onProgress, signal } = options;
  const total = tasks.length;
  const results = new Array<T>(total);
  if (total === 0) return results;

  let next = 0;
  let done = 0;
  /** 첫 실패. 세워진 뒤로는 어떤 워커도 새 작업을 꺼내지 않는다. */
  let failed = false;
  let firstError: unknown;
  const workers = Math.max(1, Math.min(concurrency, total));

  const worker = async (): Promise<void> => {
    for (;;) {
      if (failed) return;
      if (signal?.aborted) throw new PoolAbortedError(done, total);
      const index = next;
      if (index >= total) return;
      next += 1;
      try {
        results[index] = await tasks[index]();
      } catch (error) {
        if (!failed) {
          failed = true;
          firstError = error;
        }
        return;
      }
      done += 1;
      onProgress?.(done, total);
    }
  };

  // allSettled라 이미 떠 있던 작업이 전부 끝난 뒤에 오류를 던진다.
  const settled = await Promise.allSettled(
    Array.from({ length: workers }, worker),
  );
  if (failed) throw firstError;
  for (const outcome of settled) {
    if (outcome.status === 'rejected') throw outcome.reason;
  }
  return results;
}
