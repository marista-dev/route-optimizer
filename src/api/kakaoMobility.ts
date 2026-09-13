/**
 * 카카오 모빌리티 길찾기 API 래퍼 — 데스크톱판 `optimizer.py`의
 * `_get_driving_time` / `_is_rate_limit_400` / `build_time_matrix` 호출부 이식.
 *
 * 웹판은 OR-Tools TSP를 쓰지 않고 클러스터 내부 블록 쌍만 호출하므로
 * 호출 수가 크게 줄었지만, 재시도·backoff·rate limit 판정은 그대로 유지한다.
 *
 * ## 공개 계약
 *
 * ```ts
 * class RateLimitTracker {
 *   consecutive: number;   // 연속 rate-limit 감지 수
 *   total: number;         // 누적 rate-limit 감지 수
 *   reset(): void;         // 키를 갈아끼웠을 때 호출
 * }
 *
 * interface TimeMatrixOptions {
 *   concurrency?: number;                              // 기본 3
 *   onProgress?: (done: number, total: number) => void;
 *   signal?: AbortSignal;
 *   sleep?: (ms: number) => Promise<void>;
 *   tracker?: RateLimitTracker;                        // 기본: 호출마다 새 것
 *   known?: Record<string, number>;                    // 이미 받아 둔 쌍 키 → 초
 * }
 *
 * interface TimeMatrixResult {
 *   times: Record<string, number>;   // 스냅샷 복사본(호출 뒤 변하지 않는다)
 *   fallbacks: number;               // Haversine으로 메운 셀 수
 *   fallbackKeys: string[];          // 그 셀들의 키
 *   aborted: boolean;                // signal이 abort됐으면 true
 * }
 *
 * function fetchTimeMatrix(
 *   pairs: ReadonlyArray<TimePair>,
 *   headers: KakaoHeaders,
 *   options?: TimeMatrixOptions,
 * ): Promise<TimeMatrixResult>;
 * ```
 *
 * - **중단**: `signal`이 abort되면 결과를 조용히 삼키지 않고
 *   `aborted: true`로 알린다. 호출부는 이때 상태를 쓰면 안 된다.
 * - **한도 초과**: `RateLimitExceededError`를 던지되, 그때까지 모은 결과를
 *   `error.partial`(`{ times, fallbacks }`)에 실어 보낸다. 키를 바꿔 다시
 *   부를 때 `known: error.partial.times`를 넘기면 빠진 쌍만 다시 호출한다.
 * - **카운터**: 연속 5건 / 누적 10건 기준은 그대로다. 여러 클러스터에 걸쳐
 *   누적하려면 같은 `tracker`를 계속 넘긴다(안 넘기면 호출 단위로 초기화).
 */
import { haversineFallbackSec } from '../core';
import type { KakaoHeaders, LatLng } from '../types';
import { isPoolAborted, runPool } from './pool';

/** fetch가 중단될 때 던지는 오류인지. 번들 경계를 넘으면 instanceof가 깨져 이름으로도 본다. */
function isAbortError(err: unknown): boolean {
  const name = (err as Error | null)?.name;
  return name === 'AbortError' || name === 'TimeoutError';
}

const DIRECTIONS_URL = 'https://apis-navi.kakaomobility.com/v1/directions';

/** 일반 실패(타임아웃·5xx·좌표 오류). Haversine 추정으로 대체한다. */
export const DRIVING_TIME_FAIL = 999_999;
/** rate limit으로 5회 재시도까지 실패. 상위에서 카운터를 올린다. */
export const DRIVING_TIME_RATE_LIMIT = -1;

/** 요청 타임아웃(ms) */
const REQUEST_TIMEOUT_MS = 10_000;
/** 재시도 횟수 */
const MAX_ATTEMPTS = 5;
/** 연속 이 회수 rate-limit이면 중단 */
const RATE_LIMIT_CONSEC_LIMIT = 5;
/** 누적 이 회수 rate-limit이면 중단 */
const RATE_LIMIT_TOTAL_LIMIT = 10;
/** 동시 호출 수 — 카카오 모빌리티의 미공개 rate limit을 고려해 보수적으로 3개 */
const DEFAULT_CONCURRENCY = 3;

/**
 * rate-limit 감지 횟수를 호출 경계 너머로 이어 세는 통.
 *
 * 데스크톱판은 실행 한 번 동안 카운터를 누적했다(`optimizer.py:376-377`).
 * 웹판은 클러스터마다 {@link fetchTimeMatrix}를 따로 부르므로, 화면이 이
 * 객체를 하나 만들어 S5 내내 같은 것을 넘겨야 같은 판정이 된다.
 * 키를 새로 입력받았을 때만 {@link reset}한다.
 */
export class RateLimitTracker {
  /** 연속 감지 횟수. 성공/일반 실패가 하나라도 끼면 0으로 돌아간다 */
  consecutive = 0;
  /** 누적 감지 횟수 */
  total = 0;

  /** 두 카운터를 0으로 되돌린다. */
  reset(): void {
    this.consecutive = 0;
    this.total = 0;
  }
}

/** {@link RateLimitExceededError}가 실어 보내는 '여기까지는 받아 뒀다' 묶음. */
export interface TimeMatrixPartial {
  /** 쌍 키 → 주행 시간(초). 실패해서 Haversine으로 메운 칸도 들어 있다 */
  times: Record<string, number>;
  /** 그중 Haversine으로 메운 칸 수 */
  fallbacks: number;
  /**
   * Haversine으로 메운 칸의 키.
   *
   * 개수만으로는 어느 칸이 추정치인지 알 수 없어, 재계산할 때 행렬을 통째로
   * 재사용하거나 통째로 버리는 수밖에 없다. 키를 남겨 두면 실제 도로시간은
   * 그대로 쓰고 추정치가 들어간 칸만 다시 받을 수 있다.
   */
  fallbackKeys: string[];
}

/** 카카오 API 일일 한도 초과가 의심될 때 던진다. 상위에서 키 재입력 모달을 띄운다. */
export class RateLimitExceededError extends Error {
  /** 연속 감지 횟수 */
  readonly consecutive: number;
  /** 누적 감지 횟수 */
  readonly total: number;
  /**
   * 중단 전까지 모은 결과. 키를 바꿔 재시도할 때 `known`으로 넘기면
   * 빠진 쌍만 다시 호출한다. {@link fetchTimeMatrix}가 채워 준다.
   */
  partial: TimeMatrixPartial;

  constructor(
    consecutive: number,
    total: number,
    partial: TimeMatrixPartial = { times: {}, fallbacks: 0, fallbackKeys: [] },
  ) {
    super(`API 일일 한도 초과 (연속 ${consecutive}건, 누적 ${total}건)`);
    this.name = 'RateLimitExceededError';
    this.consecutive = consecutive;
    this.total = total;
    this.partial = partial;
  }
}

export interface DrivingTimeOptions {
  /** 중단 신호 */
  signal?: AbortSignal;
  /** 대기 함수. 테스트에서 즉시 반환하는 가짜 함수를 넣는다. */
  sleep?: (ms: number) => Promise<void>;
}

const defaultSleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

/**
 * 도로시간을 못 구한 셀을 메우는 추정치(초) — 직선거리를 40km/h로 달린 시간.
 * 계산은 `core/geo.ts`의 {@link haversineFallbackSec} 하나만 쓴다.
 */
export function haversineEstimateSec(a: LatLng, b: LatLng): number {
  return haversineFallbackSec(a.lat, a.lon, b.lat, b.lon);
}

/**
 * 카카오 모빌리티는 rate limit 초과 시 HTTP 400 + code -10으로 응답한다.
 * 일반 400(좌표 오류)과 구별해야 해서 본문을 본다.
 * 호출부가 `status === 400`을 먼저 확인하므로 5xx 응답의 본문은 읽지 않는다.
 */
function isRateLimit400(body: unknown): boolean {
  if (body == null || typeof body !== 'object') return false;
  const b = body as { code?: unknown; msg?: unknown };
  if (b.code === -10) return true;
  return String(b.msg ?? '')
    .toLowerCase()
    .includes('limit');
}

async function readJson(resp: Response): Promise<unknown> {
  try {
    return await resp.json();
  } catch {
    return null;
  }
}

/**
 * 두 지점 간 자동차 주행 시간(초).
 *
 * - 정상: 주행 시간
 * - 일반 실패: `DRIVING_TIME_FAIL`(999_999) → Haversine 대체
 * - rate limit 실패: `DRIVING_TIME_RATE_LIMIT`(-1) → 상위에서 카운터 증가
 *
 * backoff: rate limit 3·6·12·24·48초, 5xx 2·4·8·16·32초, 네트워크 1·2·4·8·16초.
 */
export async function drivingTimeSec(
  from: LatLng,
  to: LatLng,
  headers: KakaoHeaders,
  options: DrivingTimeOptions = {},
): Promise<number> {
  const { signal, sleep = defaultSleep } = options;
  const url =
    `${DIRECTIONS_URL}?origin=${from.lon},${from.lat}` +
    `&destination=${to.lon},${to.lat}&priority=RECOMMEND`;

  let lastWasRateLimit = false;

  for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt += 1) {
    try {
      const resp = await fetchWithTimeout(url, headers, signal);
      if (resp.status === 200) {
        const data = (await readJson(resp)) as
          | { routes?: { result_code?: number; summary?: { duration?: number } }[] }
          | null;
        const route = data?.routes?.[0];
        if (route && route.result_code === 0) {
          return route.summary?.duration ?? DRIVING_TIME_FAIL;
        }
        // 200 + result_code != 0 → 좌표/경로 문제. 재시도 의미 없음.
        return DRIVING_TIME_FAIL;
      }
      // 400일 때만 본문을 읽는다(5xx 본문까지 파싱하던 낭비 제거).
      if (resp.status === 429 || (resp.status === 400 && isRateLimit400(await readJson(resp)))) {
        lastWasRateLimit = true;
        await sleep(3_000 * 2 ** attempt);
        continue;
      }
      if (resp.status >= 500) {
        lastWasRateLimit = false;
        await sleep(2_000 * 2 ** attempt);
        continue;
      }
      if (resp.status >= 400) {
        // 그 외 4xx(일반 400, 401, 403 등) → 재시도 불필요
        return DRIVING_TIME_FAIL;
      }
      // 1xx/3xx 등 예상 밖 응답 → 일반 실패로 본다.
      return DRIVING_TIME_FAIL;
    } catch (err) {
      if (signal?.aborted) throw err;
      lastWasRateLimit = false;
      await sleep(1_000 * 2 ** attempt);
    }
  }

  return lastWasRateLimit ? DRIVING_TIME_RATE_LIMIT : DRIVING_TIME_FAIL;
}

/** 10초 타임아웃 + 외부 중단 신호를 합친 fetch. */
async function fetchWithTimeout(
  url: string,
  headers: KakaoHeaders,
  signal: AbortSignal | undefined,
): Promise<Response> {
  const controller = new AbortController();
  // signal이 이미 abort된 채로 들어오면 'abort' 리스너는 다시 불리지 않는다(스펙).
  // 그래서 여기서 즉시 한 번 확인해 준다 — 안 하면 "중단"을 눌러도 이미 대기열을
  // 빠져나온 요청은 컨트롤러가 살아 있는 채로 최대 REQUEST_TIMEOUT_MS까지 이어진다.
  if (signal?.aborted) controller.abort();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  const onAbort = () => controller.abort();
  signal?.addEventListener('abort', onAbort, { once: true });
  try {
    return await fetch(url, { headers, signal: controller.signal });
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener('abort', onAbort);
  }
}

// ── 도로시간 행렬 ────────────────────────────────────────────────────────────
/** 행렬 한 칸 = 방향 있는 좌표 쌍. `key`는 보통 `"fromGroupId-toGroupId"`. */
export interface TimePair {
  key: string;
  from: LatLng;
  to: LatLng;
}

/**
 * {@link TimeMatrixPartial}(= `{times, fallbacks, fallbackKeys}`)에
 * 중단 여부만 얹은 것 — 같은 모양을 또 선언하지 않는다(리팩터링 점검 F3).
 */
export interface TimeMatrixResult extends TimeMatrixPartial {
  /** `signal`이 abort돼서 일부만 모았으면 true. 호출부는 결과를 버려야 한다 */
  aborted: boolean;
}

export interface TimeMatrixOptions {
  /** 동시 호출 수(기본 3) */
  concurrency?: number;
  onProgress?: (done: number, total: number) => void;
  signal?: AbortSignal;
  sleep?: (ms: number) => Promise<void>;
  /**
   * rate-limit 카운터 통. 같은 것을 계속 넘기면 호출 경계를 넘어 누적된다.
   * 안 넘기면 이 호출에서만 쓰는 새 통을 만든다.
   */
  tracker?: RateLimitTracker;
  /** 이미 받아 둔 쌍 키 → 초. 호출을 건너뛰고 이 값을 그대로 결과에 싣는다 */
  known?: Record<string, number>;
}

/**
 * 좌표 쌍 목록의 도로시간을 병렬로 모은다.
 *
 * 실패 셀은 Haversine 40km/h 추정치로 채우고 그 수를 돌려준다.
 * rate limit이 연속 5건 또는 누적 10건이면 `RateLimitExceededError`를 던지고,
 * 그때까지 모은 결과를 `error.partial`에 실어 보낸다.
 * 중단 신호가 오면 모은 결과를 `aborted: true`와 함께 돌려준다 — 조용히
 * 성공처럼 보이지 않게 하는 것이 요점이다.
 *
 * `known`으로 이미 받아 둔 쌍을 알려 주면 그 쌍은 다시 부르지 않는다.
 */
export async function fetchTimeMatrix(
  pairs: ReadonlyArray<TimePair>,
  headers: KakaoHeaders,
  options: TimeMatrixOptions = {},
): Promise<TimeMatrixResult> {
  const {
    concurrency = DEFAULT_CONCURRENCY,
    onProgress,
    signal,
    sleep,
    tracker = new RateLimitTracker(),
    known,
  } = options;

  const times: Record<string, number> = {};
  const fallbackKeys: string[] = [];
  let fallbacks = 0;

  // 이미 아는 쌍은 값만 싣고 호출하지 않는다.
  const pending = pairs.filter((pair) => {
    const hit = known?.[pair.key];
    if (hit !== undefined) {
      times[pair.key] = hit;
      return false;
    }
    return true;
  });

  const snapshot = (): TimeMatrixPartial => ({
    times: { ...times },
    fallbacks,
    fallbackKeys: [...fallbackKeys],
  });

  const tasks = pending.map((pair) => async () => {
    const sec = await drivingTimeSec(pair.from, pair.to, headers, { signal, sleep });
    if (sec === DRIVING_TIME_RATE_LIMIT) {
      tracker.total += 1;
      tracker.consecutive += 1;
      if (
        tracker.consecutive >= RATE_LIMIT_CONSEC_LIMIT ||
        tracker.total >= RATE_LIMIT_TOTAL_LIMIT
      ) {
        throw new RateLimitExceededError(tracker.consecutive, tracker.total);
      }
      times[pair.key] = haversineEstimateSec(pair.from, pair.to);
      fallbackKeys.push(pair.key);
      fallbacks += 1;
    } else if (sec === DRIVING_TIME_FAIL) {
      tracker.consecutive = 0;
      times[pair.key] = haversineEstimateSec(pair.from, pair.to);
      fallbackKeys.push(pair.key);
      fallbacks += 1;
    } else {
      tracker.consecutive = 0;
      times[pair.key] = sec;
    }
  });

  try {
    await runPool(tasks, concurrency, { onProgress, signal });
  } catch (err) {
    // 일일 한도는 중단보다 먼저 알려야 한다 — 사용자가 키를 바꿔야 이어갈 수 있다.
    // 풀이 완전히 멎은 뒤에 찍어야 '여기까지 받았다'가 실제와 맞는다.
    if (err instanceof RateLimitExceededError) {
      err.partial = snapshot();
      throw err;
    }
    // 중단은 세 가지 모습으로 올라온다.
    //  - 작업 사이에서 끊기면 runPool의 PoolAbortedError
    //  - 요청이 떠 있는 중에 끊기면 fetch가 던진 AbortError가 그대로 올라온다
    //  - 그 외에도 signal이 이미 abort면 중단으로 본다
    // 어느 쪽이든 받아 둔 쌍은 살려서 돌려줘야 재호출로 쿼터를 두 번 쓰지 않는다.
    if (isPoolAborted(err) || isAbortError(err) || signal?.aborted === true) {
      return { ...snapshot(), aborted: true };
    }
    throw err;
  }
  return { ...snapshot(), aborted: signal?.aborted === true };
}
