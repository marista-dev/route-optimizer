import { afterEach, describe, expect, it, vi } from 'vitest';

import type { LatLng } from '../types';
import {
  DRIVING_TIME_FAIL,
  RateLimitExceededError,
  RateLimitTracker,
  drivingTimeSec,
  fetchTimeMatrix,
  haversineEstimateSec,
} from './kakaoMobility';

const HEADERS = { Authorization: 'KakaoAK test-key' };
const A: LatLng = { lat: 35.1595, lon: 126.8526 };
const B: LatLng = { lat: 35.1795, lon: 126.9126 };

interface FakeResponse {
  status: number;
  body?: unknown;
}

/** fetch를 대신한다. 응답 목록이 떨어지면 마지막 응답을 반복한다. */
function stubFetch(responses: Array<FakeResponse | Error>) {
  const urls: string[] = [];
  let i = 0;
  vi.stubGlobal('fetch', async (url: string) => {
    urls.push(url);
    const next = responses[Math.min(i, responses.length - 1)];
    i += 1;
    if (next instanceof Error) throw next;
    return {
      status: next.status,
      json: async () => next.body,
    } as unknown as Response;
  });
  return urls;
}

function sleepSpy() {
  const waits: number[] = [];
  return { waits, sleep: async (ms: number) => void waits.push(ms) };
}

/** 정상 길찾기 응답 본문. 스텁을 직접 짤 때 쓴다. */
function okBody(duration: number) {
  return { routes: [{ result_code: 0, summary: { duration } }] };
}

const ok = (duration: number): FakeResponse => ({
  status: 200,
  body: { routes: [{ result_code: 0, summary: { duration } }] },
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('drivingTimeSec', () => {
  it('200 + result_code 0이면 주행 시간을 돌려준다', async () => {
    const urls = stubFetch([ok(742)]);

    await expect(drivingTimeSec(A, B, HEADERS, { sleep: async () => {} })).resolves.toBe(742);

    const params = new URL(urls[0]).searchParams;
    expect(params.get('origin')).toBe(`${A.lon},${A.lat}`);
    expect(params.get('destination')).toBe(`${B.lon},${B.lat}`);
    expect(params.get('priority')).toBe('RECOMMEND');
  });

  it('200이어도 result_code가 0이 아니면 즉시 실패로 본다', async () => {
    const urls = stubFetch([{ status: 200, body: { routes: [{ result_code: 104 }] } }]);
    const { waits, sleep } = sleepSpy();

    await expect(drivingTimeSec(A, B, HEADERS, { sleep })).resolves.toBe(DRIVING_TIME_FAIL);
    expect(urls).toHaveLength(1);
    expect(waits).toEqual([]);
  });

  it('400 + code -10은 rate limit으로 보고 3·6·12·24·48초 backoff한다', async () => {
    const urls = stubFetch([{ status: 400, body: { code: -10, msg: 'API limit exceeded' } }]);
    const { waits, sleep } = sleepSpy();

    await expect(drivingTimeSec(A, B, HEADERS, { sleep })).resolves.toBe(-1);
    expect(waits).toEqual([3_000, 6_000, 12_000, 24_000, 48_000]);
    expect(urls).toHaveLength(5);
  });

  it('429도 같은 rate limit 경로를 탄다', async () => {
    stubFetch([{ status: 429 }]);
    const { waits, sleep } = sleepSpy();

    await expect(drivingTimeSec(A, B, HEADERS, { sleep })).resolves.toBe(-1);
    expect(waits).toEqual([3_000, 6_000, 12_000, 24_000, 48_000]);
  });

  it('msg에 limit이 들어간 400도 rate limit으로 본다', async () => {
    stubFetch([{ status: 400, body: { code: -1, msg: 'Daily LIMIT reached' } }]);
    const { waits, sleep } = sleepSpy();

    await expect(drivingTimeSec(A, B, HEADERS, { sleep })).resolves.toBe(-1);
    expect(waits).toHaveLength(5);
  });

  it('일반 400은 재시도 없이 실패로 본다', async () => {
    const urls = stubFetch([{ status: 400, body: { code: -2, msg: 'invalid coordinate' } }]);
    const { waits, sleep } = sleepSpy();

    await expect(drivingTimeSec(A, B, HEADERS, { sleep })).resolves.toBe(DRIVING_TIME_FAIL);
    expect(urls).toHaveLength(1);
    expect(waits).toEqual([]);
  });

  it('5xx는 2·4·8·16·32초 backoff 후 일반 실패로 끝난다', async () => {
    stubFetch([{ status: 503 }]);
    const { waits, sleep } = sleepSpy();

    await expect(drivingTimeSec(A, B, HEADERS, { sleep })).resolves.toBe(DRIVING_TIME_FAIL);
    expect(waits).toEqual([2_000, 4_000, 8_000, 16_000, 32_000]);
  });

  it('네트워크 오류는 1·2·4·8·16초 backoff 후 일반 실패로 끝난다', async () => {
    stubFetch([new Error('network')]);
    const { waits, sleep } = sleepSpy();

    await expect(drivingTimeSec(A, B, HEADERS, { sleep })).resolves.toBe(DRIVING_TIME_FAIL);
    expect(waits).toEqual([1_000, 2_000, 4_000, 8_000, 16_000]);
  });

  it('rate limit 뒤 서버 오류로 끝나면 일반 실패로 분류한다', async () => {
    stubFetch([
      { status: 429 },
      { status: 429 },
      { status: 429 },
      { status: 429 },
      { status: 503 },
    ]);
    const { waits, sleep } = sleepSpy();

    await expect(drivingTimeSec(A, B, HEADERS, { sleep })).resolves.toBe(DRIVING_TIME_FAIL);
    expect(waits).toEqual([3_000, 6_000, 12_000, 24_000, 32_000]);
  });
});

describe('fetchTimeMatrix', () => {
  const pairs = [
    { key: '0-1', from: A, to: B },
    { key: '1-0', from: B, to: A },
  ];

  it('성공한 쌍의 도로시간을 키별로 모은다', async () => {
    stubFetch([ok(300)]);

    const result = await fetchTimeMatrix(pairs, HEADERS, { sleep: async () => {} });

    expect(result).toEqual({
      times: { '0-1': 300, '1-0': 300 },
      fallbacks: 0,
      fallbackKeys: [],
      aborted: false,
    });
  });

  it('실패한 쌍은 Haversine 40km/h 추정치로 채우고 수를 센다', async () => {
    stubFetch([{ status: 200, body: { routes: [{ result_code: 104 }] } }]);

    const result = await fetchTimeMatrix(pairs, HEADERS, { sleep: async () => {} });

    expect(result.fallbacks).toBe(2);
    expect(result.times['0-1']).toBe(haversineEstimateSec(A, B));
    expect(result.times['0-1']).toBeGreaterThan(0);
    // 어느 칸이 추정치인지 알아야 재계산 때 그 칸만 다시 받을 수 있다.
    expect(result.fallbackKeys.sort()).toEqual(['0-1', '1-0']);
  });

  it('성공한 칸은 추정 키에 들어가지 않는다', async () => {
    stubFetch([ok(300)]);

    const result = await fetchTimeMatrix(pairs, HEADERS, { sleep: async () => {} });

    expect(result.fallbackKeys).toEqual([]);
  });

  it('진행률을 완료 수로 보고한다', async () => {
    stubFetch([ok(100)]);
    const seen: number[] = [];

    await fetchTimeMatrix(pairs, HEADERS, {
      sleep: async () => {},
      onProgress: (done) => seen.push(done),
    });

    expect(seen.sort((a, b) => a - b)).toEqual([1, 2]);
  });

  it('rate limit이 연속 5건이면 RateLimitExceededError를 던진다', async () => {
    stubFetch([{ status: 429 }]);
    const many = Array.from({ length: 6 }, (_, i) => ({
      key: `p${i}`,
      from: A,
      to: B,
    }));

    const error = await fetchTimeMatrix(many, HEADERS, {
      sleep: async () => {},
      concurrency: 1,
    }).catch((e: unknown) => e);

    expect(error).toBeInstanceOf(RateLimitExceededError);
    const rl = error as RateLimitExceededError;
    expect(rl.consecutive).toBe(5);
    expect(rl.total).toBe(5);
  });

  it('중단 신호가 오면 그때까지 모은 결과를 돌려준다', async () => {
    stubFetch([ok(100)]);
    const controller = new AbortController();
    const many = Array.from({ length: 5 }, (_, i) => ({ key: `p${i}`, from: A, to: B }));

    const result = await fetchTimeMatrix(many, HEADERS, {
      sleep: async () => {},
      concurrency: 1,
      onProgress: (done) => {
        if (done === 2) controller.abort();
      },
      signal: controller.signal,
    });

    expect(Object.keys(result.times)).toEqual(['p0', 'p1']);
    // 중단을 조용히 삼키지 않는다(H5) — 호출부가 결과를 버릴 수 있어야 한다.
    expect(result.aborted).toBe(true);
  });

  it('요청이 떠 있는 중에 끊어도 받아 둔 쌍을 살려 돌려준다(NEW-1)', async () => {
    // 앞선 테스트는 concurrency 1이라 작업 '사이'에서만 끊겼다. 실제 화면은
    // 동시 3개라 fetch가 AbortError를 던지며 끊긴다 — 그 경로를 고정한다.
    const controller = new AbortController();
    let started = 0;
    vi.stubGlobal('fetch', (_url: string, init?: { signal?: AbortSignal }) => {
      started += 1;
      const mine = started;
      return new Promise<Response>((resolve, reject) => {
        // 첫 두 쌍은 곧바로 응답하고, 이후 요청은 중단 신호를 기다린다.
        if (mine <= 2) {
          resolve({ status: 200, json: async () => okBody(100) } as unknown as Response);
          return;
        }
        init?.signal?.addEventListener('abort', () => {
          const err = new Error('The operation was aborted.');
          err.name = 'AbortError';
          reject(err);
        });
      });
    });

    const many = Array.from({ length: 12 }, (_, i) => ({ key: `p${i}`, from: A, to: B }));
    const promise = fetchTimeMatrix(many, HEADERS, {
      sleep: async () => {},
      concurrency: 3,
      signal: controller.signal,
    });
    // 처음 두 쌍이 끝나 값이 담긴 뒤에 끊는다.
    await new Promise((r) => setTimeout(r, 5));
    controller.abort();

    const result = await promise;
    expect(result.aborted).toBe(true);
    expect(Object.keys(result.times).length).toBeGreaterThan(0);
  });
});

describe('RateLimitTracker — 호출 경계를 넘어 누적한다(H3)', () => {
  const onePair = [{ key: 'only', from: A, to: B }];

  it('통을 넘기지 않으면 호출마다 0에서 다시 센다', async () => {
    stubFetch([{ status: 429 }]);

    // 한 쌍씩 10번 부르면 매번 consecutive=1로 끝나 임계값에 닿지 않는다.
    for (let i = 0; i < 10; i += 1) {
      const r = await fetchTimeMatrix(onePair, HEADERS, { sleep: async () => {} });
      expect(r.fallbacks).toBe(1);
    }
  });

  it('같은 통을 넘기면 누적 10건에서 RateLimitExceededError가 난다', async () => {
    stubFetch([{ status: 429 }]);
    const tracker = new RateLimitTracker();

    let caught: unknown;
    let calls = 0;
    for (let i = 0; i < 12 && !caught; i += 1) {
      calls += 1;
      caught = await fetchTimeMatrix(onePair, HEADERS, { sleep: async () => {}, tracker })
        .then(() => undefined)
        .catch((e: unknown) => e);
    }

    expect(caught).toBeInstanceOf(RateLimitExceededError);
    // 임계값은 그대로 연속 5건. 한 쌍씩이므로 5번째 호출에서 터진다.
    expect(calls).toBe(5);
    expect((caught as RateLimitExceededError).consecutive).toBe(5);
    expect(tracker.consecutive).toBe(5);
    expect(tracker.total).toBe(5);
  });

  it('성공이 하나 끼면 연속 카운터만 0으로 돌아간다', async () => {
    const tracker = new RateLimitTracker();
    stubFetch([{ status: 429 }]);
    await fetchTimeMatrix(onePair, HEADERS, { sleep: async () => {}, tracker });
    expect(tracker.consecutive).toBe(1);

    vi.unstubAllGlobals();
    stubFetch([ok(100)]);
    await fetchTimeMatrix(onePair, HEADERS, { sleep: async () => {}, tracker });

    expect(tracker.consecutive).toBe(0);
    expect(tracker.total).toBe(1);
  });

  it('reset()은 두 카운터를 모두 0으로 돌린다(키 교체용)', () => {
    const tracker = new RateLimitTracker();
    tracker.consecutive = 4;
    tracker.total = 9;
    tracker.reset();
    expect({ consecutive: tracker.consecutive, total: tracker.total })
      .toEqual({ consecutive: 0, total: 0 });
  });
});

describe('fetchTimeMatrix — 부분 결과와 스냅샷(M11 / 검증 #3)', () => {
  it('반환된 times는 복사본이라 나중에 바뀌지 않는다', async () => {
    stubFetch([ok(100)]);
    const pairs = [{ key: 'a', from: A, to: B }];

    const first = await fetchTimeMatrix(pairs, HEADERS, { sleep: async () => {} });
    const snapshot = { ...first.times };
    // 두 번째 호출이 첫 번째 결과 객체를 건드리면 안 된다.
    await fetchTimeMatrix([{ key: 'b', from: A, to: B }], HEADERS, { sleep: async () => {} });

    expect(first.times).toEqual(snapshot);
    expect(first.times.b).toBeUndefined();
  });

  it('RateLimitExceededError에 그때까지 모은 결과가 실린다', async () => {
    // 첫 쌍은 성공, 그 뒤로는 계속 429 → 연속 5건에서 중단.
    stubFetch([ok(700), { status: 429 }]);
    const many = Array.from({ length: 8 }, (_, i) => ({ key: `p${i}`, from: A, to: B }));

    const error = (await fetchTimeMatrix(many, HEADERS, {
      sleep: async () => {},
      concurrency: 1,
    }).catch((e: unknown) => e)) as RateLimitExceededError;

    expect(error).toBeInstanceOf(RateLimitExceededError);
    expect(error.partial.times.p0).toBe(700);
    // p1..p4는 Haversine으로 메워졌고 p5부터는 아예 호출되지 않았다.
    expect(error.partial.fallbacks).toBe(4);
    expect(Object.keys(error.partial.times).sort()).toEqual(['p0', 'p1', 'p2', 'p3', 'p4']);
  });

  it('known으로 넘긴 쌍은 다시 부르지 않고 값을 그대로 싣는다', async () => {
    const urls = stubFetch([ok(500)]);
    const pairs = [
      { key: 'a', from: A, to: B },
      { key: 'b', from: B, to: A },
    ];

    const result = await fetchTimeMatrix(pairs, HEADERS, {
      sleep: async () => {},
      known: { a: 123 },
    });

    expect(result.times).toEqual({ a: 123, b: 500 });
    expect(urls).toHaveLength(1);
  });
});
