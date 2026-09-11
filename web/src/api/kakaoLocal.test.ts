import { afterEach, describe, expect, it, vi } from 'vitest';

import { KakaoAuthError, buildQueries, geocode, reverseGeocode } from './kakaoLocal';

const HEADERS = { Authorization: 'KakaoAK test-key' };
// 합성 주소만 쓴다. 실제 배송 데이터는 테스트에 들어가지 않는다.
const ADDRESS = '광주 북구 테스트로 1, 101동 202호 (시험동, 샘플아파트)';

interface FakeResponse {
  status: number;
  body?: unknown;
}

/** fetch를 대신하고 호출된 URL을 기록한다. */
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

function queryOf(url: string): string {
  return decodeURIComponent(new URL(url).searchParams.get('query') ?? '');
}

function sleepSpy() {
  const waits: number[] = [];
  return {
    waits,
    sleep: async (ms: number) => {
      waits.push(ms);
    },
  };
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('buildQueries', () => {
  it('전체 → 쉼표 앞 → 괄호 제거 → 동·호 제거 순서로 후보를 만든다', () => {
    expect(buildQueries(ADDRESS)).toEqual([
      '광주 북구 테스트로 1, 101동 202호 (시험동, 샘플아파트)',
      '광주 북구 테스트로 1',
      '광주 북구 테스트로 1, 101동 202호',
      '광주 북구 테스트로 1, (시험동, 샘플아파트)',
    ]);
  });

  it('중복 후보는 넣지 않는다', () => {
    expect(buildQueries('광주 북구 테스트로 1')).toEqual(['광주 북구 테스트로 1']);
  });

  it('법정동은 건물 동으로 보지 않는다', () => {
    expect(buildQueries('광주 북구 운암1동 테스트로 5')).toEqual([
      '광주 북구 운암1동 테스트로 5',
    ]);
  });
});

describe('geocode', () => {
  it('후보를 순서대로 시도하고 문서가 나오는 첫 후보를 쓴다', async () => {
    const urls = stubFetch([
      { status: 200, body: { documents: [] } },
      { status: 200, body: { documents: [] } },
      {
        status: 200,
        body: {
          documents: [
            {
              y: '35.1234',
              x: '126.9876',
              road_address: { address_name: '광주 북구 테스트로 1' },
              address: { address_name: '광주 북구 시험동 1-1' },
            },
          ],
        },
      },
    ]);

    const result = await geocode(ADDRESS, HEADERS, { sleep: async () => {} });

    expect(result).toEqual({ lat: 35.1234, lon: 126.9876, kakaoAddr: '광주 북구 테스트로 1' });
    expect(urls.map(queryOf)).toEqual(buildQueries(ADDRESS).slice(0, 3));
  });

  it('road_address가 없으면 지번 주소를 쓴다', async () => {
    stubFetch([
      {
        status: 200,
        body: {
          documents: [
            { y: '35.1', x: '126.9', road_address: null, address: { address_name: '지번주소' } },
          ],
        },
      },
    ]);

    await expect(geocode(ADDRESS, HEADERS, { sleep: async () => {} })).resolves.toMatchObject({
      kakaoAddr: '지번주소',
    });
  });

  it('429를 만나면 3초 쉬고 다음 후보로 넘어간다', async () => {
    const { waits, sleep } = sleepSpy();
    const urls = stubFetch([
      { status: 429, body: {} },
      { status: 200, body: { documents: [{ y: '35', x: '126', address: { address_name: 'a' } }] } },
    ]);

    const result = await geocode(ADDRESS, HEADERS, { sleep });

    expect(waits).toEqual([3_000]);
    expect(urls).toHaveLength(2);
    expect(result?.lat).toBe(35);
  });

  it('예외가 나면 1초 쉬고 다음 후보로 넘어간다', async () => {
    const { waits, sleep } = sleepSpy();
    stubFetch([
      new Error('network down'),
      { status: 200, body: { documents: [{ y: '35', x: '126', address: { address_name: 'a' } }] } },
    ]);

    await expect(geocode(ADDRESS, HEADERS, { sleep })).resolves.not.toBeNull();
    expect(waits).toEqual([1_000]);
  });

  it('모든 후보가 실패하면 null을 돌려준다', async () => {
    const { waits, sleep } = sleepSpy();
    const urls = stubFetch([{ status: 200, body: { documents: [] } }]);

    await expect(geocode(ADDRESS, HEADERS, { sleep })).resolves.toBeNull();
    expect(urls).toHaveLength(buildQueries(ADDRESS).length);
    expect(waits).toEqual([]);
  });
});

describe('reverseGeocode', () => {
  it('좌표를 x=lon, y=lat, input_coord=WGS84로 보낸다', async () => {
    const urls = stubFetch([
      { status: 200, body: { documents: [{ road_address: { address_name: '광주 북구 테스트로 1' } }] } },
    ]);

    const addr = await reverseGeocode(35.5, 126.5, HEADERS);

    expect(addr).toBe('광주 북구 테스트로 1');
    const params = new URL(urls[0]).searchParams;
    expect(params.get('x')).toBe('126.5');
    expect(params.get('y')).toBe('35.5');
    expect(params.get('input_coord')).toBe('WGS84');
  });

  it('도로명이 없으면 지번 주소를 쓴다', async () => {
    stubFetch([{ status: 200, body: { documents: [{ address: { address_name: '시험동 1-1' } }] } }]);
    await expect(reverseGeocode(35, 126, HEADERS)).resolves.toBe('시험동 1-1');
  });

  it('실패하면 빈 문자열을 돌려준다', async () => {
    stubFetch([{ status: 500 }]);
    await expect(reverseGeocode(35, 126, HEADERS)).resolves.toBe('');
  });
});

// ── LOW 8: 키가 죽은 것과 '주소를 못 찾음'을 구별한다 ────────────────────────
describe('KakaoAuthError', () => {
  it('geocode는 401에서 즉시 던진다(다음 후보를 시도하지 않는다)', async () => {
    const urls = stubFetch([{ status: 401, body: { message: 'invalid app key' } }]);
    const { waits, sleep } = sleepSpy();

    const err = await geocode(ADDRESS, HEADERS, { sleep }).catch((e: unknown) => e);

    expect(err).toBeInstanceOf(KakaoAuthError);
    expect((err as KakaoAuthError).status).toBe(401);
    expect((err as KakaoAuthError).name).toBe('KakaoAuthError');
    expect(urls).toHaveLength(1);
    expect(waits).toEqual([]);
  });

  it('geocode는 403도 같은 오류로 올린다', async () => {
    stubFetch([{ status: 403 }]);

    await expect(geocode(ADDRESS, HEADERS, { sleep: async () => {} }))
      .rejects.toBeInstanceOf(KakaoAuthError);
  });

  it('reverseGeocode도 401/403이면 던진다', async () => {
    stubFetch([{ status: 401 }]);

    await expect(reverseGeocode(35.1, 126.8, HEADERS)).rejects.toBeInstanceOf(KakaoAuthError);
  });

  it('결과가 없을 뿐인 200은 예전처럼 null이다', async () => {
    stubFetch([{ status: 200, body: { documents: [] } }]);

    await expect(geocode(ADDRESS, HEADERS, { sleep: async () => {} })).resolves.toBeNull();
  });

  it('그 외 4xx/5xx는 던지지 않고 다음 후보로 넘어간다', async () => {
    const urls = stubFetch([{ status: 500 }]);

    await expect(geocode(ADDRESS, HEADERS, { sleep: async () => {} })).resolves.toBeNull();
    expect(urls.length).toBeGreaterThan(1);
  });
});
