/**
 * 카카오 로컬 API 래퍼 — `src/core/geocoder.py` 이식.
 *   - geocode        : 주소 → 위도/경도
 *   - reverseGeocode : 위도/경도 → 도로명 주소
 *
 * 주소 검증(`verify_address`)은 순수 로직이라 `core/verify.ts`가 맡는다.
 */
import { stripUnit } from '../core';
import type { KakaoHeaders } from '../types';

const SEARCH_URL = 'https://dapi.kakao.com/v2/local/search/address.json';
const COORD2ADDR_URL = 'https://dapi.kakao.com/v2/local/geo/coord2address.json';

/** 429(rate limit) 응답 뒤 쉬는 시간(ms). 데스크톱판과 동일. */
const RATE_LIMIT_WAIT_MS = 3_000;
/** 예외(네트워크 오류 등) 뒤 쉬는 시간(ms). */
const ERROR_WAIT_MS = 1_000;

/** 지오코딩 성공 결과. */
export interface GeocodeResult {
  lat: number;
  lon: number;
  /** 카카오가 돌려준 도로명(없으면 지번) 주소 */
  kakaoAddr: string;
}

export interface LocalApiOptions {
  /** 대기 함수. 테스트에서 즉시 반환하는 가짜 함수를 넣는다. */
  sleep?: (ms: number) => Promise<void>;
  /** 중단 신호 */
  signal?: AbortSignal;
}

const defaultSleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

/**
 * REST 키가 없거나 잘못됐을 때(HTTP 401/403) 던진다.
 *
 * '주소를 못 찾음'(null)과 '키가 죽었음'을 화면이 구별할 수 있어야 해서
 * 오류로 올린다. 401/403은 다른 질의 후보로 바꿔도 절대 통과하지 못하므로
 * 남은 후보를 시도하지 않고 즉시 던진다.
 * 호출부는 `err instanceof KakaoAuthError`(또는 `err.name === 'KakaoAuthError'`)로
 * 받아 S1 키 재입력으로 돌려보내면 된다.
 */
export class KakaoAuthError extends Error {
  /** 응답 HTTP 상태(401 또는 403) */
  readonly status: number;

  constructor(status: number) {
    super(`카카오 REST 키가 거부되었습니다 (HTTP ${status}). 키를 다시 확인해 주세요.`);
    this.name = 'KakaoAuthError';
    this.status = status;
  }
}

/** 401/403이면 {@link KakaoAuthError}. 그 외 상태는 아무 일도 하지 않는다. */
function throwIfAuthFailure(status: number): void {
  if (status === 401 || status === 403) throw new KakaoAuthError(status);
}

// ── 질의 생성 ────────────────────────────────────────────────────────────────
/**
 * 지오코딩 질의 후보를 순서대로 만든다(`geocoder._build_queries`).
 * 전체 → 첫 쉼표 앞 → 괄호 제거 → 동/호 제거.
 */
export function buildQueries(address: string): string[] {
  const queries = [address];
  if (address.includes(',')) queries.push(address.split(',')[0].trim());
  const noBracket = address.replace(/\(.*?\)/g, '').trim();
  if (!queries.includes(noBracket)) queries.push(noBracket);
  const stripped = stripUnit(address);
  if (stripped && !queries.includes(stripped)) queries.push(stripped);
  return queries;
}

// ── API 호출 ────────────────────────────────────────────────────────────────
function pickAddressName(doc: Record<string, unknown>): string {
  const road = doc.road_address as { address_name?: string } | null | undefined;
  if (road?.address_name) return road.address_name;
  const jibun = doc.address as { address_name?: string } | null | undefined;
  return jibun?.address_name ?? '';
}

/**
 * 주소 → 좌표. 질의 후보를 차례로 시도하고 전부 실패하면 null.
 * 429는 3초, 예외는 1초 쉬고 다음 후보로 넘어간다(데스크톱판과 동일).
 *
 * @throws {KakaoAuthError} 401/403 — 키가 잘못됐거나 권한이 없다.
 *   (null = '주소를 못 찾음'과 구별하기 위해 오류로 올린다)
 */
export async function geocode(
  address: string,
  headers: KakaoHeaders,
  options: LocalApiOptions = {},
): Promise<GeocodeResult | null> {
  const { sleep = defaultSleep, signal } = options;

  for (const query of buildQueries(address)) {
    try {
      const resp = await fetch(`${SEARCH_URL}?query=${encodeURIComponent(query)}`, {
        headers,
        signal,
      });
      if (resp.status === 200) {
        const body = (await resp.json()) as { documents?: Record<string, unknown>[] };
        const doc = body.documents?.[0];
        if (doc) {
          return {
            lat: Number(doc.y),
            lon: Number(doc.x),
            kakaoAddr: pickAddressName(doc),
          };
        }
      } else if (resp.status === 429) {
        await sleep(RATE_LIMIT_WAIT_MS);
      } else {
        throwIfAuthFailure(resp.status);
      }
    } catch (err) {
      if (err instanceof KakaoAuthError) throw err;
      if (signal?.aborted) throw err;
      await sleep(ERROR_WAIT_MS);
    }
  }
  return null;
}

/**
 * 좌표 → 도로명(없으면 지번) 주소. 실패하면 빈 문자열.
 *
 * @throws {KakaoAuthError} 401/403 — 키가 잘못됐거나 권한이 없다.
 */
export async function reverseGeocode(
  lat: number,
  lon: number,
  headers: KakaoHeaders,
  options: LocalApiOptions = {},
): Promise<string> {
  const { signal } = options;
  try {
    const url = `${COORD2ADDR_URL}?x=${encodeURIComponent(String(lon))}&y=${encodeURIComponent(
      String(lat),
    )}&input_coord=WGS84`;
    const resp = await fetch(url, { headers, signal });
    if (resp.status === 200) {
      const body = (await resp.json()) as { documents?: Record<string, unknown>[] };
      const doc = body.documents?.[0];
      if (doc) return pickAddressName(doc);
    } else {
      throwIfAuthFailure(resp.status);
    }
  } catch (err) {
    if (err instanceof KakaoAuthError) throw err;
    if (signal?.aborted) throw err;
  }
  return '';
}

// ── 키 확인 ──────────────────────────────────────────────────────────────────
/** REST 키를 카카오 인증 헤더로. */
export function authHeaders(key: string): KakaoHeaders {
  return { Authorization: `KakaoAK ${key}` };
}

/**
 * 키가 유효한지 한 번만 찔러본다.
 *
 * `geocode`는 401을 조용히 넘기고 null을 돌려주므로 잘못된 키와 '주소를 못 찾음'을
 * 구분할 수 없다. 그래서 여기서만 로컬 API를 직접 호출해 상태 코드를 본다.
 * 결과가 없어도 되는 질의라 좌표는 보지 않고 상태 코드만 읽는다.
 */
export async function probeRestKey(
  headers: KakaoHeaders,
  signal?: AbortSignal,
): Promise<'ok' | 'invalid' | 'unknown'> {
  try {
    const resp = await fetch(
      'https://dapi.kakao.com/v2/local/search/address.json?query=' + encodeURIComponent('서울특별시'),
      { headers, signal },
    );
    if (resp.status === 401 || resp.status === 403) return 'invalid';
    return resp.status === 200 ? 'ok' : 'unknown';
  } catch {
    return 'unknown';
  }
}
