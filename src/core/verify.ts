/**
 * verify.ts — `src/core/geocoder.py`의 `verify_address` / `_normalize` /
 * `_extract_road_part` 이식.
 *
 * Python은 `'요확인(<역주소>)'` 한 문자열로 판정과 역주소를 함께 돌려줬지만,
 * 웹판은 `types.ts`의 `Verdict`와 역주소를 분리해 돌려준다(표에서 열이 다르다).
 */

import type { Verdict } from '../types';

/** 비교용 정규화 — '광주광역시'→'광주', 괄호·공백 제거, 소문자화. */
export function normalize(s: string): string {
  let out = s.replace(/광주광역시/gu, '광주');
  out = out.replace(/\(.*?\)/gu, '');
  out = out.replace(/\s+/gu, '');
  return out.toLowerCase();
}

/** 쉼표·괄호·동호수 제거 후 구/군 이후 도로명만 반환. */
export function extractRoadPart(s: string): string {
  let t = s.split(',')[0].trim();
  t = t.replace(/\(.*?\)/gu, '').trim();
  // Python의 str.split()과 같게 — 공백 연속을 하나로 보고 빈 조각은 버린다.
  const parts = t.split(/\s+/u).filter((p) => p.length > 0);
  for (let i = 0; i < parts.length; i++) {
    const p = parts[i];
    if (p.endsWith('구') || p.endsWith('군')) {
      return parts.slice(i + 1).join('');
    }
  }
  return parts.join('');
}

/** {@link verifyAddress}의 결과. */
export interface VerifyResult {
  /** 판정. Python의 `'요확인(...)'`은 `'요확인'` + `reverseAddr`로 나뉜다 */
  verdict: Verdict;
  /** 비교에 쓴 역지오코딩 주소(입력 그대로) */
  reverseAddr: string;
}

/**
 * 원본 주소와 역지오코딩 주소를 정규화해 비교한다.
 *
 * - 역주소가 없거나 도로명 부분을 못 뽑으면 `확인불가`
 * - 정규화 결과가 같거나 한쪽이 다른 쪽에 포함되면 `일치`
 * - 그 외는 `요확인`
 */
export function verifyAddress(original: string, reverseAddr: string): VerifyResult {
  if (!reverseAddr) return { verdict: '확인불가', reverseAddr };
  const orig = normalize(extractRoadPart(original ?? ''));
  const rev = normalize(extractRoadPart(reverseAddr));
  if (!orig || !rev) return { verdict: '확인불가', reverseAddr };
  if (orig === rev || rev.includes(orig) || orig.includes(rev)) {
    return { verdict: '일치', reverseAddr };
  }
  return { verdict: '요확인', reverseAddr };
}
