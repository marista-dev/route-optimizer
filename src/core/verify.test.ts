/**
 * verify.test.ts — `geocoder.verify_address` 이식 검증.
 * Python의 `'요확인(<역주소>)'` 문자열은 `verdict: '요확인'` + `reverseAddr`로 나뉜다.
 */

import { describe, expect, test } from 'vitest';

import { extractRoadPart, normalize, verifyAddress } from './verify';

describe('normalize', () => {
  test('광주광역시 → 광주, 괄호·공백 제거, 소문자', () => {
    expect(normalize('광주광역시 북구 매곡로 92 (매곡동, A@)')).toBe('광주북구매곡로92');
  });

  test('영문은 소문자로', () => {
    expect(normalize('AB CD')).toBe('abcd');
  });
});

describe('extractRoadPart', () => {
  test('구 이후 도로명만 남긴다', () => {
    expect(extractRoadPart('광주 북구 매곡로 92, 102동 106호(매곡동,아남@)'))
      .toBe('매곡로92');
  });

  test('군도 같은 규칙', () => {
    expect(extractRoadPart('전남 담양군 담양로 10')).toBe('담양로10');
  });

  test('구/군이 없으면 전체를 붙인다', () => {
    expect(extractRoadPart('세종시 한누리대로 2130')).toBe('세종시한누리대로2130');
  });

  test('빈 문자열', () => {
    expect(extractRoadPart('')).toBe('');
  });
});

describe('verifyAddress', () => {
  test('역주소가 없으면 확인불가', () => {
    expect(verifyAddress('광주 북구 매곡로 92', ''))
      .toEqual({ verdict: '확인불가', reverseAddr: '' });
  });

  test('도로명을 못 뽑으면 확인불가', () => {
    expect(verifyAddress('', '광주 북구 매곡로 92').verdict).toBe('확인불가');
  });

  test('같으면 일치', () => {
    expect(verifyAddress(
      '광주 북구 매곡로 92, 102동 106호(매곡동,아남@)',
      '광주광역시 북구 매곡로 92',
    )).toEqual({ verdict: '일치', reverseAddr: '광주광역시 북구 매곡로 92' });
  });

  test('한쪽이 다른 쪽에 포함되면 일치', () => {
    expect(verifyAddress('광주 북구 매곡로 92', '광주 북구 매곡로 92번길').verdict)
      .toBe('일치');
  });

  test('다르면 요확인 + 역주소를 따로 돌려준다', () => {
    expect(verifyAddress('광주 북구 매곡로 92', '광주 북구 삼정로 7'))
      .toEqual({ verdict: '요확인', reverseAddr: '광주 북구 삼정로 7' });
  });
});
