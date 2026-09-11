/**
 * address.test.ts — `core/address.ts`가 Python `src/core/address.py`와 같은지 검증.
 *
 * 1) address.py docstring 예제 그대로
 * 2) `scripts/gen_fixtures.py`가 Python으로 떠낸 fixture와의 대조
 */

import { describe, expect, test } from 'vitest';

import {
  compareUnit,
  compareUnitAddr,
  complexKey,
  complexName,
  parseUnit,
  stripUnit,
  unitSortKey,
} from './address';
import addressFixture from './__fixtures__/address.json';

interface AddressCase {
  address: string;
  parseUnit: { dongNum: number | null; dongTxt: string | null; ho: number | null };
  complexKey: string;
  stripUnit: string;
  unitSortKey: [number, number, string, number, number, number];
}

describe('parseUnit — address.py docstring 예제', () => {
  test('숫자 동 + 호', () => {
    expect(parseUnit('광주 북구 삼정로 7, 207동 403호(두암동, 주공2단지@)'))
      .toEqual({ dongNum: 207, dongTxt: null, ho: 403 });
  });

  test('영문 동 + 호', () => {
    expect(parseUnit('광주 북구 안산로 38-3, A동 102호 (오치2동, 영진하우스)'))
      .toEqual({ dongNum: null, dongTxt: 'A', ho: 102 });
  });

  test('동 없이 호만', () => {
    expect(parseUnit('광주 북구 하백로 46번길 9, 809호(매곡동, 부림@)'))
      .toEqual({ dongNum: null, dongTxt: null, ho: 809 });
  });

  test("'호' 접미사 없는 호수", () => {
    expect(parseUnit('광주 북구 첨단연신로 250, 112동 1205(신용동,첨단휴먼시아)'))
      .toEqual({ dongNum: 112, dongTxt: null, ho: 1205 });
  });

  test('빈 주소', () => {
    expect(parseUnit('')).toEqual({ dongNum: null, dongTxt: null, ho: null });
  });

  test('법정동 숫자는 건물 동이 아니다', () => {
    expect(parseUnit('광주 북구 오치2동 123-4').dongNum).toBeNull();
    expect(parseUnit('광주 북구 운암1동 45').dongNum).toBeNull();
  });

  test("'호반리젠시빌'의 '호'는 호수가 아니다", () => {
    expect(parseUnit('광주 북구 설죽로 507 호반리젠시빌')).toEqual(
      { dongNum: null, dongTxt: null, ho: null },
    );
  });

  test('동 뒤 숫자가 호수가 아닌 경우', () => {
    expect(parseUnit('광주 북구 매곡로 92, 101동 15번지').ho).toBeNull();
    expect(parseUnit('광주 북구 매곡로 92, 101동 20-1').ho).toBeNull();
  });
});

describe('complexKey — address.py docstring 예제', () => {
  test('표기가 흔들려도 같은 키', () => {
    expect(complexKey('광주 북구 매곡로 92, 102동 106호(매곡동,아남@)'))
      .toBe('광주북구매곡로92');
    expect(complexKey('광주 북구 매곡로 92, 101동 105호(매곡동, 아남@)'))
      .toBe('광주북구매곡로92');
  });

  test('빈 주소는 빈 키', () => {
    expect(complexKey('')).toBe('');
  });
});

describe('stripUnit', () => {
  test('동·호·층만 지우고 괄호는 남긴다', () => {
    expect(stripUnit('광주 북구 삼정로 7, 207동 403호(두암동, 주공2단지@)'))
      .toBe('광주 북구 삼정로 7, (두암동, 주공2단지@)');
  });

  test('꼬리 쉼표는 떼어낸다', () => {
    expect(stripUnit('광주광역시 북구 매곡로 92 제102동 1504호'))
      .toBe('광주광역시 북구 매곡로 92');
    expect(stripUnit('광주 북구 매곡로 92, 103동 204호')).toBe('광주 북구 매곡로 92');
  });

  test('층 표기도 지운다', () => {
    expect(stripUnit('광주 북구 첨단연신로 260 2 층')).toBe('광주 북구 첨단연신로 260');
  });

  test('빈 주소', () => {
    expect(stripUnit('')).toBe('');
  });
});

describe('unitSortKey / compareUnit', () => {
  const sortAddrs = (addrs: string[]): string[] =>
    addrs
      .map((address, i) => ({ address, i }))
      .sort((a, b) => compareUnitAddr(a.address, a.i, b.address, b.i))
      .map((x) => x.address);

  test('같은 단지 안에서 동 → 호 오름차순', () => {
    // optimizer.py docstring: 202동 406호 → 206동 311호 → 207동 403호
    const given = [
      '광주 북구 삼정로 7, 207동 403호(두암동, 주공2단지@)',
      '광주 북구 삼정로 7, 202동 406호(두암동,주공2단지@)',
      '광주 북구 삼정로 7, 206동 311호 (두암동, 주공2단지@)',
    ];
    expect(sortAddrs(given)).toEqual([
      '광주 북구 삼정로 7, 202동 406호(두암동,주공2단지@)',
      '광주 북구 삼정로 7, 206동 311호 (두암동, 주공2단지@)',
      '광주 북구 삼정로 7, 207동 403호(두암동, 주공2단지@)',
    ]);
  });

  test('같은 동이면 호 오름차순', () => {
    const given = ['가로 1, 101동 1202호', '가로 1, 101동 203호', '가로 1, 101동 1101호'];
    expect(sortAddrs(given)).toEqual([
      '가로 1, 101동 203호',
      '가로 1, 101동 1101호',
      '가로 1, 101동 1202호',
    ]);
  });

  test('숫자 동 → 영문 동 → 동 없음 순', () => {
    const given = [
      '광주 북구 하백로 46번길 9, 809호(매곡동, 부림@)', // 동 없음
      '광주 북구 안산로 38-3, A동 102호 (오치2동, 영진하우스)', // 영문 동
      '광주 북구 매곡로 92, 101동 105호(매곡동, 아남@)', // 숫자 동
    ];
    expect(sortAddrs(given).map((a) => unitSortKey(a, 0)[0])).toEqual([0, 1, 2]);
  });

  test('영문 동끼리는 문자 오름차순, 호를 못 읽으면 뒤로', () => {
    expect(sortAddrs(['나로 2, B동 201호', '나로 2, A동 999호', '나로 2, A동'])).toEqual([
      '나로 2, A동 999호',
      '나로 2, A동',
      '나로 2, B동 201호',
    ]);
  });

  test('키가 같으면 tiebreak로 결정된다', () => {
    const a = unitSortKey('가로 1, 101동 101호', 7);
    const b = unitSortKey('가로 1, 101동 101호', 3);
    expect(compareUnit(a, b)).toBe(1);
    expect(compareUnit(b, a)).toBe(-1);
    expect(compareUnit(a, a)).toBe(0);
  });
});

describe('Python fixture 대조 (scripts/gen_fixtures.py)', () => {
  const cases = addressFixture as unknown as AddressCase[];

  test('합성 주소 30건이 준비돼 있다', () => {
    expect(cases.length).toBeGreaterThanOrEqual(30);
  });

  test.each(cases.map((c, i) => [i, c.address, c] as const))(
    '[%i] %s',
    (_i, _addr, c) => {
      expect(parseUnit(c.address)).toEqual(c.parseUnit);
      expect(complexKey(c.address)).toBe(c.complexKey);
      expect(stripUnit(c.address)).toBe(c.stripUnit);
      expect(unitSortKey(c.address, c.unitSortKey[5])).toEqual(c.unitSortKey);
    },
  );
});

describe('complexName — 사람 이름 대신 건물 이름', () => {
  test('빈 주소', () => {
    expect(complexName('')).toBe('');
  });

  test('complexName은 동을 붙이지 않는다(같은 단지 여러 동을 한 이름으로)', () => {
    const a = '광주 북구 매곡로 92, 102동 106호(매곡동,아남@)';
    const b = '광주 북구 매곡로 92, 제201동 제1504호 (매곡동, 아남@)';
    expect(complexName(a)).toBe('아남@');
    expect(complexName(a)).toBe(complexName(b));
  });
});
