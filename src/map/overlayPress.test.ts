import { describe, expect, it } from 'vitest';

import { createRepeatGuard, exceededSlop, isRepeatClick } from './overlayPress';

describe('exceededSlop — 클릭과 끌기를 가른다', () => {
  const start = { x: 100, y: 100 };

  it('제자리면 끌기가 아니다', () => {
    expect(exceededSlop(start, start, 5)).toBe(false);
  });

  it('정확히 slop만큼은 아직 클릭이다', () => {
    expect(exceededSlop(start, { x: 105, y: 100 }, 5)).toBe(false);
    // 3-4-5 삼각형: 대각선 거리 5
    expect(exceededSlop(start, { x: 103, y: 96 }, 5)).toBe(false);
  });

  it('slop을 넘으면 끌기다(방향 무관)', () => {
    expect(exceededSlop(start, { x: 106, y: 100 }, 5)).toBe(true);
    expect(exceededSlop(start, { x: 100, y: 94 }, 5)).toBe(true);
    expect(exceededSlop(start, { x: 96, y: 104 }, 5)).toBe(true);
  });

  it('slop 0이면 조금만 움직여도 끌기다', () => {
    expect(exceededSlop(start, { x: 100.5, y: 100 }, 0)).toBe(true);
  });
});

describe('isRepeatClick — 더블클릭의 두 번째 이후', () => {
  it('첫 클릭과 키보드·합성 클릭(0)은 반복이 아니다', () => {
    expect(isRepeatClick(0)).toBe(false);
    expect(isRepeatClick(1)).toBe(false);
  });

  it('두 번째·세 번째 클릭은 반복이다', () => {
    expect(isRepeatClick(2)).toBe(true);
    expect(isRepeatClick(3)).toBe(true);
  });
});

describe('createRepeatGuard — 같은 대상을 짧은 시간 안에 다시 누르면 무시', () => {
  it('처음 누르는 것은 받는다', () => {
    const ignore = createRepeatGuard<number>(300);
    expect(ignore(1, 1000)).toBe(false);
  });

  it('창 안에 다시 누르면 무시하고, 창이 지나면 받는다', () => {
    const ignore = createRepeatGuard<number>(300);
    expect(ignore(1, 1000)).toBe(false);
    expect(ignore(1, 1200)).toBe(true);
    // 무시된 클릭도 시각을 갱신한다 — 1200 기준 300ms 뒤부터 받는다.
    expect(ignore(1, 1400)).toBe(true);
    expect(ignore(1, 1700)).toBe(false);
  });

  it('경계: 정확히 windowMs가 지나면 받는다', () => {
    const ignore = createRepeatGuard<number>(300);
    ignore(1, 0);
    expect(ignore(1, 300)).toBe(false);
  });

  it('대상(key)마다 따로 센다', () => {
    const ignore = createRepeatGuard<number>(300);
    expect(ignore(1, 1000)).toBe(false);
    expect(ignore(2, 1050)).toBe(false);
    expect(ignore(1, 1100)).toBe(true);
    expect(ignore(2, 1500)).toBe(false);
  });

  it('시계가 뒤로 가면(다른 시간 기준이 섞여도) 무시하지 않는다', () => {
    const ignore = createRepeatGuard<string>(300);
    ignore('a', 5000);
    expect(ignore('a', 100)).toBe(false);
  });
});
