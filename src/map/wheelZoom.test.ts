import { describe, expect, it } from 'vitest';

import { WHEEL_LINE_PX, clampLevel, createWheelAccumulator, normalizeWheelDelta } from './wheelZoom';

describe('normalizeWheelDelta', () => {
  it('px 단위는 그대로', () => {
    expect(normalizeWheelDelta({ deltaY: 53, deltaMode: 0 }, 800)).toBe(53);
  });
  it('라인 단위는 ×16', () => {
    expect(normalizeWheelDelta({ deltaY: 3, deltaMode: 1 }, 800)).toBe(3 * WHEEL_LINE_PX);
    expect(WHEEL_LINE_PX).toBe(16);
  });
  it('페이지 단위는 ×높이', () => {
    expect(normalizeWheelDelta({ deltaY: -1, deltaMode: 2 }, 720)).toBe(-720);
  });
  it('NaN은 0', () => {
    expect(normalizeWheelDelta({ deltaY: Number.NaN, deltaMode: 0 }, 800)).toBe(0);
    expect(normalizeWheelDelta({ deltaY: 1, deltaMode: 2 }, Number.NaN)).toBe(0);
  });
});

describe('createWheelAccumulator', () => {
  it('마우스 휠 한 칸(100px)에 정확히 한 단계, 양수는 +1(축소)', () => {
    const step = createWheelAccumulator({ threshold: 100 });
    expect(step(100, 0)).toBe(1);
    expect(step(-100, 1000)).toBe(-1);
  });

  it('작은 입력은 모았다가 임계값에서 한 단계', () => {
    const step = createWheelAccumulator({ threshold: 100 });
    expect(step(30, 0)).toBe(0);
    expect(step(30, 10)).toBe(0);
    expect(step(30, 20)).toBe(0);
    expect(step(30, 30)).toBe(1);
  });

  it('한 번에 크게 와도 한 단계뿐이다', () => {
    const step = createWheelAccumulator({ threshold: 100 });
    expect(step(1000, 0)).toBe(1);
  });

  it('방향이 바뀌면 누적을 버린다', () => {
    const step = createWheelAccumulator({ threshold: 100 });
    expect(step(80, 0)).toBe(0);
    expect(step(-30, 10)).toBe(0);
    expect(step(-60, 20)).toBe(0);
    expect(step(-20, 30)).toBe(-1);
  });

  it('단계 직후 쿨다운 동안은 입력을 버리고, 지나면 처음부터 센다', () => {
    const step = createWheelAccumulator({ threshold: 100, cooldownMs: 250 });
    expect(step(100, 0)).toBe(1);
    expect(step(100, 100)).toBe(0);
    expect(step(100, 249)).toBe(0);
    expect(step(60, 260)).toBe(0); // 쿨다운 중 입력은 쌓이지 않았다
    expect(step(40, 270)).toBe(1);
  });

  it('트랙패드 쓸기·관성(작은 입력 연속)은 오래 이어져도 한 단계뿐이다', () => {
    const step = createWheelAccumulator({ threshold: 100 });
    let steps = 0;
    // 16ms마다 25~40px씩 1.5초 동안(쓸기 + 관성)
    for (let t = 0; t <= 1500; t += 16) steps += Math.abs(step(t < 600 ? 40 : 25, t));
    expect(steps).toBe(1);
    // 손을 떼고 쿨다운 이상 쉰 뒤의 새 쓸기는 다시 한 단계
    for (let t = 1800; t <= 2100; t += 16) steps += Math.abs(step(40, t));
    expect(steps).toBe(2);
  });

  it('마우스 휠을 빠르게 돌리면(한 칸 = 100px) 쿨다운마다 한 단계씩 나아간다', () => {
    const step = createWheelAccumulator({ threshold: 100, cooldownMs: 250 });
    let steps = 0;
    // 50ms마다 한 칸씩 1초 동안: 0·250(쿨다운 뒤 첫 칸)·500·750·1000ms
    for (let t = 0; t <= 1000; t += 50) steps += step(100, t);
    expect(steps).toBe(5);
  });

  it('오래 쉬면 남은 누적값을 비운다', () => {
    const step = createWheelAccumulator({ threshold: 100, idleResetMs: 400 });
    expect(step(90, 0)).toBe(0);
    expect(step(20, 1000)).toBe(0);
    expect(step(80, 1010)).toBe(1);
  });

  it('기본값: 파이어폭스 휠 한 칸(3줄=48px)도 한 단계, 트랙패드 쓸기는 여전히 한 단계', () => {
    const step = createWheelAccumulator();
    expect(step(normalizeWheelDelta({ deltaY: 3, deltaMode: 1 }, 800), 0)).toBe(1);
    let steps = 0;
    for (let t = 1000; t <= 2500; t += 16) steps += Math.abs(step(t < 1600 ? 40 : 25, t));
    expect(steps).toBe(1);
  });

  it('임계값을 바꿀 수 있다', () => {
    const step = createWheelAccumulator({ threshold: 50 });
    expect(step(50, 0)).toBe(1);
  });

  it('0·NaN 입력은 무시한다', () => {
    const step = createWheelAccumulator({ threshold: 100 });
    expect(step(0, 0)).toBe(0);
    expect(step(Number.NaN, 1)).toBe(0);
    expect(step(100, 2)).toBe(1);
  });
});

describe('clampLevel', () => {
  it('범위로 자르고 반올림한다', () => {
    expect(clampLevel(0, 1, 14)).toBe(1);
    expect(clampLevel(15, 1, 14)).toBe(14);
    expect(clampLevel(6.4, 1, 14)).toBe(6);
  });
});
