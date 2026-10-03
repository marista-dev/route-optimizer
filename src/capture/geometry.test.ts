import { describe, expect, it } from 'vitest';

import {
  KAKAO_LOGO_ZONE,
  MARKER_RADIUS_PX,
  clampRect,
  classifyPoint,
  classifyPoints,
  cropToVideo,
  formatOrderRanges,
  includesKakaoLogo,
  normalizeRect,
} from './geometry';

describe('cropToVideo', () => {
  it('가로·세로 비율을 따로 적용한다', () => {
    // DPR 2인데 공유 중 바 때문에 세로만 비율이 다른 경우
    const r = cropToVideo(
      { x: 100, y: 50, width: 200, height: 100 },
      { width: 1000, height: 800 },
      { width: 2000, height: 1200 },
    );
    expect(r).toEqual({ x: 200, y: 75, width: 400, height: 150 });
  });

  it('정수로 반올림한다', () => {
    const r = cropToVideo(
      { x: 10.3, y: 10.6, width: 20.2, height: 20.2 },
      { width: 100, height: 100 },
      { width: 150, height: 150 },
    );
    for (const v of Object.values(r)) expect(Number.isInteger(v)).toBe(true);
  });

  it('프레임 밖으로 나가면 잘라 낸다', () => {
    const r = cropToVideo(
      { x: -50, y: -50, width: 2000, height: 2000 },
      { width: 1000, height: 500 },
      { width: 1000, height: 500 },
    );
    expect(r).toEqual({ x: 0, y: 0, width: 1000, height: 500 });
  });

  it('완전히 밖이거나 크기가 0이어도 1px 이상을 돌려준다', () => {
    const r = cropToVideo(
      { x: 5000, y: 5000, width: 0, height: 0 },
      { width: 100, height: 100 },
      { width: 100, height: 100 },
    );
    expect(r.width).toBeGreaterThanOrEqual(1);
    expect(r.height).toBeGreaterThanOrEqual(1);
    expect(r.x + r.width).toBeLessThanOrEqual(100);
    expect(r.y + r.height).toBeLessThanOrEqual(100);
  });
});

describe('classifyPoint', () => {
  const rect = { x: 100, y: 100, width: 200, height: 100 };

  it('원 전체가 안이면 inside', () => {
    expect(classifyPoint({ x: 200, y: 150 }, rect, 20)).toBe('inside');
    expect(classifyPoint({ x: 120, y: 120 }, rect, 20)).toBe('inside');
  });

  it('경계에 걸치면 edge', () => {
    expect(classifyPoint({ x: 110, y: 150 }, rect, 20)).toBe('edge');
    expect(classifyPoint({ x: 90, y: 150 }, rect, 20)).toBe('edge');
  });

  it('전부 밖이면 outside', () => {
    expect(classifyPoint({ x: 50, y: 150 }, rect, 20)).toBe('outside');
    expect(classifyPoint({ x: 80, y: 150 }, rect, 20)).toBe('outside');
  });

  it('모서리 바깥 대각선은 거리로 판정한다', () => {
    // 모서리(100,100)에서 (15,15) 떨어진 점: 거리 ≈ 21.2 > 20
    expect(classifyPoint({ x: 85, y: 85 }, rect, 20)).toBe('outside');
    // (10,10): 거리 ≈ 14.1 < 20
    expect(classifyPoint({ x: 90, y: 90 }, rect, 20)).toBe('edge');
  });
});

describe('classifyPoints', () => {
  it('groupId별로 판정을 모은다', () => {
    const rect = { x: 0, y: 0, width: 100, height: 100 };
    const result = classifyPoints(
      [
        { groupId: 1, x: 50, y: 50 },
        { groupId: 2, x: 100, y: 50 },
        { groupId: 3, x: 300, y: 50 },
      ],
      rect,
      MARKER_RADIUS_PX,
    );
    expect(result).toEqual(
      new Map([
        [1, 'inside'],
        [2, 'edge'],
        [3, 'outside'],
      ]),
    );
  });
});

describe('formatOrderRanges', () => {
  it('연속 구간을 ~로 묶는다', () => {
    expect(formatOrderRanges([12, 13, 14, 20])).toBe('12~14, 20');
  });

  it('정렬하고 중복을 없앤다', () => {
    expect(formatOrderRanges([5, 3, 4, 4, 1])).toBe('1, 3~5');
  });

  it('빈 목록은 빈 문자열', () => {
    expect(formatOrderRanges([])).toBe('');
  });

  it('하나만 있으면 그 숫자', () => {
    expect(formatOrderRanges([7])).toBe('7');
  });
});

describe('normalizeRect', () => {
  it('어느 방향으로 끌어도 양수 크기', () => {
    expect(normalizeRect({ x: 50, y: 80 }, { x: 10, y: 20 })).toEqual({
      x: 10,
      y: 20,
      width: 40,
      height: 60,
    });
  });
});

describe('clampRect', () => {
  const bounds = { width: 500, height: 400 };

  it('밖으로 나간 사각형을 크기는 두고 안으로 민다', () => {
    expect(clampRect({ x: 450, y: -10, width: 100, height: 100 }, bounds, 40)).toEqual({
      x: 400,
      y: 0,
      width: 100,
      height: 100,
    });
  });

  it('최소 크기를 지킨다', () => {
    expect(clampRect({ x: 10, y: 10, width: 5, height: 5 }, bounds, 40)).toEqual({
      x: 10,
      y: 10,
      width: 40,
      height: 40,
    });
  });

  it('bounds보다 크면 bounds에 맞춘다', () => {
    expect(clampRect({ x: -100, y: -100, width: 900, height: 900 }, bounds, 40)).toEqual({
      x: 0,
      y: 0,
      width: 500,
      height: 400,
    });
  });
});

describe('includesKakaoLogo', () => {
  const container = { width: 1000, height: 700 };

  it('지도 전체는 로고를 포함한다', () => {
    expect(includesKakaoLogo({ x: 0, y: 0, ...container }, container)).toBe(true);
  });

  it('왼쪽 아래 로고 자리를 정확히 덮으면 포함', () => {
    const rect = {
      x: 0,
      y: container.height - KAKAO_LOGO_ZONE.height,
      width: KAKAO_LOGO_ZONE.width,
      height: KAKAO_LOGO_ZONE.height,
    };
    expect(includesKakaoLogo(rect, container)).toBe(true);
  });

  it('조금이라도 비켜나면 포함하지 않는다', () => {
    expect(includesKakaoLogo({ x: 10, y: 0, width: 990, height: 700 }, container)).toBe(false);
    expect(includesKakaoLogo({ x: 0, y: 0, width: 1000, height: 690 }, container)).toBe(false);
    expect(includesKakaoLogo({ x: 0, y: 0, width: 60, height: 700 }, container)).toBe(false);
  });
});
