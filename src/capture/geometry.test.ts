import { describe, expect, it } from 'vitest';

import {
  A4_RATIO,
  FRAME_SCALE_MAX,
  FRAME_SCALE_MIN,
  KAKAO_LOGO_ZONE,
  MARKER_RADIUS_PX,
  classifyPoint,
  classifyPoints,
  cropToVideo,
  fitA4Frame,
  formatOrderRanges,
  includesKakaoLogo,
  nextAnchor,
  planNextWindow,
  planPages,
  roundPan,
  scaleFrame,
  scaleFromCornerDrag,
} from './geometry';
import type { MarkerPoint, Rect } from './types';

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

describe('fitA4Frame', () => {
  it('세로: 높이 = 너비 × √2, 여백·하단 예약을 뺀 영역 가운데', () => {
    // 쓸 수 있는 영역: 1000-48 = 952 × 800-48-80 = 672
    const r = fitA4Frame({ width: 1000, height: 800 }, 'portrait', 80, 24);
    expect(r.height).toBe(672);
    expect(r.width).toBe(Math.floor(672 / A4_RATIO));
    expect(r.x).toBe(Math.round(24 + (952 - r.width) / 2));
    expect(r.y).toBe(24);
    expect(r.y + r.height).toBeLessThanOrEqual(800 - 80 - 24);
  });

  it('가로: 너비 = 높이 × √2, 높이가 먼저 꽉 찬다', () => {
    const r = fitA4Frame({ width: 1600, height: 800 }, 'landscape', 80, 24);
    expect(r.height).toBe(672);
    expect(r.width).toBe(Math.floor(672 * A4_RATIO));
    expect(r.x).toBe(Math.round(24 + (1552 - r.width) / 2));
  });

  it('가로: 폭이 좁으면 너비가 먼저 꽉 차고 세로 가운데', () => {
    const r = fitA4Frame({ width: 548, height: 1000 }, 'landscape', 0, 24);
    expect(r.width).toBe(500);
    expect(r.height).toBe(Math.floor(500 / A4_RATIO));
    expect(r.y).toBe(Math.round(24 + (952 - r.height) / 2));
  });

  it('너무 작거나 0인 컨테이너도 NaN 없이 최소 1px', () => {
    for (const c of [
      { width: 0, height: 0 },
      { width: 10, height: 10 },
      { width: -5, height: Number.NaN },
    ]) {
      const r = fitA4Frame(c, 'portrait', 80, 24);
      expect(r.width).toBeGreaterThanOrEqual(1);
      expect(r.height).toBeGreaterThanOrEqual(1);
      for (const v of Object.values(r)) expect(Number.isFinite(v)).toBe(true);
    }
  });
});

const R = 19;
const FRAME: Rect = { x: 100, y: 50, width: 400, height: 560 };

/** 창(프레임 크기)에 온전히 들어가는지. */
function insideFrame(p: { x: number; y: number }, frame: Rect): boolean {
  return classifyPoint(p, frame, R) === 'inside';
}

/** plan대로 panBy한 뒤의 화면 좌표. */
function shift(points: readonly MarkerPoint[], dx: number, dy: number): MarkerPoint[] {
  return points.map((p) => ({ groupId: p.groupId, x: p.x - dx, y: p.y - dy }));
}

/** 결정적 의사 난수. */
function rng(seed: number): () => number {
  let s = seed >>> 0;
  return () => {
    s = (s * 1664525 + 1013904223) >>> 0;
    return s / 2 ** 32;
  };
}

describe('nextAnchor', () => {
  it('순번이 가장 빠른 그룹, 순번 없는 그룹은 맨 뒤', () => {
    const order = new Map([
      [10, 5],
      [11, 2],
      [12, 9],
    ]);
    expect(nextAnchor(new Set([10, 11, 12, 99]), (id) => order.get(id))).toBe(11);
    expect(nextAnchor(new Set([99, 12]), (id) => order.get(id))).toBe(12);
    expect(nextAnchor(new Set([99, 98]), (id) => order.get(id))).toBe(98);
    expect(nextAnchor(new Set(), (id) => order.get(id))).toBeUndefined();
  });
});

describe('planNextWindow', () => {
  it('anchor가 없으면 null', () => {
    const pts: MarkerPoint[] = [{ groupId: 1, x: 200, y: 200 }];
    expect(planNextWindow(pts, new Set([1]), 7, FRAME, R)).toBeNull();
  });

  it('고립된 마커 하나: 안에 들어오게 옮기고 이동은 최소', () => {
    const pts: MarkerPoint[] = [{ groupId: 1, x: 2000, y: -300 }];
    const plan = planNextWindow(pts, new Set([1]), 1, FRAME, R)!;
    expect(plan.groupIds).toEqual([1]);
    const after = shift(pts, plan.dx, plan.dy);
    expect(insideFrame(after[0], FRAME)).toBe(true);
    // 최소 이동이면 마커는 프레임 오른쪽 위 가장자리에 딱 붙는다.
    expect(after[0].x).toBeCloseTo(FRAME.x + FRAME.width - R);
    expect(after[0].y).toBeCloseTo(FRAME.y + R);
  });

  it('이미 프레임 안에 다 있으면 움직이지 않는다', () => {
    const pts: MarkerPoint[] = [
      { groupId: 1, x: 200, y: 200 },
      { groupId: 2, x: 300, y: 400 },
    ];
    const plan = planNextWindow(pts, new Set([1, 2]), 1, FRAME, R)!;
    expect(plan.dx).toBe(0);
    expect(plan.dy).toBe(0);
    expect(plan.groupIds.sort()).toEqual([1, 2]);
  });

  it('anchor를 지키면서 남은 마커가 많은 쪽으로 간다', () => {
    // anchor는 왼쪽, 왼쪽 바깥에 2개, 오른쪽(프레임 너비 안쪽 거리)에 5개
    const pts: MarkerPoint[] = [
      { groupId: 1, x: 1000, y: 300 },
      { groupId: 2, x: 700, y: 300 },
      { groupId: 3, x: 720, y: 320 },
      ...[4, 5, 6, 7, 8].map((id, i) => ({ groupId: id, x: 1300 + i * 10, y: 300 + i * 20 })),
    ];
    const plan = planNextWindow(pts, new Set(pts.map((p) => p.groupId)), 1, FRAME, R)!;
    expect(plan.groupIds.sort()).toEqual([1, 4, 5, 6, 7, 8]);
    const after = shift(pts, plan.dx, plan.dy);
    for (const p of after) {
      expect(insideFrame(p, FRAME)).toBe(plan.groupIds.includes(p.groupId));
    }
  });

  it('찍은(남지 않은) 마커는 세지 않는다', () => {
    const pts: MarkerPoint[] = [
      { groupId: 1, x: 1000, y: 300 },
      { groupId: 2, x: 700, y: 300 },
      { groupId: 3, x: 720, y: 320 },
      { groupId: 4, x: 1300, y: 300 },
    ];
    const plan = planNextWindow(pts, new Set([1, 4]), 1, FRAME, R)!;
    expect(plan.groupIds.sort()).toEqual([1, 4]);
  });

  it('마커가 프레임보다 크면 anchor를 가운데 둔다', () => {
    const pts: MarkerPoint[] = [{ groupId: 1, x: 50, y: 60 }];
    const tiny: Rect = { x: 10, y: 10, width: 20, height: 20 };
    const plan = planNextWindow(pts, new Set([1]), 1, tiny, R)!;
    expect(plan).toEqual({ dx: 50 - 10 - 10, dy: 60 - 10 - 10, groupIds: [1] });
  });

  it('마커 500개도 금방 끝난다', () => {
    const rand = rng(42);
    const pts: MarkerPoint[] = Array.from({ length: 500 }, (_, i) => ({
      groupId: i + 1,
      x: rand() * 3000 - 1000,
      y: rand() * 3000 - 1000,
    }));
    const t0 = performance.now();
    const plan = planNextWindow(pts, new Set(pts.map((p) => p.groupId)), 1, FRAME, R);
    expect(plan).not.toBeNull();
    // 넉넉한 상한(CI 편차 고려). 실제로는 수십 ms.
    expect(performance.now() - t0).toBeLessThan(2000);
  });
});

describe('planPages', () => {
  /** 계획을 순서대로 적용하며 각 장에 실제로 들어가는지 확인하고, 찍힌 id를 모은다. */
  function replay(pts: readonly MarkerPoint[], pages: ReturnType<typeof planPages>) {
    let cur = [...pts];
    const covered = new Set<number>();
    for (const page of pages) {
      cur = shift(cur, page.dx, page.dy);
      for (const id of page.groupIds) {
        const p = cur.find((q) => q.groupId === id)!;
        expect(insideFrame(p, FRAME)).toBe(true);
        expect(covered.has(id)).toBe(false);
        covered.add(id);
      }
    }
    return covered;
  }

  it('모든 마커가 한 프레임에 들어가면 1장', () => {
    const pts: MarkerPoint[] = [
      { groupId: 1, x: 900, y: 900 },
      { groupId: 2, x: 1000, y: 1100 },
      { groupId: 3, x: 1200, y: 1300 },
    ];
    const pages = planPages(pts, new Set([1, 2, 3]), (id) => id, FRAME, R);
    expect(pages).toHaveLength(1);
    expect(pages[0].groupIds.sort()).toEqual([1, 2, 3]);
  });

  it('1~20·100~120이 한 곳, 21~99가 다른 곳에 있으면 첫 장 뒤 21로 넘어간다', () => {
    const rand = rng(7);
    const pts: MarkerPoint[] = [];
    // 묶음 A: 프레임 안 (150~450, 100~560)
    for (let o = 1; o <= 20; o++) pts.push({ groupId: o, x: 150 + rand() * 300, y: 100 + rand() * 460 });
    for (let o = 100; o <= 120; o++) pts.push({ groupId: o, x: 150 + rand() * 300, y: 100 + rand() * 460 });
    // 묶음 B: 오른쪽 멀리 (3000~3300, 200~600)
    for (let o = 21; o <= 99; o++) pts.push({ groupId: o, x: 3000 + rand() * 300, y: 200 + rand() * 400 });

    const all = new Set(pts.map((p) => p.groupId));
    const first = planNextWindow(pts, all, 1, FRAME, R)!;
    const firstIds = new Set(first.groupIds);
    for (let o = 1; o <= 20; o++) expect(firstIds.has(o)).toBe(true);
    for (let o = 100; o <= 120; o++) expect(firstIds.has(o)).toBe(true);
    expect(firstIds.has(21)).toBe(false);

    const left = new Set([...all].filter((id) => !firstIds.has(id)));
    expect(nextAnchor(left, (id) => id)).toBe(21);

    const moved = shift(pts, first.dx, first.dy);
    const second = planNextWindow(moved, left, 21, FRAME, R)!;
    expect(second.dx).toBeGreaterThan(2000); // 묶음 B 쪽(오른쪽)으로
    expect(second.groupIds).toContain(21);
    expect(second.groupIds.every((id) => id >= 21 && id <= 99)).toBe(true);

    const pages = planPages(pts, all, (id) => id, FRAME, R);
    expect(pages[0].groupIds.sort((a, b) => a - b)).toEqual(
      first.groupIds.sort((a, b) => a - b),
    );
    expect(pages[1].groupIds).toContain(21);
    expect(replay(pts, pages)).toEqual(all);
  });

  it('dx/dy는 앞 장 기준 누적 이동이고, 결국 남은 마커를 모두 덮는다', () => {
    const rand = rng(123);
    const pts: MarkerPoint[] = Array.from({ length: 150 }, (_, i) => ({
      groupId: i + 1,
      x: rand() * 4000 - 1500,
      y: rand() * 4000 - 1500,
    }));
    const remaining = new Set(pts.filter((p) => p.groupId % 5 !== 0).map((p) => p.groupId));
    const pages = planPages(pts, remaining, (id) => id, FRAME, R);
    expect(pages.length).toBeGreaterThan(1);
    expect(replay(pts, pages)).toEqual(remaining);
  });

  it('좌표 없는 그룹은 건너뛰고, maxPages에서 멈춘다', () => {
    const pts: MarkerPoint[] = [
      { groupId: 1, x: 0, y: 0 },
      { groupId: 2, x: 5000, y: 0 },
      { groupId: 3, x: 10000, y: 0 },
    ];
    expect(planPages(pts, new Set([1, 2, 3, 99]), (id) => id, FRAME, R)).toHaveLength(3);
    expect(planPages(pts, new Set([1, 2, 3]), (id) => id, FRAME, R, 2)).toHaveLength(2);
  });
});

describe('roundPan', () => {
  const frame: Rect = { x: 100, y: 100, width: 200, height: 300 };

  it('정수 값은 그대로 둔다', () => {
    expect(roundPan([], { dx: 12, dy: -7, groupIds: [] }, frame, 10)).toEqual({ dx: 12, dy: -7 });
  });

  it('정수로는 둘 다 담을 수 없어도 하나는 온전히 담는다', () => {
    // r=10. 마커1은 dx ≥ 0.5, 마커2는 dx ≤ 0.5여야 온전히 들어간다 → 정수 해는 없다.
    const points: MarkerPoint[] = [
      { groupId: 1, x: 290.5, y: 200 },
      { groupId: 2, x: 110.5, y: 200 },
    ];
    const r = roundPan(points, { dx: 0.5, dy: 0, groupIds: [1, 2] }, frame, 10);
    const placed = classifyPoints(
      points.map((p) => ({ ...p, x: p.x - r.dx, y: p.y - r.dy })),
      frame,
      10,
    );
    expect([...placed.values()].filter((v) => v === 'inside')).toHaveLength(1);
    expect(Number.isInteger(r.dx)).toBe(true);
  });

  it('그냥 반올림하면 걸치는 경우 담기는 쪽을 고른다', () => {
    // 위쪽 경계(100)에 r=10 원이 붙으려면 y − dy ≥ 110 → dy ≤ −0.5.
    // Math.round(−0.5)는 0이라 109.5에서 걸친다. 내림(−1)을 골라야 한다.
    const points: MarkerPoint[] = [{ groupId: 1, x: 200, y: 109.5 }];
    expect(roundPan(points, { dx: 0, dy: -0.5, groupIds: [1] }, frame, 10)).toEqual({
      dx: 0,
      dy: -1,
    });
  });
});

describe('scaleFrame', () => {
  const portrait: Rect = { x: 20, y: 10, width: 500, height: 707 };
  const landscape: Rect = { x: 0, y: 30, width: 990, height: 700 };
  const center = (r: Rect) => ({ x: r.x + r.width / 2, y: r.y + r.height / 2 });
  const inside = (r: Rect, outer: Rect) =>
    r.x >= outer.x &&
    r.y >= outer.y &&
    r.x + r.width <= outer.x + outer.width &&
    r.y + r.height <= outer.y + outer.height;

  it('배율 1은 최대 프레임 그대로다', () => {
    expect(scaleFrame(portrait, 1)).toEqual(portrait);
    expect(FRAME_SCALE_MAX).toBe(1);
  });

  it('세로: 중심과 비율을 지키며 줄인다', () => {
    const r = scaleFrame(portrait, 0.5);
    expect(r.width).toBe(250);
    expect(r.height).toBe(353);
    expect(Math.abs(center(r).x - center(portrait).x)).toBeLessThanOrEqual(0.5);
    expect(Math.abs(center(r).y - center(portrait).y)).toBeLessThanOrEqual(1);
    expect(Math.abs(r.height / r.width - portrait.height / portrait.width)).toBeLessThan(0.01);
    expect(inside(r, portrait)).toBe(true);
    for (const v of Object.values(r)) expect(Number.isInteger(v)).toBe(true);
  });

  it('가로: 중심과 비율을 지키며 줄인다', () => {
    const r = scaleFrame(landscape, 0.7);
    expect(r.width).toBe(693);
    expect(r.height).toBe(490);
    expect(Math.abs(center(r).x - center(landscape).x)).toBeLessThanOrEqual(1);
    expect(Math.abs(center(r).y - center(landscape).y)).toBeLessThanOrEqual(1);
    expect(inside(r, landscape)).toBe(true);
  });

  it('배율은 [MIN, 1]로 잘린다', () => {
    expect(scaleFrame(portrait, 2)).toEqual(portrait);
    const big: Rect = { x: 0, y: 0, width: 1000, height: 1414 };
    const r = scaleFrame(big, 0.1);
    expect(r.width).toBe(Math.floor(1000 * FRAME_SCALE_MIN));
    expect(r.height).toBe(Math.floor(1414 * FRAME_SCALE_MIN));
  });

  it('짧은 변이 minShort 아래로 내려가지 않는다', () => {
    // 짧은 변 300 → 0.4배면 120이라 160/300 배까지만 줄인다
    const r = scaleFrame({ x: 0, y: 0, width: 300, height: 424 }, 0.4);
    expect(r.width).toBe(160);
    const l = scaleFrame({ x: 0, y: 0, width: 424, height: 300 }, 0.4, 200);
    expect(l.height).toBe(200);
  });

  it('최대 프레임이 minShort보다 작으면 그대로 돌려준다', () => {
    const tiny: Rect = { x: 5, y: 5, width: 100, height: 141 };
    expect(scaleFrame(tiny, 0.4)).toEqual(tiny);
    expect(scaleFrame({ x: 0, y: 0, width: 1, height: 1 }, 0.5)).toEqual({ x: 0, y: 0, width: 1, height: 1 });
  });

  it('NaN 배율은 1로 본다', () => {
    expect(scaleFrame(portrait, Number.NaN)).toEqual(portrait);
  });
});

describe('scaleFromCornerDrag', () => {
  const portrait: Rect = { x: 0, y: 0, width: 500, height: 708 };
  const landscape: Rect = { x: 100, y: 0, width: 708, height: 500 };

  it('모서리를 그대로 두면 1', () => {
    expect(scaleFromCornerDrag(portrait, { x: 500, y: 708 })).toBe(1);
    expect(scaleFromCornerDrag(portrait, { x: 0, y: 0 })).toBe(1);
  });

  it('중심 쪽으로 끌면 가로·세로 중 큰 비율을 쓴다', () => {
    // 중심 (250, 354). 오른쪽 아래 모서리를 (400, 450)으로: x 150/250=0.6, y 96/354≈0.27
    expect(scaleFromCornerDrag(portrait, { x: 400, y: 450 })).toBeCloseTo(0.6);
    // 왼쪽 위 모서리를 (200, 100)으로: x 50/250=0.2, y 254/354≈0.717
    expect(scaleFromCornerDrag(portrait, { x: 200, y: 100 })).toBeCloseTo(254 / 354);
  });

  it('가로 프레임에서도 같은 규칙이다', () => {
    // 중심 (454, 250). (754, 300): x 300/354≈0.847, y 50/250=0.2
    expect(scaleFromCornerDrag(landscape, { x: 754, y: 300 })).toBeCloseTo(300 / 354);
  });

  it('범위를 넘으면 잘린다', () => {
    expect(scaleFromCornerDrag(portrait, { x: 900, y: 900 })).toBe(1);
    // 중심에 가까우면 MIN(짧은 변 500×0.4=200 ≥ 160이라 MIN이 하한)
    expect(scaleFromCornerDrag(portrait, { x: 250, y: 354 })).toBe(FRAME_SCALE_MIN);
    // 짧은 변 300이면 160/300이 하한
    expect(scaleFromCornerDrag({ x: 0, y: 0, width: 300, height: 424 }, { x: 150, y: 212 })).toBeCloseTo(
      160 / 300,
    );
  });

  it('최대 프레임이 minShort보다 작으면 늘 1', () => {
    expect(scaleFromCornerDrag({ x: 0, y: 0, width: 100, height: 141 }, { x: 50, y: 70 })).toBe(1);
  });

  it('결과를 scaleFrame에 넣으면 끈 모서리가 포인터 근처에 온다', () => {
    const s = scaleFromCornerDrag(portrait, { x: 400, y: 600 });
    const r = scaleFrame(portrait, s);
    expect(Math.abs(r.y + r.height - 600)).toBeLessThanOrEqual(1);
  });
});
