/**
 * hull.test.ts — 클러스터 다각형(계획 8절).
 * 점이 1개든 2개든 항상 면적 있는 다각형이 나오고, 원본 마커가 경계에 걸리지
 * 않아야 한다(엄격히 내부).
 */

import { describe, expect, test } from 'vitest';

import type { LatLng } from '../types';
import {
  BASE_HULL_RADIUS_M,
  MIN_HULL_RADIUS_M,
  bufferedHull,
  convexHull,
  hullRadiusM,
  spreadM,
} from './hull';

/** 볼록 다각형 내부에 엄격히 들어 있는지 — 모든 변에 대한 외적 부호가 같아야 한다. */
function strictlyInside(polygon: LatLng[], p: LatLng): boolean {
  if (polygon.length < 3) return false;
  let sign = 0;
  for (let i = 0; i < polygon.length; i++) {
    const a = polygon[i];
    const b = polygon[(i + 1) % polygon.length];
    const cross = (b.lon - a.lon) * (p.lat - a.lat) - (b.lat - a.lat) * (p.lon - a.lon);
    if (cross === 0) return false; // 변 위 → 엄격한 내부가 아니다
    const s = cross > 0 ? 1 : -1;
    if (sign === 0) sign = s;
    else if (sign !== s) return false;
  }
  return true;
}

/** 신발끈 공식 면적(제곱도). 0이면 면적이 없다. */
function area(polygon: LatLng[]): number {
  let acc = 0;
  for (let i = 0; i < polygon.length; i++) {
    const a = polygon[i];
    const b = polygon[(i + 1) % polygon.length];
    acc += a.lon * b.lat - b.lon * a.lat;
  }
  return Math.abs(acc) / 2;
}

describe('bufferedHull', () => {
  test('점 1개 — 8각형이 나오고 원본 점은 내부', () => {
    const p: LatLng = { lat: 35.2, lon: 126.85 };
    const hull = bufferedHull([p]);
    expect(hull.length).toBeGreaterThanOrEqual(8);
    expect(area(hull)).toBeGreaterThan(0);
    expect(strictlyInside(hull, p)).toBe(true);
  });

  test('점 2개 — 면적이 있고 두 점 모두 내부', () => {
    const pts: LatLng[] = [
      { lat: 35.2, lon: 126.85 },
      { lat: 35.2009, lon: 126.8511 },
    ];
    const hull = bufferedHull(pts);
    expect(hull.length).toBeGreaterThanOrEqual(8);
    expect(area(hull)).toBeGreaterThan(0);
    for (const p of pts) expect(strictlyInside(hull, p)).toBe(true);
  });

  test('일직선 위 점 2개도 면적이 있다', () => {
    const pts: LatLng[] = [
      { lat: 35.2, lon: 126.85 },
      { lat: 35.2, lon: 126.8522 },
    ];
    const hull = bufferedHull(pts);
    expect(area(hull)).toBeGreaterThan(0);
    for (const p of pts) expect(strictlyInside(hull, p)).toBe(true);
  });

  test('점 5개 — 전부 내부', () => {
    const pts: LatLng[] = [
      { lat: 35.2, lon: 126.85 },
      { lat: 35.2009, lon: 126.85 },
      { lat: 35.2018, lon: 126.8511 },
      { lat: 35.2005, lon: 126.8522 },
      { lat: 35.1995, lon: 126.8509 },
    ];
    const hull = bufferedHull(pts);
    expect(hull.length).toBeGreaterThanOrEqual(8);
    expect(area(hull)).toBeGreaterThan(0);
    for (const p of pts) expect(strictlyInside(hull, p)).toBe(true);
  });

  test('반경이 커지면 다각형도 커진다', () => {
    const pts: LatLng[] = [{ lat: 35.2, lon: 126.85 }];
    expect(area(bufferedHull(pts, 80))).toBeGreaterThan(area(bufferedHull(pts, 40)));
  });

  test('기본 반경은 40m — 중심에서 약 40m 떨어진다', () => {
    const hull = bufferedHull([{ lat: 35.2, lon: 126.85 }]);
    const north = hull.reduce((a, b) => (a.lat > b.lat ? a : b));
    expect((north.lat - 35.2) * 111320).toBeCloseTo(40, 6);
  });

  test('빈 입력', () => {
    expect(bufferedHull([])).toEqual([]);
  });
});

describe('convexHull', () => {
  test('내부 점은 버린다', () => {
    const square: LatLng[] = [
      { lat: 0, lon: 0 }, { lat: 0, lon: 1 }, { lat: 1, lon: 1 }, { lat: 1, lon: 0 },
      { lat: 0.5, lon: 0.5 }, // 내부
    ];
    expect(convexHull(square)).toHaveLength(4);
  });

  test('일직선 위 중간 점은 버린다', () => {
    const line: LatLng[] = [
      { lat: 0, lon: 0 }, { lat: 0, lon: 1 }, { lat: 0, lon: 2 },
    ];
    expect(convexHull(line).length).toBeLessThanOrEqual(2);
  });

  test('중복 좌표는 하나로', () => {
    const dup: LatLng[] = [{ lat: 1, lon: 1 }, { lat: 1, lon: 1 }];
    expect(convexHull(dup)).toEqual([{ lat: 1, lon: 1 }]);
  });
});

describe('hullRadiusM — 작은 클러스터일수록 크게', () => {
  const at = (dLat: number, dLon: number): LatLng => ({ lat: 35.2 + dLat, lon: 126.85 + dLon });

  test('점이 없거나 1개면 최소 반경(60m)', () => {
    expect(hullRadiusM([])).toBe(MIN_HULL_RADIUS_M);
    expect(hullRadiusM([at(0, 0)])).toBe(MIN_HULL_RADIUS_M);
    expect(MIN_HULL_RADIUS_M).toBeGreaterThan(BASE_HULL_RADIUS_M);
  });

  test('같은 자리에 겹친 점들도 최소 반경', () => {
    expect(hullRadiusM([at(0, 0), at(0, 0), at(0, 0)])).toBe(MIN_HULL_RADIUS_M);
  });

  test('200m 이상 퍼지면 기본 반경(40m)으로 돌아온다', () => {
    // 위도 0.0027도 ≈ 300m
    expect(hullRadiusM([at(0, 0), at(0.0027, 0)])).toBe(BASE_HULL_RADIUS_M);
  });

  test('중간 크기는 두 값 사이에서 선형으로 줄어든다', () => {
    // 100m 퍼짐 → 60 + (40-60) * 0.5 = 50m
    const r = hullRadiusM([at(0, 0), at(100 / 111320, 0)]);
    expect(r).toBeCloseTo(50, 6);
  });

  test('퍼질수록 반경은 단조 감소하고 항상 [40, 60] 안', () => {
    let prev = Infinity;
    for (const meters of [0, 25, 50, 100, 150, 200, 400, 2000]) {
      const r = hullRadiusM([at(0, 0), at(meters / 111320, 0)]);
      expect(r).toBeLessThanOrEqual(MIN_HULL_RADIUS_M);
      expect(r).toBeGreaterThanOrEqual(BASE_HULL_RADIUS_M);
      expect(r).toBeLessThanOrEqual(prev);
      prev = r;
    }
  });

  test('반경을 적용해도 원본 점은 다각형 내부에 남는다', () => {
    const pts = [at(0, 0), at(0.0004, 0.0004)];
    const hull = bufferedHull(pts, hullRadiusM(pts));
    for (const p of pts) expect(strictlyInside(hull, p)).toBe(true);
  });

  test('1건 클러스터는 기본 반경보다 넓어진다', () => {
    const one = [at(0, 0)];
    expect(area(bufferedHull(one, hullRadiusM(one)))).toBeGreaterThan(area(bufferedHull(one)));
  });
});

describe('spreadM', () => {
  test('점 0~1개는 0', () => {
    expect(spreadM([])).toBe(0);
    expect(spreadM([{ lat: 35.2, lon: 126.85 }])).toBe(0);
  });

  test('남북 100m는 약 100m', () => {
    expect(
      spreadM([
        { lat: 35.2, lon: 126.85 },
        { lat: 35.2 + 100 / 111320, lon: 126.85 },
      ]),
    ).toBeCloseTo(100, 6);
  });
});
