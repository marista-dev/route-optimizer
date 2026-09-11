/**
 * hull.ts — 클러스터 다각형 좌표 생성(계획 8절).
 *
 * 배송지 좌표를 각각 반경 `radiusM` 원의 8개 점으로 부풀린 뒤 전체의 볼록껍질을
 * 구한다. 점이 1개든 2개든 항상 면적 있는 다각형이 나오고, 마커가 다각형
 * 경계에 걸리지 않는다.
 */

import type { LatLng } from '../types';

/** 위도 1도의 길이(m). 경도는 여기에 cos(위도)를 곱해 환산한다. */
const M_PER_DEG_LAT = 111320;

/** 넓게 퍼진 클러스터의 버퍼 반경(m). 다각형이 실제 범위보다 커지지 않게 하는 하한이다. */
export const BASE_HULL_RADIUS_M = 40;

/**
 * 점이 한곳에 모인(특히 1건짜리) 클러스터의 버퍼 반경(m).
 * 40m짜리 원은 실사용 배율에서 마커에 완전히 가려 클릭이 빗나간다.
 */
export const MIN_HULL_RADIUS_M = 60;

/** 이만큼(m) 퍼진 클러스터는 더 부풀릴 필요가 없다. */
const SPREAD_FULL_M = 200;

/**
 * 좌표들이 퍼진 정도(m) — 위경도 bounding box의 대각선 길이.
 * 정확한 최대 거리가 아니라 "얼마나 작은 클러스터인가"를 재는 값이다.
 */
export function spreadM(points: LatLng[]): number {
  if (points.length <= 1) return 0;
  let minLat = Infinity;
  let maxLat = -Infinity;
  let minLon = Infinity;
  let maxLon = -Infinity;
  for (const p of points) {
    if (p.lat < minLat) minLat = p.lat;
    if (p.lat > maxLat) maxLat = p.lat;
    if (p.lon < minLon) minLon = p.lon;
    if (p.lon > maxLon) maxLon = p.lon;
  }
  const midLat = (minLat + maxLat) / 2;
  const cosLat = Math.max(Math.abs(Math.cos((midLat * Math.PI) / 180)), 1e-6);
  const dy = (maxLat - minLat) * M_PER_DEG_LAT;
  const dx = (maxLon - minLon) * M_PER_DEG_LAT * cosLat;
  return Math.sqrt(dx * dx + dy * dy);
}

/**
 * 클러스터 크기에 맞춘 버퍼 반경(m) — 계획 8절 + 현장 피드백(단일 클러스터 클릭 미스).
 *
 * 작은 클러스터일수록 크게(최대 {@link MIN_HULL_RADIUS_M}) 부풀리고, 200m 이상
 * 퍼진 클러스터는 {@link BASE_HULL_RADIUS_M}으로 되돌린다. 넓은 클러스터를 더
 * 부풀리면 실제보다 큰 영역을 주장하게 되므로 반대 방향으로 가지 않는다.
 *
 * 반경은 "그려지는 모양"만 손본다. 클릭 자체는 `ClusterLayer`가 중심에 얹는
 * 고정 크기 히트 타깃이 보장한다(배율과 무관하게 44px).
 */
export function hullRadiusM(points: LatLng[]): number {
  const t = Math.min(1, spreadM(points) / SPREAD_FULL_M);
  return MIN_HULL_RADIUS_M + (BASE_HULL_RADIUS_M - MIN_HULL_RADIUS_M) * t;
}

/**
 * 좌표 목록을 반경 `radiusM`만큼 부풀린 볼록껍질 다각형으로 바꾼다.
 *
 * @returns 닫히지 않은 반시계 방향 좌표열. 입력이 비면 빈 배열.
 */
export function bufferedHull(points: LatLng[], radiusM = 40): LatLng[] {
  if (points.length === 0) return [];

  const dLat = radiusM / M_PER_DEG_LAT;
  const expanded: LatLng[] = [];
  for (const p of points) {
    // 극점 근처에서 0으로 나누지 않도록 cos에 하한을 둔다.
    const cosLat = Math.max(Math.abs(Math.cos((p.lat * Math.PI) / 180)), 1e-6);
    const dLon = radiusM / (M_PER_DEG_LAT * cosLat);
    for (let k = 0; k < 8; k++) {
      const a = (k * Math.PI) / 4;
      expanded.push({
        lat: p.lat + dLat * Math.sin(a),
        lon: p.lon + dLon * Math.cos(a),
      });
    }
  }
  return convexHull(expanded);
}

/**
 * Monotone chain 볼록껍질. x=경도, y=위도로 본다.
 *
 * 일직선 위의 중간 점은 버린다(cross <= 0). 결과는 반시계 방향이며
 * 첫 점을 끝에 반복하지 않는다.
 */
export function convexHull(points: LatLng[]): LatLng[] {
  const sorted = [...points].sort((a, b) => (a.lon - b.lon) || (a.lat - b.lat));
  const pts = sorted.filter(
    (p, i) => i === 0 || p.lon !== sorted[i - 1].lon || p.lat !== sorted[i - 1].lat,
  );
  if (pts.length <= 2) return pts;

  const cross = (o: LatLng, a: LatLng, b: LatLng): number =>
    (a.lon - o.lon) * (b.lat - o.lat) - (a.lat - o.lat) * (b.lon - o.lon);

  const lower: LatLng[] = [];
  for (const p of pts) {
    while (lower.length >= 2 && cross(lower[lower.length - 2], lower[lower.length - 1], p) <= 0) {
      lower.pop();
    }
    lower.push(p);
  }

  const upper: LatLng[] = [];
  for (let i = pts.length - 1; i >= 0; i--) {
    const p = pts[i];
    while (upper.length >= 2 && cross(upper[upper.length - 2], upper[upper.length - 1], p) <= 0) {
      upper.pop();
    }
    upper.push(p);
  }

  lower.pop();
  upper.pop();
  return lower.concat(upper);
}
