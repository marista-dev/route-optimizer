/**
 * geo.ts — `src/core/optimizer.py`의 `_haversine_km` 이식.
 */

/** 지구 반지름(km). Python 원본과 같은 값을 쓴다. */
const R_KM = 6371.0;

function toRadians(deg: number): number {
  return (deg * Math.PI) / 180;
}

/** 두 좌표 간 Haversine 직선 거리(km). */
export function haversineKm(
  lat1: number, lon1: number,
  lat2: number, lon2: number,
): number {
  const dlat = toRadians(lat2 - lat1);
  const dlon = toRadians(lon2 - lon1);
  const a =
    Math.sin(dlat / 2) ** 2 +
    Math.cos(toRadians(lat1)) * Math.cos(toRadians(lat2)) * Math.sin(dlon / 2) ** 2;
  return R_KM * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

/**
 * 도로시간 API가 실패했을 때 쓰는 대체 추정치(초).
 * Python `build_time_matrix`와 같은 40km/h 가정이다.
 */
export function haversineFallbackSec(
  lat1: number, lon1: number,
  lat2: number, lon2: number,
): number {
  return Math.trunc((haversineKm(lat1, lon1, lat2, lon2) / 40.0) * 3600);
}
