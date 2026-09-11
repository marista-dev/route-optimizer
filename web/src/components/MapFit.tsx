import { useEffect } from 'react';

import { useMapContext } from '../map';
import type { LatLng } from '../types';

/**
 * `MapCanvas` 안에 두면 `points`가 바뀔 때마다 화면을 맞춘다.
 * 배열은 반드시 `useMemo`로 감싸 넘길 것(정체성이 바뀌면 매번 다시 맞춘다).
 */
export function MapFit({ points }: { points: LatLng[] }) {
  const { map, fitBounds } = useMapContext();
  useEffect(() => {
    if (!map || points.length === 0) return;
    fitBounds(points);
  }, [map, fitBounds, points]);
  return null;
}
