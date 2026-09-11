import { useEffect } from 'react';

import { useMapContext } from '../map';
import type { LatLng } from '../types';

/**
 * `MapCanvas` 안에 두면 `target`이 바뀔 때마다 그 지점으로 지도를 부드럽게 옮긴다.
 * 배율은 건드리지 않는다(사용자가 맞춰 둔 확대 수준을 유지한다).
 *
 * 같은 지점을 다시 눌러도 움직이게 하려면 매번 새 객체를 넘길 것
 * (정체성이 바뀌어야 effect가 다시 돈다). 이동이 필요 없으면 null.
 */
export function MapPan({ target }: { target: LatLng | null }) {
  const { map } = useMapContext();
  useEffect(() => {
    if (!map || !target) return;
    map.panTo(new kakao.maps.LatLng(target.lat, target.lon));
  }, [map, target]);
  return null;
}
