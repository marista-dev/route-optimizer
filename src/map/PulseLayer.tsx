import { useEffect } from 'react';

import { useMapContext } from './MapContext';
import type { LatLng } from '../types';

declare const kakao: any;

export interface PulseLayerProps {
  /** 반짝일 지점들. 빈 배열이면 아무것도 그리지 않는다. 반드시 `useMemo`로 감쌀 것 */
  points: LatLng[];
}

/** 겹침 순서. 클러스터 다각형·마커보다 위. */
const Z_INDEX = 9;

/**
 * 지정한 지점에서 물결처럼 퍼지는 표시를 띄운다.
 *
 * "여기를 보라"는 신호 전용이다. 클릭도 받지 않고 상태도 없다. 켜고 끄는 것은
 * 부모가 `points`를 채웠다 비우는 것으로 한다(보통 몇 초 뒤 타이머로 비운다).
 */
export function PulseLayer({ points }: PulseLayerProps) {
  const { map } = useMapContext();

  useEffect(() => {
    if (!map || points.length === 0) return;

    const overlays = points.map((p) => {
      const el = document.createElement('div');
      el.className = 'ro-pulse';
      el.innerHTML = '<span class="ro-pulse__ring"></span><span class="ro-pulse__dot"></span>';
      const overlay = new kakao.maps.CustomOverlay({
        position: new kakao.maps.LatLng(p.lat, p.lon),
        content: el,
        zIndex: Z_INDEX,
        // 클릭을 가로채지 않도록 지도 이벤트는 그대로 통과시킨다.
        clickable: false,
      });
      overlay.setMap(map);
      return overlay;
    });

    return () => {
      for (const o of overlays) o.setMap(null);
    };
  }, [map, points]);

  return null;
}
