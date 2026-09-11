import { useEffect } from 'react';

import { useMapContext } from './MapContext';
import type { LatLng } from '../types';

/** 참고점 종류 — `prev`는 직전 클러스터 이탈점, `next`는 다음 클러스터 중심. */
export type RefPointKind = 'prev' | 'next';

/** 지도에 찍는 참고점 하나. */
export interface RefPoint {
  /** React key 겸 식별자 */
  id: string;
  /** 위치 */
  at: LatLng;
  /** 라벨 문구(예: "이전 위치 · 아남@ 101동") */
  label: string;
  /** 색 구분 */
  kind: RefPointKind;
}

export interface RefPointLayerProps {
  /** 찍을 참고점들. 배열 정체성이 바뀌면 전부 다시 만든다 — `useMemo`로 감싸 넘길 것 */
  points: RefPoint[];
}

/**
 * 클릭되지 않는 참고점 라벨 레이어.
 *
 * S5에서 이전 클러스터의 이탈점과 다음 클러스터의 중심을 보여 주기 위한 것으로,
 * 고를 수 있는 대상이 아니므로 `clickable: false` + CSS `pointer-events: none`이다.
 */
export function RefPointLayer({ points }: RefPointLayerProps) {
  const { map } = useMapContext();

  useEffect(() => {
    if (!map) return;
    const overlays = points.map((point) => {
      const el = document.createElement('div');
      el.className = `ro-refpoint ro-refpoint--${point.kind}`;
      el.textContent = point.label;
      const overlay = new kakao.maps.CustomOverlay({
        content: el,
        position: new kakao.maps.LatLng(point.at.lat, point.at.lon),
        xAnchor: 0.5,
        yAnchor: 1.6,
        zIndex: 4,
        clickable: false,
      });
      overlay.setMap(map);
      return overlay;
    });

    return () => {
      for (const overlay of overlays) overlay.setMap(null);
    };
  }, [map, points]);

  return null;
}
