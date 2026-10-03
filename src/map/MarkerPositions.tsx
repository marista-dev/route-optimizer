import { useEffect, useRef } from 'react';

import { useMapContext } from './MapContext';
import type { MarkerPoint } from '../capture/types';
import type { PrimaryGroup } from '../types';

export interface MarkerPositionsProps {
  /** 화면 좌표를 구할 1차 그룹(= 지도 마커 1개) 목록. `MarkerLayer`에 넘기는 배열과 같다 */
  groups: PrimaryGroup[];
  /**
   * 그룹별 화면 좌표(지도 컨테이너 왼쪽 위 기준 CSS px)가 바뀔 때마다 불린다.
   * 지도를 끌거나 확대하는 동안 프레임마다 불릴 수 있으므로 무거운 일은 하지 말 것.
   */
  onChange: (points: MarkerPoint[]) => void;
}

/**
 * 다시 계산할 지도 이벤트. `idle`만 받으면 끄는 동안 주황 테두리가 늦게 따라온다.
 * `bounds_changed`는 컨테이너 크기만 바뀐 뒤의 `relayout()`(캡처 모드에서 헤더를 접을 때)도 잡는다.
 */
const MAP_EVENTS = ['idle', 'zoom_changed', 'center_changed', 'bounds_changed'] as const;

/**
 * 1차 그룹 마커의 화면 좌표를 콜백으로 올려 주는 보이지 않는 레이어.
 *
 * 캡처 영역 판정(안·걸침·밖)은 DOM 쪽에서 하므로, 위경도를 지도 컨테이너 px로
 * 바꿔 주는 일만 맡는다. 지도 이벤트는 한 프레임에 여러 번 올 수 있어 rAF로 묶는다.
 * 그리는 것이 없으므로 null을 돌려준다.
 */
export function MarkerPositions({ groups, onChange }: MarkerPositionsProps) {
  const { map } = useMapContext();

  // 콜백은 매 렌더 새로 만들어도 리스너를 다시 달지 않도록 최신 값만 ref로 들고 있는다.
  const onChangeRef = useRef(onChange);
  useEffect(() => {
    onChangeRef.current = onChange;
  });

  useEffect(() => {
    if (!map) return;

    // 위경도 객체는 그룹 배열이 바뀔 때만 만든다. 이벤트마다 새로 만들 이유가 없다.
    const coords = groups.map((g) => ({
      groupId: g.id,
      latlng: new kakao.maps.LatLng(g.lat, g.lon),
    }));

    const compute = () => {
      const projection = map.getProjection();
      const points: MarkerPoint[] = coords.map(({ groupId, latlng }) => {
        const p = projection.containerPointFromCoords(latlng);
        return { groupId, x: p.x, y: p.y };
      });
      onChangeRef.current(points);
    };

    let raf: number | null = null;
    const schedule = () => {
      if (raf !== null) return;
      raf = requestAnimationFrame(() => {
        raf = null;
        compute();
      });
    };

    for (const name of MAP_EVENTS) kakao.maps.event.addListener(map, name, schedule);
    window.addEventListener('resize', schedule);
    // 마운트 직후 한 번. 지도가 이미 멈춰 있으면 idle이 다시 오지 않는다.
    schedule();

    return () => {
      for (const name of MAP_EVENTS) kakao.maps.event.removeListener(map, name, schedule);
      window.removeEventListener('resize', schedule);
      if (raf !== null) cancelAnimationFrame(raf);
    };
  }, [map, groups]);

  return null;
}
