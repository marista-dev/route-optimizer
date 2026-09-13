import { useEffect } from 'react';

import { useMapContext } from './MapContext';
import { MAP_COLOR } from './palette';
import type { LatLng } from '../types';

/** 경로선 모양. `dashed`는 순서 안내용, `solid`는 확정 경로용이다. */
export type RouteStyle = 'dashed' | 'solid';

export interface RouteLayerProps {
  /** 경로 하나당 좌표 배열. 배열 정체성이 바뀌면 선을 다시 그린다 */
  paths: LatLng[][];
  /** 선 모양(기본 solid) */
  style?: RouteStyle;
  /** 선 색(기본 브랜드 파랑) */
  color?: string;
}

const DEFAULT_COLOR = MAP_COLOR.base;

/** 좌표열들을 Polyline으로 그리는 레이어. */
export function RouteLayer({
  paths,
  style = 'solid',
  color = DEFAULT_COLOR,
}: RouteLayerProps) {
  const { map } = useMapContext();

  useEffect(() => {
    if (!map) return;

    const lines = paths
      .filter((points) => points.length >= 2)
      .map((points) => {
        const line = new kakao.maps.Polyline({
          path: points.map((p) => new kakao.maps.LatLng(p.lat, p.lon)),
          strokeWeight: 4,
          strokeColor: color,
          strokeOpacity: 0.9,
          strokeStyle: style === 'dashed' ? 'shortdash' : 'solid',
        });
        line.setMap(map);
        return line;
      });

    return () => {
      for (const line of lines) line.setMap(null);
    };
  }, [map, paths, style, color]);

  return null;
}
