import { useMemo, useRef } from 'react';
import type { ReactNode } from 'react';

import { MapContext } from './MapContext';
import { useKakaoMap } from './useKakaoMap';
import type { LatLng } from '../types';
import './map.css';

/** 광주시청 부근. 출발지가 정해지기 전의 기본 중심. */
const DEFAULT_CENTER: LatLng = { lat: 35.18, lon: 126.9 };

/** 초기 확대 레벨. 아무 화면도 다른 값을 넘기지 않는다. */
const DEFAULT_LEVEL = 6;

export interface MapCanvasProps {
  /** 초기 중심 좌표(최초 생성 시에만 사용) */
  center?: LatLng;
  /** 지도 레이어들. 컨텍스트로 map을 받는다 */
  children?: ReactNode;
}

/**
 * 화면을 꽉 채우는 지도 컨테이너.
 * 지도 인스턴스를 만들어 `MapContext`로 내려보내므로 레이어들은 props로 데이터만 받으면 된다.
 */
export function MapCanvas({ center = DEFAULT_CENTER, children }: MapCanvasProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const { map, error, fitBounds } = useKakaoMap(containerRef, { center, level: DEFAULT_LEVEL });

  const value = useMemo(() => ({ map, fitBounds }), [map, fitBounds]);

  return (
    <div className="ro-map">
      <div ref={containerRef} className="ro-map__canvas" />
      {error ? <p className="ro-map__error">{error}</p> : null}
      <MapContext.Provider value={value}>{children}</MapContext.Provider>
    </div>
  );
}
