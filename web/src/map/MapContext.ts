import { createContext, useContext } from 'react';

import type { KakaoMap } from './useKakaoMap';
import type { LatLng } from '../types';

/** 지도 레이어들이 공유하는 값. `MapCanvas`가 주입한다. */
export interface MapContextValue {
  /** 생성된 카카오 지도. 아직 준비 전이면 null */
  map: KakaoMap | null;
  /** 주어진 좌표가 모두 보이도록 화면을 맞춘다 */
  fitBounds: (points: LatLng[]) => void;
}

const noop = () => {};

export const MapContext = createContext<MapContextValue>({
  map: null,
  fitBounds: noop,
});

/** 레이어 컴포넌트에서 지도 컨텍스트를 읽는다. `MapCanvas` 밖에서는 map이 null이다. */
export function useMapContext(): MapContextValue {
  return useContext(MapContext);
}
