import { useEffect, useRef } from 'react';

import { useMapContext } from './MapContext';

export interface MapInteractionProps {
  /** 사용자가 지도를 끌기 시작하거나(`dragstart`) 확대·축소가 시작될 때(`zoom_start`) */
  onInteractStart: () => void;
  /** 지도가 멈췄을 때(`idle`). 시작 없이 `idle`만 와도(예: `panBy` 뒤) 불린다 */
  onInteractEnd: () => void;
}

const START_EVENTS = ['dragstart', 'zoom_start'] as const;

/**
 * 지도 조작 시작·끝을 콜백으로 올려 주는 보이지 않는 레이어(S6 캡처의 독·라벨 흐리기용).
 *
 * `panBy`처럼 코드로 옮길 때는 `dragstart`가 오지 않으므로 시작으로 치지 않는다.
 * 다만 `zoom_start`는 코드로 확대 수준을 바꿀 때도 온다.
 * 조작 도중 언마운트되면 끝 콜백을 한 번 불러 흐린 상태가 남지 않게 한다.
 * 그리는 것이 없으므로 null을 돌려준다.
 */
export function MapInteraction({ onInteractStart, onInteractEnd }: MapInteractionProps) {
  const { map } = useMapContext();

  // 콜백은 매 렌더 새로 만들어도 리스너를 다시 달지 않도록 최신 값만 ref로 들고 있는다.
  const latest = useRef({ onInteractStart, onInteractEnd });
  useEffect(() => {
    latest.current = { onInteractStart, onInteractEnd };
  });

  useEffect(() => {
    if (!map) return;
    let interacting = false;

    const start = () => {
      interacting = true;
      latest.current.onInteractStart();
    };
    const end = () => {
      interacting = false;
      latest.current.onInteractEnd();
    };

    for (const name of START_EVENTS) kakao.maps.event.addListener(map, name, start);
    kakao.maps.event.addListener(map, 'idle', end);

    return () => {
      for (const name of START_EVENTS) kakao.maps.event.removeListener(map, name, start);
      kakao.maps.event.removeListener(map, 'idle', end);
      if (interacting) latest.current.onInteractEnd();
    };
  }, [map]);

  return null;
}
