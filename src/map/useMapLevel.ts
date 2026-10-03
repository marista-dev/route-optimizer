import { useCallback, useSyncExternalStore } from 'react';

import { useMapContext } from './MapContext';

/**
 * 지금 지도의 확대 레벨(1~14, 작을수록 확대)을 구독한다. 지도 준비 전이면 null.
 *
 * `zoom_changed`마다 갱신한다. 레벨은 일부러 `MapContext` 값에 넣지 않는다 — 넣으면 확대할
 * 때마다 컨텍스트 객체가 새로 만들어져 모든 레이어가 다시 그려지고, `MapBridge`도
 * `onChange(null)`→`onChange(ctx)`를 반복한다. 레벨이 필요한 곳만 이 훅으로 따로 구독한다.
 */
export function useMapLevel(): number | null {
  const { map } = useMapContext();

  const subscribe = useCallback(
    (notify: () => void) => {
      if (!map) return () => {};
      kakao.maps.event.addListener(map, 'zoom_changed', notify);
      return () => kakao.maps.event.removeListener(map, 'zoom_changed', notify);
    },
    [map],
  );
  const getSnapshot = useCallback((): number | null => {
    if (!map) return null;
    const v = Number(map.getLevel());
    return Number.isFinite(v) ? v : null;
  }, [map]);

  return useSyncExternalStore(subscribe, getSnapshot, getServerSnapshot);
}

/** 서버 렌더에는 지도가 없다 */
function getServerSnapshot(): null {
  return null;
}
