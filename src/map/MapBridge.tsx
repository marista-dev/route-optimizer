import { useEffect } from 'react';

import { useMapContext } from './MapContext';
import type { MapContextValue } from './MapContext';

export interface MapBridgeProps {
  /**
   * 지도 컨텍스트가 바뀔 때마다(지도 생성 등) 불린다. 화면 쪽 훅이 ref에 담아 두고
   * 이벤트 처리기 안에서 `panBy`·`whenIdle`·`relayout`을 부를 때 쓴다.
   * 언마운트되면 null로 한 번 더 불린다.
   */
  onChange: (ctx: MapContextValue | null) => void;
}

/**
 * `MapCanvas` 바깥의 화면 로직(예: S6 캡처 훅)이 지도 명령을 쓸 수 있게 컨텍스트를 건네주는
 * 보이지 않는 레이어. 데이터는 읽지 않고 컨텍스트 값만 위로 올린다. 그리는 것이 없으므로 null.
 */
export function MapBridge({ onChange }: MapBridgeProps) {
  const ctx = useMapContext();
  useEffect(() => {
    onChange(ctx);
    return () => onChange(null);
  }, [ctx, onChange]);
  return null;
}
