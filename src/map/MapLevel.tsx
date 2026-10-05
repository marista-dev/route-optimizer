import { useEffect, useRef } from 'react';

import { useMapLevel } from './useMapLevel';

export interface MapLevelProps {
  /** 확대 레벨이 바뀔 때마다(처음 한 번 포함) 불린다. 언마운트되면 null로 한 번 더 불린다 */
  onChange: (level: number | null) => void;
}

/**
 * `MapCanvas` 바깥의 화면 로직(예: S6 캡처 독의 확대 단계 표시)이 확대 레벨을 받을 수 있게 올려 주는
 * 보이지 않는 레이어. {@link useMapLevel}을 감싼다. 그리는 것이 없으므로 null.
 */
export function MapLevel({ onChange }: MapLevelProps) {
  const level = useMapLevel();

  // 콜백은 매 렌더 새로 만들어도 다시 부르지 않도록 최신 값만 ref로 들고 있는다.
  const onChangeRef = useRef(onChange);
  useEffect(() => {
    onChangeRef.current = onChange;
  });

  useEffect(() => {
    onChangeRef.current(level);
  }, [level]);

  useEffect(() => () => onChangeRef.current(null), []);

  return null;
}
