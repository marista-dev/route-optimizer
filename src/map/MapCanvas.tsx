import { useEffect, useMemo, useRef } from 'react';
import type { ReactNode } from 'react';

import { MapContext } from './MapContext';
import { useKakaoMap } from './useKakaoMap';
import type { LatLng } from '../types';
import './map.css';

/** 광주시청 부근. 출발지가 정해지기 전의 기본 중심. */
const DEFAULT_CENTER: LatLng = { lat: 35.18, lon: 126.9 };

/** 초기 확대 레벨. 아무 화면도 다른 값을 넘기지 않는다. */
const DEFAULT_LEVEL = 6;

/** 입력 칸(지도 안에는 없지만 혹시 들어와도) — 선택 방지에서 뺀다. */
function isEditable(target: EventTarget | null): boolean {
  if (!(target instanceof Element)) return false;
  return Boolean(target.closest('input, textarea, select, [contenteditable]:not([contenteditable="false"])'));
}

/**
 * 지도 위에서 글자가 선택(파란 블록)되거나 이미지 고스트가 끌려 나오지 않게 막는다.
 * CSS `user-select: none`(map.css)을 보강한다 — 누르기 전에 이미 걸려 있던 선택은 CSS로 못 지운다.
 * 해제 함수를 돌려준다.
 */
function guardSelection(root: HTMLElement): () => void {
  const prevent = (event: Event) => {
    if (!isEditable(event.target)) event.preventDefault();
  };
  const clear = (event: Event) => {
    if (isEditable(event.target)) return;
    const sel = window.getSelection?.();
    if (sel && sel.rangeCount > 0 && !sel.isCollapsed) sel.removeAllRanges();
  };
  // 오버레이·SDK가 전파를 막을 수 있어 모두 캡처 단계에서 받는다.
  root.addEventListener('selectstart', prevent, true);
  root.addEventListener('dragstart', prevent, true);
  root.addEventListener('pointerdown', clear, true);
  root.addEventListener('mousedown', clear, true);
  return () => {
    root.removeEventListener('selectstart', prevent, true);
    root.removeEventListener('dragstart', prevent, true);
    root.removeEventListener('pointerdown', clear, true);
    root.removeEventListener('mousedown', clear, true);
  };
}

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
  const rootRef = useRef<HTMLDivElement>(null);
  const { map, error, fitBounds, relayout, panBy, whenIdle } = useKakaoMap(containerRef, {
    center,
    level: DEFAULT_LEVEL,
  });

  const value = useMemo(
    () => ({ map, fitBounds, relayout, panBy, whenIdle }),
    [map, fitBounds, relayout, panBy, whenIdle],
  );

  useEffect(() => {
    const root = rootRef.current;
    if (!root) return;
    return guardSelection(root);
  }, []);

  return (
    <div ref={rootRef} className="ro-map">
      <div ref={containerRef} className="ro-map__canvas" />
      {error ? <p className="ro-map__error">{error}</p> : null}
      <MapContext.Provider value={value}>{children}</MapContext.Provider>
    </div>
  );
}
