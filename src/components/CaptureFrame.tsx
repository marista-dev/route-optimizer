import { useEffect, useRef } from 'react';
import type { PointerEvent as ReactPointerEvent } from 'react';
import { Check } from 'lucide-react';

import type { MarkerPoint, Rect } from '../capture/types';

export interface CaptureFrameProps {
  /** 고정 A4 프레임 자리. 지도 컨테이너 왼쪽 위 기준 CSS px(보통 `fitA4Frame` 결과) */
  rect: Rect;
  /** 프레임 왼쪽 위에 붙는 짧은 라벨(예: "A4 세로 · 이 장 12~27, 103~110"). 길면 말줄임 */
  label?: string;
  /** 프레임 경계에 걸쳐 잘리는 마커들. 주황 링으로 알려 준다 */
  edgeMarkers?: MarkerPoint[];
  /** 이미 저장한 장에 온전히 들어간 마커들. 오른쪽 위에 ✓ 배지를 띄운다 */
  capturedMarkers?: MarkerPoint[];
  /** 마커 반지름(px). 링 크기와 배지 위치에 쓴다(보통 `MARKER_RADIUS_PX`) */
  markerRadius: number;
  /**
   * 실제 캡처 직전에 true로 둔다. 어두운 처리·테두리·라벨·링·배지를 모두 숨긴다
   * (`visibility: hidden`이라 마운트 상태는 그대로다).
   */
  hidden?: boolean;
  /**
   * 지도를 끌거나 확대하는 동안 true. 라벨·링·배지를 흐리게 한다.
   * 어두운 처리와 테두리는 그대로 둔다 — 어디까지 찍히는지는 계속 보여야 한다.
   */
  faded?: boolean;
  /**
   * true면 프레임 네 모서리에 크기 조절 핸들을 띄운다. 핸들만 포인터를 받고,
   * 나머지 프레임은 그대로 지도 조작을 통과시킨다.
   */
  resizable?: boolean;
  /** 모서리 핸들을 누른 순간 한 번 */
  onResizeStart?: () => void;
  /**
   * 핸들을 끄는 동안 매 이동마다. 좌표는 `.ro-capture` 컨테이너(= 지도 컨테이너) 왼쪽 위 기준 CSS px.
   * 크기 계산(중심 고정·A4 비율 유지)은 호출 쪽이 한다(`scaleFromCornerDrag`).
   */
  onResize?: (pointer: { x: number; y: number }) => void;
  /** 끌기가 끝났을 때(pointerup·pointercancel·포인터 캡처 상실) 한 번 */
  onResizeEnd?: () => void;
}

/** 모서리 핸들 위치. CSS 수정자와 커서 방향을 정한다 */
type Corner = 'nw' | 'ne' | 'sw' | 'se';
const CORNERS: Corner[] = ['nw', 'ne', 'sw', 'se'];

/** ✓ 배지 한 변(px) */
const BADGE_SIZE = 14;
/** 핸들 히트 영역 한 변(px). 보이는 사각형은 CSS가 그 안 가운데에 작게 그린다 */
const HANDLE_HIT = 32;

/**
 * 지도 위에 얹는 고정 A4 캡처 프레임.
 *
 * 지도 컨테이너와 같은 자리·크기의 DOM 오버레이다(카카오 오버레이가 아니다).
 * 프레임은 움직이지 않고 사용자가 아래의 지도를 끌어 맞춘다. 그래서 라벨까지 모든 요소가
 * `pointer-events: none`이다. 라벨 칩은 예전에 tooltip(`title`)을 띄우려고 포인터를 받았지만,
 * 칩 위에서 시작한 끌기가 지도로 가지 않아 "가끔 지도가 안 움직인다"가 되므로 tooltip을 포기했다.
 * 칩 문구가 곧 전체 정보이고(장 크기·이 장의 순번), 저장 진행은 하단 독의 개수가 알려 준다.
 * `resizable`이면 네 모서리 핸들만 예외로 포인터를 받는다(끌어서 크기 조절, 지도는 움직이지 않는다).
 * 프레임 바깥 어두운 처리는 큰 `box-shadow` 하나로 그리고 컨테이너의 `overflow: hidden`으로 자른다(컨테이너 크기를 따로 받지 않아도 된다).
 */
export function CaptureFrame({
  rect,
  label,
  edgeMarkers,
  capturedMarkers,
  markerRadius,
  hidden,
  faded,
  resizable,
  onResizeStart,
  onResize,
  onResizeEnd,
}: CaptureFrameProps) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  /** 지금 끌고 있는 핸들의 포인터 id. null이면 끌고 있지 않다 */
  const dragPointer = useRef<number | null>(null);
  const onResizeEndRef = useRef(onResizeEnd);
  useEffect(() => {
    onResizeEndRef.current = onResizeEnd;
  }, [onResizeEnd]);

  // 끄는 도중 핸들이 사라지면(크기 조절 잠금·언마운트) pointerup이 오지 않으므로 여기서 끝을 알린다.
  useEffect(() => {
    if (!resizable) return;
    return () => {
      if (dragPointer.current === null) return;
      dragPointer.current = null;
      onResizeEndRef.current?.();
    };
  }, [resizable]);

  const toLocal = (e: ReactPointerEvent<HTMLElement>) => {
    const box = rootRef.current?.getBoundingClientRect();
    return { x: e.clientX - (box?.left ?? 0), y: e.clientY - (box?.top ?? 0) };
  };

  const handlePointerDown = (e: ReactPointerEvent<HTMLDivElement>) => {
    // 주 버튼(터치·펜은 0)만 받는다. 지도 끌기·텍스트 선택으로 번지지 않게 막는다.
    if (e.button !== 0 || dragPointer.current !== null) return;
    e.preventDefault();
    e.stopPropagation();
    try {
      e.currentTarget.setPointerCapture(e.pointerId);
    } catch {
      // 캡처를 못 해도 끌기는 계속한다(포인터가 핸들 밖으로 나가면 끊길 수 있다).
    }
    dragPointer.current = e.pointerId;
    onResizeStart?.();
  };

  const handlePointerMove = (e: ReactPointerEvent<HTMLDivElement>) => {
    if (dragPointer.current !== e.pointerId) return;
    e.preventDefault();
    e.stopPropagation();
    onResize?.(toLocal(e));
  };

  const handlePointerEnd = (e: ReactPointerEvent<HTMLDivElement>) => {
    if (dragPointer.current !== e.pointerId) return;
    e.stopPropagation();
    dragPointer.current = null;
    if (e.currentTarget.hasPointerCapture(e.pointerId)) {
      e.currentTarget.releasePointerCapture(e.pointerId);
    }
    onResizeEnd?.();
  };

  const ringSize = markerRadius * 2;
  // 배지 중심을 마커 중심에서 오른쪽 위로 반지름의 0.7배만큼 비켜 둔다.
  const badgeOffset = markerRadius * 0.7;

  const className = ['ro-capture', hidden ? 'is-hidden' : '', faded ? 'is-faded' : '']
    .filter(Boolean)
    .join(' ');

  return (
    <div ref={rootRef} className={className} aria-hidden={hidden || undefined}>
      <div
        className="ro-capture-mask"
        aria-hidden
        style={{ left: rect.x, top: rect.y, width: rect.width, height: rect.height }}
      />

      {edgeMarkers?.map((p) => (
        <div
          key={p.groupId}
          className="ro-capture-ring ro-capture-deco"
          aria-hidden
          style={{
            left: p.x - markerRadius,
            top: p.y - markerRadius,
            width: ringSize,
            height: ringSize,
          }}
        />
      ))}

      {capturedMarkers?.map((p) => (
        <div
          key={p.groupId}
          className="ro-capture-badge ro-capture-deco"
          aria-hidden
          style={{
            left: p.x + badgeOffset - BADGE_SIZE / 2,
            top: p.y - badgeOffset - BADGE_SIZE / 2,
            width: BADGE_SIZE,
            height: BADGE_SIZE,
          }}
        >
          <Check size={10} strokeWidth={3.5} />
        </div>
      ))}

      <div
        className="ro-capture-frame"
        style={{ left: rect.x, top: rect.y, width: rect.width, height: rect.height }}
      >
        {label ? (
          <div className="ro-capture-label ro-capture-deco">
            {label}
          </div>
        ) : null}
      </div>

      {resizable
        ? CORNERS.map((c) => (
            <div
              key={c}
              className={`ro-capture-handle ro-capture-handle--${c}`}
              aria-hidden
              title="끌어서 크기 조절 (A4 비율 유지)"
              style={{
                left: (c === 'nw' || c === 'sw' ? rect.x : rect.x + rect.width) - HANDLE_HIT / 2,
                top: (c === 'nw' || c === 'ne' ? rect.y : rect.y + rect.height) - HANDLE_HIT / 2,
                width: HANDLE_HIT,
                height: HANDLE_HIT,
              }}
              onPointerDown={handlePointerDown}
              onPointerMove={handlePointerMove}
              onPointerUp={handlePointerEnd}
              onPointerCancel={handlePointerEnd}
              onLostPointerCapture={handlePointerEnd}
            />
          ))
        : null}
    </div>
  );
}
