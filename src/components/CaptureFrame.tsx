import { useRef } from 'react';
import type { KeyboardEvent, PointerEvent as ReactPointerEvent } from 'react';
import { Move } from 'lucide-react';

import { clampRect } from '../capture/geometry';
import type { MarkerPoint, Rect, Size } from '../capture/types';

export interface CaptureFrameProps {
  /** 캡처 영역. 지도 컨테이너 왼쪽 위 기준 CSS px */
  rect: Rect;
  /** 지도 컨테이너 크기(px). 영역은 이 안으로만 움직인다 */
  bounds: Size;
  /** 끌기·크기 조절·방향키로 영역이 바뀔 때. 값은 이미 `bounds` 안으로 잘려 있다 */
  onChange: (rect: Rect) => void;
  /** 영역 경계에 걸쳐 잘리는 마커들. 주황 테두리로 알려 준다 */
  edgeMarkers?: MarkerPoint[];
  /** 마커 반지름(px). 주황 테두리 크기에 쓴다(보통 `MARKER_RADIUS_PX`) */
  markerRadius: number;
  /**
   * 실제 캡처 직전에 true로 둔다. 어두운 처리·테두리·핸들을 모두 숨긴다
   * (`visibility: hidden`이라 마운트와 드래그 상태는 그대로다).
   */
  hidden?: boolean;
}

/** 영역 최소 한 변(px). 이보다 작으면 마커 한두 개도 온전히 담기 어렵다 */
const MIN_SIZE = 120;
/** 방향키 한 번에 옮기는 거리(px) */
const KEY_STEP = 10;

/** 끌 수 있는 부분. 방위(n/e/s/w 조합) 또는 통째 이동. */
type Handle = 'n' | 'ne' | 'e' | 'se' | 's' | 'sw' | 'w' | 'nw' | 'move';

const RESIZE_HANDLES: Exclude<Handle, 'move'>[] = ['n', 'ne', 'e', 'se', 's', 'sw', 'w', 'nw'];

interface DragState {
  handle: Handle;
  pointerId: number;
  startX: number;
  startY: number;
  start: Rect;
}

function clampNum(v: number, min: number, max: number): number {
  return Math.min(Math.max(v, min), max);
}

/**
 * 끌기 시작 시점의 영역과 이동량으로 새 영역을 만든다.
 *
 * 크기 조절은 잡은 변만 움직이고 반대쪽 변은 고정한다. 그래서 `clampRect`에 바로
 * 넘기지 않고 변 단위로 먼저 자른다 — `clampRect`는 넘친 만큼 사각형을 통째로 밀어
 * 고정해야 할 반대쪽 변까지 끌려간다. 마지막에 한 번 더 `clampRect`로 안전하게 감싼다.
 */
function dragRect(handle: Handle, start: Rect, dx: number, dy: number, bounds: Size): Rect {
  if (handle === 'move') {
    return clampRect({ ...start, x: start.x + dx, y: start.y + dy }, bounds, MIN_SIZE);
  }
  const minW = Math.min(MIN_SIZE, bounds.width);
  const minH = Math.min(MIN_SIZE, bounds.height);
  let left = start.x;
  let top = start.y;
  let right = start.x + start.width;
  let bottom = start.y + start.height;
  if (handle.includes('w')) left = clampNum(left + dx, 0, right - minW);
  if (handle.includes('e')) right = clampNum(right + dx, left + minW, bounds.width);
  if (handle.includes('n')) top = clampNum(top + dy, 0, bottom - minH);
  if (handle.includes('s')) bottom = clampNum(bottom + dy, top + minH, bounds.height);
  return clampRect(
    { x: left, y: top, width: right - left, height: bottom - top },
    bounds,
    MIN_SIZE,
  );
}

/**
 * 지도 위에 얹는 캡처 영역 프레임.
 *
 * 지도 컨테이너와 같은 자리·크기의 DOM 오버레이다(카카오 오버레이가 아니다).
 * 핸들과 이동 손잡이만 포인터를 받고, 프레임 안쪽과 어두운 바깥은 `pointer-events: none`이다 —
 * 프레임을 둔 채로 지도를 끌고 확대해 다음 구역을 맞출 수 있어야 하기 때문이다.
 *
 * 키보드: 이동 손잡이에 포커스를 두고 방향키로 10px씩 옮긴다. Shift+방향키는
 * 오른쪽 아래 모서리를 움직여 크기를 바꾼다.
 */
export function CaptureFrame({
  rect,
  bounds,
  onChange,
  edgeMarkers,
  markerRadius,
  hidden,
}: CaptureFrameProps) {
  const dragRef = useRef<DragState | null>(null);

  const onPointerDown = (handle: Handle) => (e: ReactPointerEvent<HTMLElement>) => {
    // 주 버튼(마우스 왼쪽·터치·펜)만. 오른쪽 클릭으로 끌리면 어색하다.
    if (e.button !== 0) return;
    e.preventDefault();
    e.stopPropagation();
    e.currentTarget.setPointerCapture(e.pointerId);
    dragRef.current = {
      handle,
      pointerId: e.pointerId,
      startX: e.clientX,
      startY: e.clientY,
      start: rect,
    };
  };

  const onPointerMove = (e: ReactPointerEvent<HTMLElement>) => {
    const drag = dragRef.current;
    if (!drag || drag.pointerId !== e.pointerId) return;
    const next = dragRect(
      drag.handle,
      drag.start,
      e.clientX - drag.startX,
      e.clientY - drag.startY,
      bounds,
    );
    if (
      next.x !== rect.x ||
      next.y !== rect.y ||
      next.width !== rect.width ||
      next.height !== rect.height
    ) {
      onChange(next);
    }
  };

  // pointerup·pointercancel·lostpointercapture 어느 쪽으로 끝나도 같은 정리를 한다.
  const endDrag = (e: ReactPointerEvent<HTMLElement>) => {
    const drag = dragRef.current;
    if (!drag || drag.pointerId !== e.pointerId) return;
    dragRef.current = null;
    if (e.currentTarget.hasPointerCapture(e.pointerId)) {
      e.currentTarget.releasePointerCapture(e.pointerId);
    }
  };

  const dragProps = (handle: Handle) => ({
    onPointerDown: onPointerDown(handle),
    onPointerMove,
    onPointerUp: endDrag,
    onPointerCancel: endDrag,
    onLostPointerCapture: endDrag,
  });

  const onGripKeyDown = (e: KeyboardEvent<HTMLButtonElement>) => {
    let dx = 0;
    let dy = 0;
    if (e.key === 'ArrowLeft') dx = -KEY_STEP;
    else if (e.key === 'ArrowRight') dx = KEY_STEP;
    else if (e.key === 'ArrowUp') dy = -KEY_STEP;
    else if (e.key === 'ArrowDown') dy = KEY_STEP;
    else return;
    e.preventDefault();
    onChange(dragRect(e.shiftKey ? 'se' : 'move', rect, dx, dy, bounds));
  };

  const right = rect.x + rect.width;
  const bottom = rect.y + rect.height;

  return (
    <div className={`ro-capture${hidden ? ' is-hidden' : ''}`} aria-hidden={hidden || undefined}>
      {/* 바깥 어둡게: 위·아래는 전체 너비, 왼쪽·오른쪽은 영역 높이만큼 */}
      <div
        className="ro-capture-mask"
        style={{ left: 0, top: 0, width: bounds.width, height: rect.y }}
      />
      <div
        className="ro-capture-mask"
        style={{ left: 0, top: bottom, width: bounds.width, height: bounds.height - bottom }}
      />
      <div
        className="ro-capture-mask"
        style={{ left: 0, top: rect.y, width: rect.x, height: rect.height }}
      />
      <div
        className="ro-capture-mask"
        style={{ left: right, top: rect.y, width: bounds.width - right, height: rect.height }}
      />

      {edgeMarkers?.map((p) => (
        <div
          key={p.groupId}
          className="ro-capture-ring"
          style={{
            left: p.x - markerRadius,
            top: p.y - markerRadius,
            width: markerRadius * 2,
            height: markerRadius * 2,
          }}
        />
      ))}

      <div
        className="ro-capture-frame"
        style={{ left: rect.x, top: rect.y, width: rect.width, height: rect.height }}
      >
        <button
          type="button"
          className="ro-capture-grip"
          aria-label="캡처 영역 이동. 방향키로 옮기고 Shift+방향키로 크기를 바꿉니다"
          title="끌어서 영역 이동"
          onKeyDown={onGripKeyDown}
          {...dragProps('move')}
        >
          <Move size={14} aria-hidden />
          영역 이동
        </button>

        {RESIZE_HANDLES.map((h) => (
          <div
            key={h}
            className={`ro-capture-handle ro-capture-handle--${h}`}
            aria-hidden
            {...dragProps(h)}
          />
        ))}

        <div className="ro-capture-size" aria-hidden>
          {Math.round(rect.width)} × {Math.round(rect.height)}
        </div>
      </div>
    </div>
  );
}
