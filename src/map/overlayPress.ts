import type { KakaoMap } from './useKakaoMap';

/**
 * 클릭을 받는 CustomOverlay(마커·클러스터 중심 히트 타깃)의 누르기 처리.
 *
 * `clickable: true` 오버레이 위에서 시작한 끌기는 카카오가 지도로 넘기지 않는다. 그래서
 * 지도를 옮기려고 마커를 누르고 끌면 지도는 안 움직이고, 손을 떼는 순간 클릭으로 처리됐다.
 * 여기서는 누른 뒤 slop을 넘게 움직이면 클릭을 취소하고 **지도를 직접 끈다**.
 *
 * 순수 판정(slop·반복 클릭)은 함수로 따로 빼 단위 테스트한다(`overlayPress.test.ts`).
 */

/** 화면 좌표(px) */
export interface PressPoint {
  x: number;
  y: number;
}

/** 시작점에서 지금 위치까지가 `slop`(px)보다 멀면 true — 클릭이 아니라 끌기다. */
export function exceededSlop(start: PressPoint, cur: PressPoint, slop: number): boolean {
  return Math.hypot(cur.x - start.x, cur.y - start.y) > slop;
}

/**
 * DOM `click`의 `detail`(연속 클릭 수)로 더블클릭의 두 번째 이후인지 본다.
 * 0은 키보드·합성 클릭이라 반복으로 치지 않는다.
 */
export function isRepeatClick(detail: number): boolean {
  return detail > 1;
}

/**
 * 같은 대상(`key`)을 `windowMs` 안에 다시 누르면 무시하라고(true) 알려 주는 판정기를 만든다.
 * DOM `detail`이 없는 카카오 이벤트(다각형 `click`)의 더블클릭 가드용이다.
 *
 * 무시된 클릭도 시각을 갱신한다 — 빠르게 연타하는 동안은 계속 무시하고, 손을 멈췄다가
 * `windowMs`가 지나면 다시 받는다.
 */
export function createRepeatGuard<K>(windowMs: number): (key: K, now: number) => boolean {
  const last = new Map<K, number>();
  return (key, now) => {
    const prev = last.get(key);
    last.set(key, now);
    return prev !== undefined && now - prev >= 0 && now - prev < windowMs;
  };
}

/**
 * 지도를 화면 px만큼 즉시(애니메이션 없이) 옮긴다. 포인터가 오른쪽으로 dx 움직이면 지도 내용도
 * 오른쪽으로 따라가야 하므로 중심은 반대로 옮긴다. `map.panBy`는 부드럽게 움직여 끌기에 뒤처진다.
 */
function dragMapBy(map: KakaoMap, dx: number, dy: number): void {
  const proj = map.getProjection();
  const c = proj.containerPointFromCoords(map.getCenter());
  const next = new kakao.maps.Point(c.x - dx, c.y - dy);
  map.setCenter(proj.coordsFromContainerPoint(next));
}

/** 운영체제 더블클릭 간격(기본 500ms)을 넉넉히 덮는 상한. `detail > 1`을 같은 요소의 반복으로 볼 창 */
const MULTI_CLICK_MAX_MS = 1000;

export interface OverlayPressOptions {
  /** slop 안에서 눌렀다 뗀 한 번의 클릭(더블클릭의 두 번째 이후는 제외) */
  onClick: () => void;
  /** slop을 넘어 지도 끌기에 들어간 순간 한 번 */
  onDragStart?: () => void;
  /** 끌기가 끝났을 때(pointerup·pointercancel·포인터 캡처 상실) */
  onDragEnd?: () => void;
  /** 이만큼(px) 넘게 움직이면 클릭이 아니라 끌기다. 기본 5 */
  slop?: number;
  /** 이 시간(ms) 안에 다시 들어온 클릭은 무시한다(`detail`이 없는 환경 대비). 기본 300 */
  repeatMs?: number;
}

/**
 * `el`(clickable CustomOverlay의 content)에 누르기 처리를 붙이고 해제 함수를 돌려준다.
 *
 * - pointerdown(주 버튼만): 시작점을 기록하고 포인터를 잡는다(`setPointerCapture`).
 * - pointermove: slop을 넘으면 끌기로 바꾸고, 이후 매 이동마다 직전 위치와의 차이만큼 지도를 옮긴다.
 * - pointerup / pointercancel / lostpointercapture: 끌기였으면 `onDragEnd`.
 * - click: 끌기였으면 삼킨다. 아니면 더블클릭의 두 번째(`detail > 1`이고 직전 클릭도 이 요소)가
 *   아니고 반복 가드를 통과할 때만 `onClick`.
 *
 * 클릭 판정을 pointerup이 아니라 `click`에서 하는 이유: Pointer Events 명세상 pointer 이벤트의
 * `detail`은 늘 0이라 더블클릭을 가릴 수 없다. `click`의 `detail`만 연속 클릭 수를 담는다.
 *
 * 카카오 `dragstart`/`dragend`는 SDK가 자기 끌기에만 내므로, 이 끌기를 지도 이벤트로 알리려면
 * `onDragStart`/`onDragEnd`에서 직접 `kakao.maps.event.trigger`한다(레이어 쪽 책임).
 * `getMap`은 매번 최신 지도를 읽도록 함수로 받는다. null이면 끌기는 하지 않는다.
 */
export function attachOverlayPress(
  el: HTMLElement,
  getMap: () => KakaoMap | null,
  { onClick, onDragStart, onDragEnd, slop = 5, repeatMs = 300 }: OverlayPressOptions,
): () => void {
  /** 진행 중인 누르기. 없으면 null */
  let press: { id: number; start: PressPoint; last: PressPoint; dragging: boolean } | null = null;
  /** 직전 누르기가 끌기로 끝났다 — 뒤따르는 `click` 하나를 삼킨다 */
  let swallowClick = false;
  const repeat = createRepeatGuard<0>(repeatMs);
  /** 이 요소가 마지막으로 받은(삼키지 않은) `click`의 시각. `detail`을 이 요소 기준으로 거른다 */
  let lastClickAt: number | null = null;

  const finish = () => {
    if (!press) return;
    const wasDragging = press.dragging;
    const id = press.id;
    press = null;
    if (el.hasPointerCapture?.(id)) el.releasePointerCapture(id);
    if (wasDragging) {
      swallowClick = true;
      onDragEnd?.();
    }
  };

  const onPointerDown = (event: PointerEvent) => {
    // 주 버튼(왼쪽·터치·펜)만. 오른쪽·가운데 버튼은 건드리지 않는다.
    if (event.button !== 0 || !event.isPrimary) return;
    // 끝 이벤트를 놓친 이전 누르기(캡처 실패 후 요소 밖에서 뗌 등)가 남아 있으면 먼저 닫는다 —
    // 끌기였다면 `dragend` 짝을 맞춰야 `MapInteraction`이 끌기 중 상태에 갇히지 않는다.
    finish();
    // 다음 누르기가 시작됐으면 이전 끌기의 `click`이 오지 않은 것이므로 잊는다.
    swallowClick = false;
    const p = { x: event.clientX, y: event.clientY };
    press = { id: event.pointerId, start: p, last: p, dragging: false };
    try {
      el.setPointerCapture(event.pointerId);
    } catch {
      // 이미 끝난 포인터 등 — 캡처 없이도 element 위에서는 동작한다.
    }
  };

  const onPointerMove = (event: PointerEvent) => {
    if (!press || event.pointerId !== press.id) return;
    const cur = { x: event.clientX, y: event.clientY };
    if (!press.dragging) {
      if (!exceededSlop(press.start, cur, slop)) return;
      press.dragging = true;
      onDragStart?.();
      // slop 안에서 움직인 만큼도 반영해 지도가 포인터에서 밀리지 않게 한다(last = start).
    }
    const dx = cur.x - press.last.x;
    const dy = cur.y - press.last.y;
    press.last = cur;
    const map = getMap();
    if (map && (dx !== 0 || dy !== 0)) dragMapBy(map, dx, dy);
  };

  const onPointerEnd = (event: PointerEvent) => {
    if (!press || event.pointerId !== press.id) return;
    finish();
  };

  const onClickEvent = (event: MouseEvent) => {
    // 이 요소의 클릭은 여기서 끝낸다(지도·다른 리스너로 넘기지 않는다).
    event.stopPropagation();
    if (swallowClick) {
      swallowClick = false;
      return;
    }
    const prev = lastClickAt;
    lastClickAt = event.timeStamp;
    // `detail`은 요소와 무관하게 세어진다 — 가까운 다른 마커를 빠르게 이어 누르면 2가 올 수 있다.
    // 직전 클릭도 이 요소였을 때만 더블클릭의 두 번째로 본다.
    if (
      isRepeatClick(event.detail) &&
      prev !== null &&
      event.timeStamp - prev >= 0 &&
      event.timeStamp - prev < MULTI_CLICK_MAX_MS
    ) {
      return;
    }
    if (repeat(0, event.timeStamp)) return;
    onClick();
  };

  el.addEventListener('pointerdown', onPointerDown);
  el.addEventListener('pointermove', onPointerMove);
  el.addEventListener('pointerup', onPointerEnd);
  el.addEventListener('pointercancel', onPointerEnd);
  el.addEventListener('lostpointercapture', onPointerEnd);
  el.addEventListener('click', onClickEvent);

  return () => {
    // 끌기 도중 해제되면 끝 콜백을 불러 지도 이벤트 짝(dragstart/dragend)을 맞춘다.
    finish();
    el.removeEventListener('pointerdown', onPointerDown);
    el.removeEventListener('pointermove', onPointerMove);
    el.removeEventListener('pointerup', onPointerEnd);
    el.removeEventListener('pointercancel', onPointerEnd);
    el.removeEventListener('lostpointercapture', onPointerEnd);
    el.removeEventListener('click', onClickEvent);
  };
}

/**
 * 오버레이 끌기를 카카오 지도의 `dragstart`/`dragend`로 알리는 콜백 쌍.
 * `MapInteraction`(S6 캡처 독 흐리기·자동 저장 중지)이 SDK 자체 끌기와 똑같이 받는다.
 */
export function mapDragNotifier(getMap: () => KakaoMap | null): {
  onDragStart: () => void;
  onDragEnd: () => void;
} {
  return {
    onDragStart: () => {
      const map = getMap();
      if (map) kakao.maps.event.trigger(map, 'dragstart');
    },
    onDragEnd: () => {
      const map = getMap();
      if (map) kakao.maps.event.trigger(map, 'dragend');
    },
  };
}
