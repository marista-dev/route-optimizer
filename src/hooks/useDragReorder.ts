import { useCallback, useEffect, useRef, useState } from 'react';

/**
 * 세로 목록의 드래그 재정렬 보조.
 *
 * HTML5 드래그는 두 가지가 기본으로 빠져 있다.
 *  - 목록 가장자리에 끌고 가도 스크롤이 따라오지 않는다. 화면 밖 위치에는 놓을 수 없다.
 *  - 어디에 꽂히는지 보이지 않는다.
 * 이 훅이 둘을 채운다. 스크롤 컨테이너에 네이티브 리스너를 달아 커서가 위·아래
 * 가장자리에 들어오면 매 프레임 조금씩 스크롤하고, 커서가 어느 항목의 위/아래
 * 절반에 있는지로 삽입 위치(`dropAt`)를 계산해 돌려준다.
 *
 * `dropAt`은 "빼내기 전" 기준의 삽입 인덱스다(0..length). 실제 이동 대상 인덱스는
 * `targetIndex`로 바꿔 쓴다.
 */

/** 가장자리 감지 폭(px). 이 안으로 들어오면 스크롤이 시작된다. */
const EDGE_PX = 56;
/** 프레임당 최대 스크롤 거리(px). 가장자리에 가까울수록 이 값에 가까워진다. */
const MAX_STEP_PX = 16;
/** 재정렬 대상 항목을 찾는 선택자. */
const ITEM_SELECTOR = '[data-reorder-item]';

/**
 * 삽입 인덱스를 실제 이동 대상 인덱스로 바꾼다.
 *
 * `dropAt`은 원소를 빼기 전 기준이므로, 자기 자신보다 뒤에 꽂을 때는 한 칸 당겨야 한다.
 * 제자리(자기 앞/자기 뒤)면 `null`을 돌려 호출부가 아무것도 하지 않게 한다.
 */
export function targetIndex(dropAt: number, from: number): number | null {
  const to = dropAt > from ? dropAt - 1 : dropAt;
  return to === from ? null : to;
}

export interface DragReorder {
  /** 스크롤 컨테이너에 붙일 ref */
  listRef: (el: HTMLDivElement | null) => void;
  /** 끌고 있는 항목의 인덱스 */
  dragFrom: number | null;
  /** 삽입될 위치(0..length). 끌고 있지 않으면 null */
  dropAt: number | null;
  /**
   * 삽입선을 그릴 자리. `dropAt`과 달리 제자리(자기 앞/뒤)는 `null`로 걸러져 있다 —
   * 그 자리에 선을 그리면 "놓아도 안 옮겨지는 자리"가 생긴다. 호출부는 삽입선
   * 표시에 `dropAt`이 아니라 이 값을 써야 한다.
   */
  dropLine: number | null;
  /** 항목의 onDragStart에 연결 */
  onItemDragStart: (index: number) => void;
  /** 항목·컨테이너의 onDragEnd에 연결 */
  onDragEnd: () => void;
}

/**
 * @param onMove 실제 이동. `from`에서 `to`로 옮긴다.
 */
export function useDragReorder(onMove: (from: number, to: number) => void): DragReorder {
  const [dragFrom, setDragFrom] = useState<number | null>(null);
  const [dropAt, setDropAt] = useState<number | null>(null);

  const elRef = useRef<HTMLDivElement | null>(null);
  const fromRef = useRef<number | null>(null);
  const dropRef = useRef<number | null>(null);
  const rafRef = useRef<number | null>(null);
  const speedRef = useRef(0);
  const moveRef = useRef(onMove);
  // 렌더 중에 ref를 건드리지 않는다. 최신 콜백은 커밋 뒤에 갈아 끼운다.
  useEffect(() => {
    moveRef.current = onMove;
  }, [onMove]);

  const stopScroll = useCallback(() => {
    if (rafRef.current !== null) cancelAnimationFrame(rafRef.current);
    rafRef.current = null;
    speedRef.current = 0;
  }, []);

  const finish = useCallback(() => {
    stopScroll();
    fromRef.current = null;
    dropRef.current = null;
    setDragFrom(null);
    setDropAt(null);
  }, [stopScroll]);

  const onItemDragStart = useCallback((index: number) => {
    fromRef.current = index;
    setDragFrom(index);
  }, []);

  // 컨테이너에 네이티브 리스너를 단다. 항목 사이 빈 틈에서도 동작해야 하므로
  // 개별 항목이 아니라 스크롤 컨테이너가 받는다.
  const listRef = useCallback(
    (el: HTMLDivElement | null) => {
      elRef.current = el;
    },
    [],
  );

  useEffect(() => {
    const el = elRef.current;
    if (!el || dragFrom === null) return;

    const tick = () => {
      const node = elRef.current;
      if (!node || speedRef.current === 0) {
        rafRef.current = null;
        return;
      }
      node.scrollTop += speedRef.current;
      rafRef.current = requestAnimationFrame(tick);
    };

    const handleDragOver = (e: DragEvent) => {
      if (fromRef.current === null) return;
      e.preventDefault();
      if (e.dataTransfer) e.dataTransfer.dropEffect = 'move';

      // 삽입 위치 — 커서가 어느 항목의 위쪽 절반에 있는지로 정한다.
      const items = Array.from(el.querySelectorAll<HTMLElement>(ITEM_SELECTOR));
      let at = items.length;
      for (let i = 0; i < items.length; i += 1) {
        const r = items[i].getBoundingClientRect();
        if (e.clientY < r.top + r.height / 2) {
          at = i;
          break;
        }
      }
      dropRef.current = at;
      setDropAt(at);

      // 가장자리 자동 스크롤 — 가까울수록 빠르게.
      const box = el.getBoundingClientRect();
      const fromTop = e.clientY - box.top;
      const fromBottom = box.bottom - e.clientY;
      let speed = 0;
      if (fromTop < EDGE_PX) speed = -Math.ceil(((EDGE_PX - fromTop) / EDGE_PX) * MAX_STEP_PX);
      else if (fromBottom < EDGE_PX) speed = Math.ceil(((EDGE_PX - fromBottom) / EDGE_PX) * MAX_STEP_PX);
      speedRef.current = speed;
      if (speed !== 0 && rafRef.current === null) rafRef.current = requestAnimationFrame(tick);
      if (speed === 0) stopScroll();
    };

    const handleDrop = (e: DragEvent) => {
      e.preventDefault();
      const from = fromRef.current;
      const at = dropRef.current;
      if (from !== null && at !== null) {
        const to = targetIndex(at, from);
        if (to !== null) moveRef.current(from, to);
      }
      finish();
    };

    // 컨테이너 밖으로 나가면 스크롤만 멈춘다(드래그 자체는 계속될 수 있다).
    const handleLeave = () => stopScroll();

    el.addEventListener('dragover', handleDragOver);
    el.addEventListener('drop', handleDrop);
    el.addEventListener('dragleave', handleLeave);
    return () => {
      el.removeEventListener('dragover', handleDragOver);
      el.removeEventListener('drop', handleDrop);
      el.removeEventListener('dragleave', handleLeave);
      stopScroll();
    };
  }, [dragFrom, finish, stopScroll]);

  // 제자리(자기 앞/자기 뒤)에 놓으면 targetIndex가 null을 돌려준다 — 그 자리는
  // 삽입선도 그리지 않는다. 여기서 한 번만 걸러 두면 호출부가 매번 다시 계산할 필요가 없다.
  const dropLine =
    dragFrom !== null && dropAt !== null && targetIndex(dropAt, dragFrom) !== null ? dropAt : null;

  return { listRef, dragFrom, dropAt, dropLine, onItemDragStart, onDragEnd: finish };
}
