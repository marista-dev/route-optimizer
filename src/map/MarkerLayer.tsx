import { useCallback, useEffect, useRef } from 'react';

import { useMapContext } from './MapContext';
import type { Origin, PrimaryGroup } from '../types';

export interface MarkerLayerProps {
  /** 그릴 1차 그룹(= 지도 마커 1개) 목록 */
  groups: PrimaryGroup[];
  /** 표시할 그룹 id. 생략하면 전부 표시(S5에서 현재 클러스터만 남길 때 쓴다) */
  visibleGroupIds?: number[];
  /** 출발지. 있으면 별도 마커로 표시 */
  origin?: Origin | null;
  /** 진입 지점으로 고른 그룹(초록) */
  entryGroupId?: number;
  /** 이탈 지점으로 고른 그룹(빨강) */
  exitGroupId?: number;
  /** 마커에 띄울 숫자 라벨(S6 결과 화면의 배송 순번 등) */
  orderLabel?: (groupId: number) => string | undefined;
  /** 마커 툴팁(건물 이름 등). 마우스를 올리면 브라우저 기본 툴팁으로 뜬다 */
  tooltipOf?: (groupId: number) => string | undefined;
  /** 강조할 그룹(S6에서 표 행에 호버했을 때). 재생성 없이 클래스만 바뀐다 */
  highlightGroupId?: number;
  /** 강조 마커 위에 띄울 라벨(배송순서 + 건물 이름) */
  highlightText?: string;
  /** 마커 클릭 */
  onGroupClick?: (groupId: number) => void;
  /** 마커에 마우스가 올라가거나(id) 벗어날 때(null) */
  onGroupHover?: (groupId: number | null) => void;
}

interface MarkerEntry {
  id: number;
  overlay: any;
  position: any;
  root: HTMLDivElement;
  text: HTMLSpanElement;
  cleanup: () => void;
}

/** 강조되지 않은 마커의 zIndex. 강조 마커는 그 위로 올린다. */
const Z_MARKER = 5;
const Z_MARKER_HIGHLIGHT = 8;

/** 1차 그룹 마커 레이어. 진입·이탈 색과 순번 라벨은 재생성 없이 갱신한다. */
export function MarkerLayer({
  groups,
  visibleGroupIds,
  origin,
  entryGroupId,
  exitGroupId,
  orderLabel,
  tooltipOf,
  highlightGroupId,
  highlightText,
  onGroupClick,
  onGroupHover,
}: MarkerLayerProps) {
  const { map } = useMapContext();

  const latest = useRef({
    map,
    visibleGroupIds,
    entryGroupId,
    exitGroupId,
    orderLabel,
    tooltipOf,
    highlightGroupId,
    highlightText,
    onGroupClick,
    onGroupHover,
  });
  // 아래 effect들보다 먼저 선언해 매 커밋에서 가장 먼저 갱신되게 한다.
  useEffect(() => {
    latest.current = {
      map,
      visibleGroupIds,
      entryGroupId,
      exitGroupId,
      orderLabel,
      tooltipOf,
      highlightGroupId,
      highlightText,
      onGroupClick,
      onGroupHover,
    };
  });

  const entriesRef = useRef<MarkerEntry[]>([]);
  /** 강조 마커 위에 띄우는 라벨. 마커와 같은 수명이다 */
  const labelRef = useRef<{ overlay: any; el: HTMLDivElement } | null>(null);

  /** 표시 여부·색·라벨·강조만 갱신한다. */
  const applyStates = useCallback(() => {
    const cur = latest.current;
    const visible = cur.visibleGroupIds
      ? new Set(cur.visibleGroupIds)
      : null;
    let highlighted: MarkerEntry | null = null;

    for (const entry of entriesRef.current) {
      const shown = visible === null || visible.has(entry.id);
      entry.overlay.setMap(shown ? cur.map : null);
      if (!shown) continue;

      const orderText = cur.orderLabel?.(entry.id) ?? '';
      const classes = ['ro-marker'];
      // 순번 마커는 정원을 유지해야 한다. CSS는 자릿수를 셀 수 없으므로
      // 여기서 자릿수를 클래스로 알려 주고, 지름은 스타일시트가 정한다.
      if (orderText) {
        classes.push('is-order');
        classes.push(
          orderText.length >= 3
            ? 'is-order-3'
            : orderText.length === 2
              ? 'is-order-2'
              : 'is-order-1',
        );
      }
      if (cur.entryGroupId === entry.id) classes.push('is-entry');
      if (cur.exitGroupId === entry.id) classes.push('is-exit');
      const isHighlight = cur.highlightGroupId === entry.id;
      if (isHighlight) {
        classes.push('is-highlight');
        highlighted = entry;
      }
      entry.root.className = classes.join(' ');
      entry.overlay.setZIndex(isHighlight ? Z_MARKER_HIGHLIGHT : Z_MARKER);
      entry.text.textContent = orderText;
      const tooltip = cur.tooltipOf?.(entry.id);
      if (tooltip) entry.root.title = tooltip;
      else entry.root.removeAttribute('title');
    }

    const label = labelRef.current;
    if (label) {
      if (highlighted && cur.highlightText) {
        label.el.textContent = cur.highlightText;
        label.overlay.setPosition(highlighted.position);
        label.overlay.setMap(cur.map);
      } else {
        label.overlay.setMap(null);
      }
    }
  }, []);

  // 마커 생성/파기.
  useEffect(() => {
    if (!map) return;

    const labelEl = document.createElement('div');
    labelEl.className = 'ro-marker-label';
    const labelOverlay = new kakao.maps.CustomOverlay({
      content: labelEl,
      xAnchor: 0.5,
      yAnchor: 2,
      zIndex: 9,
    });
    labelRef.current = { overlay: labelOverlay, el: labelEl };

    const entries: MarkerEntry[] = groups.map((group) => {
      const root = document.createElement('div');
      root.className = 'ro-marker';

      const text = document.createElement('span');
      root.appendChild(text);

      if (group.members.length > 1) {
        const count = document.createElement('span');
        count.className = 'ro-marker__count';
        count.textContent = String(group.members.length);
        root.appendChild(count);
      }

      const onClick = () => latest.current.onGroupClick?.(group.id);
      const onEnter = () => latest.current.onGroupHover?.(group.id);
      const onLeave = () => latest.current.onGroupHover?.(null);
      root.addEventListener('click', onClick);
      root.addEventListener('mouseenter', onEnter);
      root.addEventListener('mouseleave', onLeave);

      const position = new kakao.maps.LatLng(group.lat, group.lon);
      const overlay = new kakao.maps.CustomOverlay({
        content: root,
        position,
        xAnchor: 0.5,
        yAnchor: 0.5,
        zIndex: Z_MARKER,
        clickable: true,
      });
      overlay.setMap(map);

      return {
        id: group.id,
        overlay,
        position,
        root,
        text,
        cleanup: () => {
          root.removeEventListener('click', onClick);
          root.removeEventListener('mouseenter', onEnter);
          root.removeEventListener('mouseleave', onLeave);
        },
      };
    });

    entriesRef.current = entries;
    applyStates();

    return () => {
      for (const entry of entries) {
        entry.cleanup();
        entry.overlay.setMap(null);
      }
      labelOverlay.setMap(null);
      labelRef.current = null;
      entriesRef.current = [];
    };
  }, [map, groups, applyStates]);

  // 진입·이탈·가시성·라벨·강조 변경 반영.
  useEffect(() => {
    applyStates();
  }, [
    applyStates,
    visibleGroupIds,
    entryGroupId,
    exitGroupId,
    orderLabel,
    tooltipOf,
    highlightGroupId,
    highlightText,
  ]);

  // 출발지 마커.
  useEffect(() => {
    if (!map || !origin) return;
    const el = document.createElement('div');
    el.className = 'ro-origin';
    el.textContent = '출발';
    const overlay = new kakao.maps.CustomOverlay({
      content: el,
      position: new kakao.maps.LatLng(origin.lat, origin.lon),
      xAnchor: 0.5,
      yAnchor: 0.5,
      zIndex: 6,
    });
    overlay.setMap(map);
    return () => overlay.setMap(null);
  }, [map, origin]);

  return null;
}
