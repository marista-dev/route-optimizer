import { useCallback, useEffect, useMemo, useRef } from 'react';

import { useMapContext } from './MapContext';
import { MAP_COLOR } from './palette';
import type { Cluster } from '../types';

/**
 * 클러스터 다각형의 표시 모드.
 * - `info`: S3. hover 정보 + click 인포윈도우
 * - `order`: S4. 순번이 매겨진 클러스터를 완료색으로 칠하고 중심에 큰 번호를 띄운다
 * - `dim`: S5. `activeClusterId` 하나만 선명하게 두고 나머지는 흐리게
 *
 * 각 모드가 정확히 무엇을 켜는지는 아래 {@link MODE} 표 하나에 모여 있다.
 */
export type ClusterMode = 'info' | 'order' | 'dim';

export interface ClusterLayerProps {
  /** 그릴 클러스터 목록. 배열 정체성이 바뀌면 다각형을 전부 재생성한다 */
  clusters: Cluster[];
  /** 표시 모드 */
  mode: ClusterMode;
  /** 클러스터의 방문 순번(1부터). 아직 지정 전이면 undefined */
  orderOf: (clusterId: number) => number | undefined;
  /** `dim` 모드에서 선명하게 남길 클러스터 */
  activeClusterId?: number;
  /** `dim` 모드에서 이미 확정된 클러스터(초록으로 칠한다) */
  isDone?: (clusterId: number) => boolean;
  /** `dim` 모드에서 직전 방문 클러스터(진한 초록으로 남긴다) */
  prevClusterId?: number;
  /** `dim` 모드에서 다음 방문 클러스터(빨간 점선 외곽선) */
  nextClusterId?: number;
  /** 다각형 클릭 */
  onClusterClick?: (clusterId: number) => void;
  /** `info` 모드 클릭 시 인포윈도우에 넣을 HTML(멤버 이름 목록 등) */
  memberSummary?: (clusterId: number) => string;
  /**
   * 클러스터의 배송 건수. 주면 hover 라벨의 "배송지 n건"이 이 값이 된다.
   * 없으면 그룹(건물) 수를 그대로 "건물 n곳"이라 부른다 — 단위를 속이지 않는다.
   */
  countOf?: (clusterId: number) => number;
  /** 클러스터를 부를 이름(대표 건물명 등). hover 라벨 끝에 붙는다 */
  nameOf?: (clusterId: number) => string | undefined;
  /** 목록 hover 등으로 잠시 강조할 클러스터 */
  highlightClusterId?: number;
}

// `palette.ts`의 지도 팔레트. 다른 레이어·화면과 같은 상수를 쓴다.
/** 기본(브랜드 파랑) — 다각형·순번 라벨·경로선이 모두 이 색이다 */
const BASE_STROKE = MAP_COLOR.base;
const BASE_FILL = MAP_COLOR.base;
/** 확정된 클러스터 */
const DONE_STROKE = MAP_COLOR.done;
const DONE_FILL = MAP_COLOR.done;
/** S5에서 다음에 갈 클러스터 — "여기로 빠져나간다"를 빨강으로 못 박는다 */
const NEXT_STROKE = MAP_COLOR.next;
const NEXT_FILL = MAP_COLOR.next;
/** S5에서 아직 손대지 않은(흐린) 클러스터 */
const IDLE_STROKE = MAP_COLOR.idleStroke;
const IDLE_FILL = MAP_COLOR.idleFill;

const FILL_OPACITY = 0.15;
const FILL_OPACITY_ORDERED = 0.28;
const FILL_OPACITY_UNORDERED = 0.08;
const FILL_OPACITY_HOVER = 0.22;
const FILL_OPACITY_ACTIVE = 0.16;
const FILL_OPACITY_DONE = 0.12;
/** S5에서 현재·이전·다음이 아닌 클러스터 — 거의 안 보이게 민다 */
const FILL_OPACITY_DIM = 0.03;
const FILL_OPACITY_PREV = 0.12;
const FILL_OPACITY_NEXT = 0.1;
const STROKE_OPACITY = 0.9;
const STROKE_OPACITY_DIM = 0.22;
const STROKE_OPACITY_NEIGHBOR = 0.85;
const STROKE_WEIGHT = 2;
const STROKE_WEIGHT_ACTIVE = 3;

/** 순번 배지의 zIndex. 지도 오버레이 중 가장 위다(`src/map/README.md`의 표 참고). */
const Z_ORDER_BADGE = 10;

/**
 * 중심 히트 타깃의 지름(px). 이 값이 유일한 출처다 — map.css는 크기를 정하지 않는다.
 * 44px 원이 화면에 30~40개 흩어지면 지도를 끌려고 누른 지점이 자주 원 안에 걸려
 * "가끔 지도가 안 움직인다"가 된다(이미 44→32로 한 번 줄인 이력이 있다).
 * 클릭은 여전히 넉넉한 선까지 줄였다.
 */
const HIT_SIZE_PX = 32;
/** 이만큼 넘게 끌었으면 클릭으로 치지 않는다(지도를 옮기려던 손). */
const DRAG_SLOP_PX = 5;

/**
 * 모드별로 무엇이 켜지는지 한곳에서 본다.
 * 전에는 "이 모드가 무엇을 켜는가"에 답하려면 파일 곳곳의 `mode === '…'` 분기
 * 7곳을 다 읽어야 했다. 새 모드를 넣거나 기존 동작을 바꿀 때 이 표 한 줄만 보면 된다.
 */
const MODE: Record<ClusterMode, {
  /** 중심 히트 타깃을 만든다(:hitTarget 사용처의 생성 effect) */
  hitTarget: boolean;
  /** 클릭 시 인포윈도우를 연다({@link ClusterLayer}의 `select`) */
  infoWindow: boolean;
  /** 순번 배지를 띄우고, 미지정 클러스터를 "아직 미지정"이라 부르며 흐리게 칠한다
   *  ({@link pickState}의 `unordered`, {@link defaultLabel}, `applyStyles`의 `ordered`) */
  orderBadge: boolean;
  /** active/prev/next/done 이외는 흐리게 민다(`applyStyles`의 `dim`) */
  dimOthers: boolean;
}> = {
  info: { hitTarget: true, infoWindow: true, orderBadge: false, dimOthers: false },
  order: { hitTarget: true, infoWindow: false, orderBadge: true, dimOthers: false },
  dim: { hitTarget: false, infoWindow: false, orderBadge: false, dimOthers: true },
};

/** {@link applyStyles}가 매 다각형마다 계산하는 표시 상태 하나. */
type ClusterVisualState =
  | 'active' | 'prev' | 'next' | 'done' | 'idle' | 'ordered' | 'unordered' | 'base';

/** `active`~`base`가 서로 배타적이 되도록(우선순위 순서대로) 상태 하나로 접는다. */
function pickState(
  mode: ClusterMode,
  active: boolean,
  prev: boolean,
  next: boolean,
  done: boolean,
  idle: boolean,
  ordered: boolean,
): ClusterVisualState {
  if (active) return 'active';
  if (prev) return 'prev';
  if (next) return 'next';
  if (done) return 'done';
  if (idle) return 'idle';
  if (ordered) return 'ordered';
  if (MODE[mode].orderBadge) return 'unordered';
  return 'base';
}

/** 상태별 채움 투명도. 기존 7단 중첩 삼항과 동일한 값. */
const FILL: Record<ClusterVisualState, number> = {
  active: FILL_OPACITY_ACTIVE,
  prev: FILL_OPACITY_PREV,
  next: FILL_OPACITY_NEXT,
  done: FILL_OPACITY_DONE,
  idle: FILL_OPACITY_DIM,
  ordered: FILL_OPACITY_ORDERED,
  unordered: FILL_OPACITY_UNORDERED,
  base: FILL_OPACITY,
};

/** 상태별 외곽선 투명도. 기존 `prev||next` / `idle||done` / 기본 삼항과 동일한 값. */
const STROKE: Record<ClusterVisualState, number> = {
  active: STROKE_OPACITY,
  prev: STROKE_OPACITY_NEIGHBOR,
  next: STROKE_OPACITY_NEIGHBOR,
  done: STROKE_OPACITY_DIM,
  idle: STROKE_OPACITY_DIM,
  ordered: STROKE_OPACITY,
  unordered: STROKE_OPACITY,
  base: STROKE_OPACITY,
};

interface ClusterEntry {
  id: number;
  polygon: any;
  orderOverlay: any;
  orderEl: HTMLDivElement;
  /** 중심에 얹는 고정 크기 클릭 타깃(작은 클러스터 클릭 미스 방지). 비대화형이면 null */
  hitOverlay: any | null;
}

/**
 * hover 라벨.
 *
 * 순번이 붙은 클러스터는 순번으로 부른다 — S4 지도에 찍히는 숫자는 클러스터 id가
 * 아니라 방문 순번이라, id를 보여 주면 화면 어디에도 없는 번호를 말하는 셈이 된다.
 * 순번을 매기는 화면에서 아직 순번이 없으면 번호를 아예 쓰지 않는다 — 배지의 "5"와
 * "클러스터 5"가 서로 다른 것을 가리켜 읽는 사람을 멈춰 세우기 때문이다.
 */
function defaultLabel(
  cluster: Cluster,
  mode: ClusterMode,
  order?: number | null,
  count?: number,
  name?: string,
): string {
  const head = order
    ? `${order}번째 방문`
    : MODE[mode].orderBadge
      ? '아직 미지정'
      : `클러스터 ${cluster.id + 1}`;
  // 건수를 모르면 그룹 수를 그룹 수라고 부른다. 목록·통계의 "배송지 n건"은 배송 건수다.
  const size = count === undefined ? `건물 ${cluster.groupIds.length}곳` : `배송지 ${count}건`;
  return [head, size, name].filter(Boolean).join(' · ');
}

/** 클러스터별 다각형 + hover 라벨 + 순번 라벨 레이어. */
export function ClusterLayer({
  clusters,
  mode,
  orderOf,
  activeClusterId,
  isDone,
  prevClusterId,
  nextClusterId,
  onClusterClick,
  memberSummary,
  countOf,
  nameOf,
  highlightClusterId,
}: ClusterLayerProps) {
  const { map } = useMapContext();

  /**
   * 클릭을 받는 화면에서만 중심 히트 타깃을 만든다(S3 정보 · S4 순서).
   * 생성 effect 안에서 `latest.current`로 한 번만 읽으면, 그 effect의 dep(`clusters`
   * 정체성)이 안 바뀐 채로 `mode`나 `onClusterClick`만 바뀔 때 히트 타깃 유무가
   * 낡은 채로 남는다 — 지금은 화면마다 모드가 고정이고 전환 시 컴포넌트째 다시
   * 마운트돼 드러나지 않지만, 그 전제가 깨지면 클릭이 조용히 죽거나 살아난다.
   * `useMemo`로 뽑아 생성 effect의 dep에 넣어 둔다.
   */
  const interactive = useMemo(
    () =>
      MODE[mode].hitTarget &&
      (Boolean(onClusterClick) || (MODE[mode].infoWindow && Boolean(memberSummary))),
    [mode, onClusterClick, memberSummary],
  );

  // 리스너는 한 번만 붙이므로 최신 props는 ref로 읽는다.
  const latest = useRef({
    map,
    mode,
    orderOf,
    activeClusterId,
    isDone,
    prevClusterId,
    nextClusterId,
    onClusterClick,
    memberSummary,
    countOf,
    nameOf,
    highlightClusterId,
  });
  // 아래 effect들보다 먼저 선언해 매 커밋에서 가장 먼저 갱신되게 한다.
  useEffect(() => {
    latest.current = {
      map,
      mode,
      orderOf,
      activeClusterId,
      isDone,
      prevClusterId,
      nextClusterId,
      onClusterClick,
      memberSummary,
      countOf,
      nameOf,
      highlightClusterId,
    };
  });

  const entriesRef = useRef<ClusterEntry[]>([]);

  /** 현재 모드·순번에 맞춰 다각형 옵션과 순번 라벨만 갱신한다(재생성 없음). */
  const applyStyles = useCallback(() => {
    const cur = latest.current;
    for (const entry of entriesRef.current) {
      const order = cur.orderOf(entry.id);
      const ordered = MODE[cur.mode].orderBadge && order !== undefined;
      const dim = MODE[cur.mode].dimOthers;
      const active = dim && cur.activeClusterId === entry.id;
      const prev = dim && !active && cur.prevClusterId === entry.id;
      const next = dim && !active && !prev && cur.nextClusterId === entry.id;
      const done = dim && !active && !prev && !next && (cur.isDone?.(entry.id) ?? false);
      const idle = dim && !active && !prev && !next && !done;
      const state = pickState(cur.mode, active, prev, next, done, idle, ordered);
      // 목록에서 가리키는 다각형은 마우스를 올린 것과 똑같이 보이게 한다(같은 값을 쓴다).
      const highlight = cur.highlightClusterId === entry.id;

      entry.polygon.setOptions({
        strokeColor:
          prev || done ? DONE_STROKE : next ? NEXT_STROKE : idle ? IDLE_STROKE : BASE_STROKE,
        strokeOpacity: highlight ? STROKE_OPACITY : STROKE[state],
        strokeWeight: active || highlight ? STROKE_WEIGHT_ACTIVE : STROKE_WEIGHT,
        strokeStyle: next ? 'shortdash' : 'solid',
        fillColor: prev || done ? DONE_FILL : next ? NEXT_FILL : idle ? IDLE_FILL : BASE_FILL,
        fillOpacity: highlight ? FILL_OPACITY_HOVER : FILL[state],
      });

      if (ordered && order !== undefined) {
        entry.orderEl.textContent = String(order);
        entry.orderOverlay.setMap(cur.map);
      } else {
        entry.orderOverlay.setMap(null);
      }
    }
  }, []);

  // 다각형 생성/파기. clusters 정체성이 바뀔 때만 돈다.
  useEffect(() => {
    if (!map) return;

    const hoverEl = document.createElement('div');
    hoverEl.className = 'ro-cluster-label';
    const hoverOverlay = new kakao.maps.CustomOverlay({
      content: hoverEl,
      xAnchor: 0.5,
      yAnchor: 1.4,
      zIndex: 4,
    });
    const infoWindow = new kakao.maps.InfoWindow({ removable: true });

    const listeners: Array<[any, string, (event: any) => void]> = [];
    const domCleanups: Array<() => void> = [];

    const entries: ClusterEntry[] = clusters.map((cluster) => {
      const path = cluster.hull.map(
        (p) => new kakao.maps.LatLng(p.lat, p.lon),
      );
      const polygon = new kakao.maps.Polygon({
        path,
        strokeWeight: STROKE_WEIGHT,
        strokeColor: BASE_STROKE,
        strokeOpacity: STROKE_OPACITY,
        strokeStyle: 'solid',
        fillColor: BASE_FILL,
        fillOpacity: FILL_OPACITY,
      });
      polygon.setMap(map);

      const orderEl = document.createElement('div');
      orderEl.className = 'ro-cluster-order';
      // 1건짜리 클러스터는 중심 = 그 그룹의 좌표라, 배지를 중심에 놓으면 마커가
      // 정확히 겹쳐 숫자가 보이지 않는다. 이럴 때만 마커 위로 올려 찍는다.
      const singleGroup = cluster.groupIds.length <= 1;
      const orderOverlay = new kakao.maps.CustomOverlay({
        content: orderEl,
        position: new kakao.maps.LatLng(
          cluster.centroid.lat,
          cluster.centroid.lon,
        ),
        xAnchor: 0.5,
        yAnchor: singleGroup ? 1.25 : 0.5,
        // 마커(5)·출발지(6)·히트 타깃(7)보다 위. 순번은 무엇에도 가려지지 않는다.
        zIndex: Z_ORDER_BADGE,
      });

      /** 지금 값으로 만든 hover 라벨. 다각형과 중심 히트 타깃이 같은 문구를 쓴다. */
      const labelNow = (): string => {
        const cur = latest.current;
        return defaultLabel(
          cluster,
          cur.mode,
          cur.orderOf?.(cluster.id),
          cur.countOf?.(cluster.id),
          cur.nameOf?.(cluster.id),
        );
      };

      const onMouseOver = (event: any) => {
        const cur = latest.current;
        hoverEl.textContent = labelNow();
        hoverOverlay.setPosition(event.latLng);
        hoverOverlay.setMap(cur.map);
        polygon.setOptions({ fillOpacity: FILL_OPACITY_HOVER });
      };
      const onMouseMove = (event: any) => {
        hoverOverlay.setPosition(event.latLng);
      };
      const onMouseOut = () => {
        hoverOverlay.setMap(null);
        applyStyles();
      };
      /** 다각형·히트 타깃이 공유하는 선택 동작. `ll`은 카카오 LatLng. */
      const select = (ll: any) => {
        const cur = latest.current;
        if (MODE[cur.mode].infoWindow && cur.memberSummary) {
          infoWindow.setContent(cur.memberSummary(cluster.id));
          infoWindow.setPosition(ll);
          infoWindow.setMap(cur.map);
        }
        cur.onClusterClick?.(cluster.id);
      };
      const onClick = (event: any) => select(event.latLng);

      kakao.maps.event.addListener(polygon, 'mouseover', onMouseOver);
      kakao.maps.event.addListener(polygon, 'mousemove', onMouseMove);
      kakao.maps.event.addListener(polygon, 'mouseout', onMouseOut);
      kakao.maps.event.addListener(polygon, 'click', onClick);
      listeners.push(
        [polygon, 'mouseover', onMouseOver],
        [polygon, 'mousemove', onMouseMove],
        [polygon, 'mouseout', onMouseOut],
        [polygon, 'click', onClick],
      );

      // 배율이 낮으면 1건짜리 클러스터 다각형은 몇 px밖에 안 돼 클릭이 빗나간다.
      // 중심에 배율과 무관한 고정 크기(44px) 투명 원을 얹어 클릭을 받는다.
      // 클릭이 필요 없는 화면(S5의 dim)에서는 만들지 않는다 — 마커 클릭을 가리면 안 된다.
      let hitOverlay: any = null;
      if (interactive) {
        const centroidLL = new kakao.maps.LatLng(cluster.centroid.lat, cluster.centroid.lon);
        const hitEl = document.createElement('div');
        hitEl.className = 'ro-cluster-hit';
        hitEl.setAttribute('role', 'presentation');
        hitEl.style.width = `${HIT_SIZE_PX}px`;
        hitEl.style.height = `${HIT_SIZE_PX}px`;
        // 누른 자리에서 끌었으면 클릭으로 치지 않는다 — 지도를 옮기려던 손이
        // 방문 순서를 바꾸면 되돌릴 길이 없다.
        let downAt: { x: number; y: number } | null = null;
        const onHitDown = (event: MouseEvent) => {
          downAt = { x: event.clientX, y: event.clientY };
        };
        const onHitClick = (event: MouseEvent) => {
          const moved =
            downAt !== null &&
            Math.hypot(event.clientX - downAt.x, event.clientY - downAt.y) > DRAG_SLOP_PX;
          downAt = null;
          if (moved) return;
          select(centroidLL);
        };
        const onHitEnter = () => {
          const cur = latest.current;
          hoverEl.textContent = labelNow();
          hoverOverlay.setPosition(centroidLL);
          hoverOverlay.setMap(cur.map);
          polygon.setOptions({ fillOpacity: FILL_OPACITY_HOVER });
        };
        const onHitLeave = () => {
          hoverOverlay.setMap(null);
          applyStyles();
        };
        hitEl.addEventListener('mousedown', onHitDown);
        hitEl.addEventListener('click', onHitClick);
        hitEl.addEventListener('mouseenter', onHitEnter);
        hitEl.addEventListener('mouseleave', onHitLeave);
        domCleanups.push(() => {
          hitEl.removeEventListener('mousedown', onHitDown);
          hitEl.removeEventListener('click', onHitClick);
          hitEl.removeEventListener('mouseenter', onHitEnter);
          hitEl.removeEventListener('mouseleave', onHitLeave);
        });
        hitOverlay = new kakao.maps.CustomOverlay({
          content: hitEl,
          position: centroidLL,
          xAnchor: 0.5,
          yAnchor: 0.5,
          zIndex: 7,
          clickable: true,
        });
        hitOverlay.setMap(map);
      }

      return { id: cluster.id, polygon, orderOverlay, orderEl, hitOverlay };
    });

    entriesRef.current = entries;
    applyStyles();

    return () => {
      for (const [target, type, handler] of listeners) {
        kakao.maps.event.removeListener(target, type, handler);
      }
      for (const cleanup of domCleanups) cleanup();
      for (const entry of entries) {
        entry.polygon.setMap(null);
        entry.orderOverlay.setMap(null);
        entry.hitOverlay?.setMap(null);
      }
      hoverOverlay.setMap(null);
      infoWindow.setMap(null);
      entriesRef.current = [];
    };
  }, [map, clusters, applyStyles, interactive]);

  // 모드·순번·활성 클러스터가 바뀌면 옵션만 다시 칠한다.
  useEffect(() => {
    applyStyles();
  }, [
    applyStyles,
    mode,
    orderOf,
    activeClusterId,
    isDone,
    prevClusterId,
    nextClusterId,
    highlightClusterId,
  ]);

  return null;
}
