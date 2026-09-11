import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { CSSProperties } from 'react';
import { ArrowRight, ChevronDown, ChevronUp, GripVertical } from 'lucide-react';

import { MapFit, SidePanel } from '../components';
import { targetIndex, useDragReorder } from '../hooks/useDragReorder';
import { ClusterLayer, MapCanvas, MarkerLayer, OrderLinkLayer } from '../map';
import { PulseLayer } from '../map/PulseLayer';
import { useSessionStore } from '../store/session';
import { showInfo } from '../store/toast';
import type { LatLng } from '../types';
import { groupBuildingLabel, makeOrderOf, moveItem } from './helpers';

/** 이만큼 찍어 둔 뒤의 "전체 초기화"는 확인을 받는다. 한 번에 날릴 양이 크다. */
const RESET_CONFIRM_AT = 3;

/** 미지정 지점을 반짝여 보여 주는 시간(ms). */
const FLASH_MS = 3000;

/** 위·아래 이동 버튼. 아이콘은 15px이지만 누를 수 있는 영역은 24×24 이상으로 잡는다(WCAG 2.2). */
const MOVE_BTN_STYLE: CSSProperties = {
  minWidth: 24,
  minHeight: 24,
  padding: 0,
  justifyContent: 'center',
};

/** 목록 행의 건물 이름. 길면 잘라 한 줄로 둔다(전체는 title에 남는다). */
const ROW_NAME_STYLE: CSSProperties = {
  display: 'block',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
};

/** 화면에서는 감추고 스크린리더에만 읽히는 영역. CSS를 늘리지 않으려고 인라인으로 둔다. */
const SR_ONLY: CSSProperties = {
  position: 'absolute',
  width: 1,
  height: 1,
  margin: -1,
  padding: 0,
  overflow: 'hidden',
  clip: 'rect(0 0 0 0)',
  whiteSpace: 'nowrap',
};

/** 순서열이 값까지 같은지. 스토어의 같은 가드와 맞춘다 — 진짜 달라질 때만 물어야 한다. */
function sameOrder(a: readonly number[], b: readonly number[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i += 1) if (a[i] !== b[i]) return false;
  return true;
}

/**
 * S4 클러스터 순서 — 다각형을 방문 순서대로 클릭한다. 다시 누르면 해제된다.
 * 핸드오프의 기본 변형인 "A: 우측 목록 카드"를 구현했다
 * (B: 하단 순서 스트립, C: 지도 우선 최소 UI는 미구현).
 */
export function OrderScreen() {
  const origin = useSessionStore((s) => s.origin);
  const nodes = useSessionStore((s) => s.nodes);
  const groups = useSessionStore((s) => s.groups);
  const clusters = useSessionStore((s) => s.clusters);
  const clusterOrder = useSessionStore((s) => s.clusterOrder);
  const setClusterOrder = useSessionStore((s) => s.setClusterOrder);
  const setStep = useSessionStore((s) => s.setStep);
  // 순서를 바꾸면 지워지는 양. 바꾸기 전에 알려 주려고 읽는다.
  const pickCount = useSessionStore((s) => Object.keys(s.clusterPicks).length);

  const sizeById = useMemo(() => {
    const groupSize = new Map(groups.map((g) => [g.id, g.members.length]));
    return new Map(
      clusters.map((c) => [c.id, c.groupIds.reduce((n, gid) => n + (groupSize.get(gid) ?? 0), 0)]),
    );
  }, [clusters, groups]);

  /**
   * 클러스터 id → 부를 이름. 대표 그룹의 건물명에 나머지 수를 붙인다.
   * 40행이 전부 "배송지 4건"이면 재정렬은 눈을 감고 하는 일이 된다.
   */
  const nameById = useMemo(() => {
    const nodeById = new Map(nodes.map((n) => [n.id, n]));
    const groupById = new Map(groups.map((g) => [g.id, g]));
    const map = new Map<number, string>();
    for (const cluster of clusters) {
      const head = groupById.get(cluster.groupIds[0]);
      const label = head
        ? groupBuildingLabel(head.members.map((id) => nodeById.get(id)?.address ?? ''))
        : '';
      const rest = cluster.groupIds.length - 1;
      map.set(cluster.id, label && rest > 0 ? `${label} 외 ${rest}곳` : label);
    }
    return map;
  }, [clusters, groups, nodes]);

  const orderOf = useMemo(() => makeOrderOf(clusterOrder), [clusterOrder]);
  const countOf = useCallback((clusterId: number) => sizeById.get(clusterId) ?? 0, [sizeById]);
  const nameOf = useCallback((clusterId: number) => nameById.get(clusterId), [nameById]);

  /**
   * 순서열을 바꾼다. 값이 실제로 달라지면 스토어가 진입·이탈과 도로시간 행렬을 버리므로,
   * 그 첫 조작에서 한 번만 묻는다 — 지도 클릭 40번마다 물으면 아무도 읽지 않는다.
   * 사용자가 취소하면 false.
   */
  const warnedRef = useRef(false);
  const applyOrder = useCallback(
    (next: number[]): boolean => {
      const state = useSessionStore.getState();
      const picks = Object.keys(state.clusterPicks).length;
      if (picks > 0 && !warnedRef.current && !sameOrder(state.clusterOrder, next)) {
        if (!window.confirm(`순서를 바꾸면 진입·이탈 ${picks}개가 지워집니다. 계속할까요?`)) {
          return false;
        }
        warnedRef.current = true;
      }
      setClusterOrder(next);
      return true;
    },
    [setClusterOrder],
  );

  // 지도 클릭으로 새로 붙은 행. 목록이 패널 높이를 넘으면 스크롤 밖에 생겨 보이지 않는다.
  const rowRefs = useRef(new Map<number, HTMLDivElement>());
  const scrollToRef = useRef<number | null>(null);
  useEffect(() => {
    const id = scrollToRef.current;
    if (id === null) return;
    scrollToRef.current = null;
    rowRefs.current.get(id)?.scrollIntoView({ block: 'center', behavior: 'smooth' });
  }, [clusterOrder]);

  const onClusterClick = useCallback(
    (clusterId: number) => {
      const current = useSessionStore.getState().clusterOrder;
      const i = current.indexOf(clusterId);
      // 이미 순번이 있는 다각형을 다시 누르면 해제한다. 뒤 순번은 하나씩 당겨진다.
      if (i >= 0) {
        // 지도에서는 배지 여러 개가 한꺼번에 바뀌어 무엇이 일어났는지 알기 어렵다.
        if (applyOrder([...current.slice(0, i), ...current.slice(i + 1)])) {
          showInfo(`${i + 1}번 해제됨 · 다시 누르면 맨 뒤에 붙습니다`);
        }
        return;
      }
      if (applyOrder([...current, clusterId])) scrollToRef.current = clusterId;
    },
    [applyOrder],
  );

  const [moveMessage, setMoveMessage] = useState('');
  const move = useCallback(
    (from: number, to: number) => {
      const current = useSessionStore.getState().clusterOrder;
      const next = moveItem(current, from, to);
      if (next === current) return;
      if (!applyOrder(next)) return;
      // 목록이 바뀌는 것은 눈으로 보이지만, 키보드·스크린리더에는 말해 주어야 한다.
      setMoveMessage(`${from + 1}번째에서 ${to + 1}번째로 옮겼습니다`);
    },
    [applyOrder],
  );

  const resetAll = useCallback(() => {
    const state = useSessionStore.getState();
    if (state.clusterOrder.length >= RESET_CONFIRM_AT) {
      const picks = Object.keys(state.clusterPicks).length;
      const extra = picks > 0 ? ` 진입·이탈 ${picks}개도 함께 지워집니다.` : '';
      if (
        !window.confirm(`지정한 순번 ${state.clusterOrder.length}개를 모두 지웁니다.${extra}`)
      ) {
        return;
      }
      // 여기서 이미 물었으니 순서 변경 확인을 겹쳐 띄우지 않는다.
      warnedRef.current = true;
    }
    applyOrder([]);
  }, [applyOrder]);

  const { listRef, dragFrom, dropAt, onItemDragStart, onDragEnd } = useDragReorder(move);
  // 자기 바로 위·아래에 놓는 것은 아무 일도 하지 않는다. 그 자리에는 선을 긋지 않는다.
  const dropLine =
    dragFrom !== null && dropAt !== null && targetIndex(dropAt, dragFrom) !== null ? dropAt : null;

  const remain = clusters.length - clusterOrder.length;
  const allAssigned = remain === 0 && clusters.length > 0;

  // 아직 순번을 안 준 클러스터. "미지정 n개" 버튼이 이 지점들을 반짝여 알려준다.
  const unassigned = useMemo(
    () => clusters.filter((c) => !clusterOrder.includes(c.id)),
    [clusters, clusterOrder],
  );
  const [flashing, setFlashing] = useState(false);
  const flashTimer = useRef<number | null>(null);
  const flashPoints = useMemo<LatLng[]>(
    () => (flashing ? unassigned.map((c) => c.centroid) : []),
    [flashing, unassigned],
  );

  // 남은 지점이 화면 밖일 수 있으니 반짝일 때 거기에 맞춘다. 다만 버튼을 누른 순간의
  // 좌표로 한 번만 맞춘다 — 남은 지점을 계속 따라가면 하나 찍을 때마다 지도가 발밑에서
  // 다시 움직여, 다음에 찍으려던 다각형이 커서 아래에서 사라진다.
  const [fitPoints, setFitPoints] = useState<LatLng[]>([]);

  const showUnassigned = useCallback(() => {
    if (unassigned.length === 0) return;
    if (flashTimer.current !== null) window.clearTimeout(flashTimer.current);
    setFitPoints(unassigned.map((c) => c.centroid));
    setFlashing(true);
    // 3초면 눈에 들어오고, 계속 깜빡여 방해가 되지도 않는다.
    flashTimer.current = window.setTimeout(() => {
      setFlashing(false);
      setFitPoints([]);
    }, FLASH_MS);
  }, [unassigned]);

  useEffect(
    () => () => {
      if (flashTimer.current !== null) window.clearTimeout(flashTimer.current);
    },
    [],
  );

  // 목록 행에 마우스를 올리면 지도의 그 다각형을 강조한다(목록↔지도 연결).
  const [hoverClusterId, setHoverClusterId] = useState<number | undefined>(undefined);

  return (
    <div className="ro-mapscreen">
      <div className="ro-mapscreen__canvas">
        <MapCanvas center={origin ?? undefined}>
          <ClusterLayer
            clusters={clusters}
            mode="order"
            orderOf={orderOf}
            onClusterClick={onClusterClick}
            countOf={countOf}
            nameOf={nameOf}
            highlightClusterId={hoverClusterId}
          />
          <OrderLinkLayer clusters={clusters} clusterOrder={clusterOrder} origin={origin} />
          <MarkerLayer groups={groups} origin={origin} />
          <PulseLayer points={flashPoints} />
          <MapFit points={fitPoints} />
        </MapCanvas>
      </div>

      <SidePanel
        className="ro-panel--s4"
        title="방문 순서"
        note={
          <span className="ro-num" style={{ color: allAssigned ? 'var(--success-fg)' : undefined }}>
            {clusterOrder.length} / {clusters.length} 지정
          </span>
        }
        subtitle="다각형을 방문 순서대로 클릭. 다시 누르면 해제."
        extraHead={
          <>
            {pickCount > 0 ? (
              <p className="ro-hint ro-warn-text">
                순서를 바꾸면 진입·이탈 {pickCount}개가 지워집니다.
              </p>
            ) : null}
            <p aria-live="polite" style={SR_ONLY}>
              {moveMessage}
            </p>
          </>
        }
        bodyClassName="ro-panel__scroll ro-orderlist"
        bodyRef={listRef}
        footer={
          clusters.length === 0 ? (
            // 옛 세션을 이어받으면 클러스터 없이 이 화면에 설 수 있다. 이유와 돌아갈 길을 준다.
            <>
              <p className="ro-hint">
                클러스터가 없습니다. 클러스터링으로 돌아가 묶음을 먼저 만들어 주세요.
              </p>
              <button
                type="button"
                className="ro-btn ro-btn--md ro-btn--block"
                onClick={() => setStep(3)}
              >
                클러스터링으로
              </button>
            </>
          ) : (
            <>
              <button
                type="button"
                className="ro-btn ro-btn--sm ro-btn--block"
                disabled={clusterOrder.length === 0}
                onClick={() => applyOrder(clusterOrder.slice(0, -1))}
              >
                마지막 취소
              </button>
              <button
                type="button"
                className={`ro-btn ro-btn--md ro-btn--block ${
                  allAssigned ? 'ro-btn--primary' : 'ro-btn--warn'
                }`}
                // 미지정이 남아도 누를 수 있다. 다음 단계로 가는 대신 남은 지점을 지도에서 알려준다.
                onClick={() => (allAssigned ? setStep(5) : showUnassigned())}
              >
                {allAssigned ? (
                  <>
                    진입·이탈 지점
                    <ArrowRight size={18} />
                  </>
                ) : (
                  `미지정 ${remain}개 — 위치 보기`
                )}
              </button>
              {/*
                되돌릴 수 없는 조작이다. "마지막 취소" 옆에 같은 크기로 두면 8px 차이로
                지도 클릭 수십 번이 날아간다. 줄을 떼고 크기를 낮춰 무게를 달리한다.
              */}
              <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
                <button
                  type="button"
                  className="ro-btn ro-btn--xs ro-btn--danger"
                  disabled={clusterOrder.length === 0}
                  onClick={resetAll}
                >
                  전체 초기화
                </button>
              </div>
            </>
          )
        }
      >
        {clusterOrder.length === 0 ? (
          <div className="ro-table__empty">아직 지정한 클러스터가 없습니다</div>
        ) : (
          clusterOrder.map((clusterId, i) => (
            <div
              key={clusterId}
              ref={(el) => {
                if (el) rowRefs.current.set(clusterId, el);
                else rowRefs.current.delete(clusterId);
              }}
              data-reorder-item=""
              className={[
                'ro-orderitem',
                dragFrom === i ? 'is-drag' : '',
                // 삽입선은 드롭될 자리 바로 앞뒤 항목에 붙인다.
                dropLine === i ? 'is-dropbefore' : '',
                dropLine === clusterOrder.length && i === clusterOrder.length - 1
                  ? 'is-dropafter'
                  : '',
              ]
                .filter(Boolean)
                .join(' ')}
              draggable
              onDragStart={() => onItemDragStart(i)}
              onDragEnd={onDragEnd}
              onMouseEnter={() => setHoverClusterId(clusterId)}
              onMouseLeave={() => setHoverClusterId(undefined)}
            >
              <span className="ro-orderitem__handle" aria-hidden>
                <GripVertical size={16} />
              </span>
              {/*
                클러스터 id는 더 보여 주지 않는다. 지도에는 방문 순번이 찍히는데
                목록에 '클러스터 6, 클러스터 1'이 섞여 나오면 순서가 틀린 것처럼 읽힌다.
                대신 어디인지 알 수 있게 대표 건물 이름을 적는다.
              */}
              <span className="ro-orderitem__no">{i + 1}</span>
              {/* 300px 패널에 이름과 건수를 나란히 놓으면 이름이 두세 줄로 접힌다.
                  이름은 한 줄로 자르고(전체는 title로), 건수는 그 아래 줄에 둔다. */}
              <span className="ro-orderitem__name" style={{ minWidth: 0 }}>
                <span style={ROW_NAME_STYLE} title={nameById.get(clusterId) || undefined}>
                  {nameById.get(clusterId) || '이름 없는 묶음'}
                </span>
                <span className="ro-orderitem__count">배송지 {sizeById.get(clusterId) ?? 0}건</span>
              </span>
              <span style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                <button
                  type="button"
                  className="ro-orderitem__move"
                  style={MOVE_BTN_STYLE}
                  aria-label={`${i + 1}번째를 위로`}
                  disabled={i === 0}
                  onClick={() => move(i, i - 1)}
                >
                  <ChevronUp size={15} />
                </button>
                <button
                  type="button"
                  className="ro-orderitem__move"
                  style={MOVE_BTN_STYLE}
                  aria-label={`${i + 1}번째를 아래로`}
                  disabled={i === clusterOrder.length - 1}
                  onClick={() => move(i, i + 1)}
                >
                  <ChevronDown size={15} />
                </button>
              </span>
            </div>
          ))
        )}
      </SidePanel>
    </div>
  );
}
