import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { ArrowRight, ChevronDown, ChevronUp, GripVertical } from 'lucide-react';

import { MapFit, SidePanel } from '../components';
import { useDragReorder } from '../hooks/useDragReorder';
import { ClusterLayer, MapCanvas, MarkerLayer, OrderLinkLayer } from '../map';
import { PulseLayer } from '../map/PulseLayer';
import { useSessionStore } from '../store/session';
import type { LatLng } from '../types';
import { makeOrderOf, moveItem } from './helpers';

/**
 * S4 클러스터 순서 — 다각형을 방문 순서대로 클릭한다. 다시 누르면 해제된다.
 * 핸드오프의 기본 변형인 "A: 우측 목록 카드"를 구현했다
 * (B: 하단 순서 스트립, C: 지도 우선 최소 UI는 미구현).
 */
export function OrderScreen() {
  const origin = useSessionStore((s) => s.origin);
  const groups = useSessionStore((s) => s.groups);
  const clusters = useSessionStore((s) => s.clusters);
  const clusterOrder = useSessionStore((s) => s.clusterOrder);
  const setClusterOrder = useSessionStore((s) => s.setClusterOrder);
  const setStep = useSessionStore((s) => s.setStep);


  const sizeById = useMemo(() => {
    const groupSize = new Map(groups.map((g) => [g.id, g.members.length]));
    return new Map(
      clusters.map((c) => [c.id, c.groupIds.reduce((n, gid) => n + (groupSize.get(gid) ?? 0), 0)]),
    );
  }, [clusters, groups]);

  const orderOf = useMemo(() => makeOrderOf(clusterOrder), [clusterOrder]);

  const onClusterClick = useCallback(
    (clusterId: number) => {
      const current = useSessionStore.getState().clusterOrder;
      const i = current.indexOf(clusterId);
      // 이미 순번이 있는 다각형을 다시 누르면 해제한다. 뒤 순번은 하나씩 당겨진다.
      if (i >= 0) {
        setClusterOrder([...current.slice(0, i), ...current.slice(i + 1)]);
        return;
      }
      setClusterOrder([...current, clusterId]);
    },
    [setClusterOrder],
  );

  const move = useCallback(
    (from: number, to: number) => {
      const current = useSessionStore.getState().clusterOrder;
      const next = moveItem(current, from, to);
      if (next !== current) setClusterOrder(next);
    },
    [setClusterOrder],
  );

  const { listRef, dragFrom, dropAt, onItemDragStart, onDragEnd } = useDragReorder(move);

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

  // 남은 지점이 화면 밖일 수 있으니 반짝일 때 거기에 맞춘다.
  const fitPoints = useMemo<LatLng[]>(() => (flashing ? flashPoints : []), [flashing, flashPoints]);

  const showUnassigned = useCallback(() => {
    if (unassigned.length === 0) return;
    if (flashTimer.current !== null) window.clearTimeout(flashTimer.current);
    setFlashing(true);
    // 3초면 눈에 들어오고, 계속 깜빡여 방해가 되지도 않는다.
    flashTimer.current = window.setTimeout(() => setFlashing(false), 3000);
  }, [unassigned.length]);

  useEffect(
    () => () => {
      if (flashTimer.current !== null) window.clearTimeout(flashTimer.current);
    },
    [],
  );

  return (
    <div className="ro-mapscreen">
      <div className="ro-mapscreen__canvas">
        <MapCanvas center={origin ?? undefined}>
          <ClusterLayer
            clusters={clusters}
            mode="order"
            orderOf={orderOf}
            onClusterClick={onClusterClick}
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
        subtitle="지도의 다각형을 방문할 순서대로 클릭하세요. 순번이 매겨진 다각형을 다시 누르면 해제됩니다. 목록은 드래그해 바꿀 수 있습니다."
        bodyClassName="ro-panel__scroll ro-orderlist"
        bodyRef={listRef}
        footer={
          <>
            <div className="ro-row">
              <button
                type="button"
                className="ro-btn ro-btn--sm ro-btn--grow"
                disabled={clusterOrder.length === 0}
                onClick={() => setClusterOrder(clusterOrder.slice(0, -1))}
              >
                마지막 취소
              </button>
              <button
                type="button"
                className="ro-btn ro-btn--sm ro-btn--grow ro-btn--danger"
                disabled={clusterOrder.length === 0}
                onClick={() => setClusterOrder([])}
              >
                전체 초기화
              </button>
            </div>
            <button
              type="button"
              className={`ro-btn ro-btn--md ro-btn--block ${
                allAssigned ? 'ro-btn--primary' : 'ro-btn--warn'
              }`}
              // 미지정이 남아도 누를 수 있다. 다음 단계로 가는 대신 남은 지점을 지도에서 알려준다.
              disabled={clusters.length === 0}
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
          </>
        }
      >
        {clusterOrder.length === 0 ? (
          <div className="ro-table__empty">아직 지정한 클러스터가 없습니다</div>
        ) : (
          clusterOrder.map((clusterId, i) => (
            <div
              key={clusterId}
              data-reorder-item=""
              className={[
                'ro-orderitem',
                dragFrom === i ? 'is-drag' : '',
                // 삽입선은 드롭될 자리 바로 앞뒤 항목에 붙인다.
                dragFrom !== null && dropAt === i ? 'is-dropbefore' : '',
                dragFrom !== null && dropAt === clusterOrder.length && i === clusterOrder.length - 1
                  ? 'is-dropafter'
                  : '',
              ]
                .filter(Boolean)
                .join(' ')}
              draggable
              onDragStart={() => onItemDragStart(i)}
              onDragEnd={onDragEnd}
            >
              <span className="ro-orderitem__handle" aria-hidden>
                <GripVertical size={16} />
              </span>
              {/*
                클러스터 id는 더 보여 주지 않는다. 지도에는 방문 순번이 찍히는데
                목록에 '클러스터 6, 클러스터 1'이 섞여 나오면 순서가 틀린 것처럼 읽힌다.
                이 줄에서 뜻이 있는 숫자는 방문 순번과 배송지 건수뿐이다.
              */}
              <span className="ro-orderitem__no">{i + 1}</span>
              <span className="ro-orderitem__name">배송지 {sizeById.get(clusterId) ?? 0}건</span>
              <span style={{ display: 'flex', flexDirection: 'column' }}>
                <button
                  type="button"
                  className="ro-orderitem__move"
                  aria-label="위로"
                  disabled={i === 0}
                  onClick={() => move(i, i - 1)}
                >
                  <ChevronUp size={15} />
                </button>
                <button
                  type="button"
                  className="ro-orderitem__move"
                  aria-label="아래로"
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
