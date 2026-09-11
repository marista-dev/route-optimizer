import { useCallback, useMemo, useState } from 'react';
import { ArrowRight } from 'lucide-react';

import { MapFit, SidePanel } from '../components';
import { buildClusters, buildPrimaryGroups } from '../core';
import { ClusterLayer, MapCanvas, MarkerLayer } from '../map';
import { useSessionStore } from '../store/session';
import { DEFAULT_THRESHOLD_M, type LatLng } from '../types';
import { escapeHtml, groupBuildingLabel, sameClustering } from './helpers';

/*
 * 임계값 슬라이더의 범위. 500m를 넘으면 멀리 떨어진 동네까지 한 덩어리로 묶여
 * 클러스터 순서를 사람이 정한다는 전제가 무너지므로 위를 500m로 막았다.
 * 10m 단위로 움직여야 경계에 걸친 동을 붙이거나 뗄 수 있다.
 */
const MIN_THRESHOLD_M = 200;
const MAX_THRESHOLD_M = 500;
const THRESHOLD_STEP_M = 10;

/** S3 클러스터링 — 임계값 슬라이더 + 다각형 미리보기. */
export function ClusterScreen() {
  const nodes = useSessionStore((s) => s.nodes);
  const origin = useSessionStore((s) => s.origin);
  const storedThreshold = useSessionStore((s) => s.thresholdM);
  const setThreshold = useSessionStore((s) => s.setThreshold);
  const setClusters = useSessionStore((s) => s.setClusters);
  const setStep = useSessionStore((s) => s.setStep);

  // 슬라이더는 즉시 반응해야 하므로 로컬 상태로 두고, 다음 단계로 넘어갈 때 스토어에 넣는다.
  // 예전 세션이 500m를 넘는 값을 들고 있을 수 있어 범위 안으로 잘라서 시작한다.
  const [thresholdM, setLocalThreshold] = useState(() =>
    Math.min(Math.max(storedThreshold, MIN_THRESHOLD_M), MAX_THRESHOLD_M),
  );

  const groups = useMemo(() => buildPrimaryGroups(nodes), [nodes]);
  const clusters = useMemo(() => buildClusters(groups, thresholdM), [groups, thresholdM]);
  const fitPoints = useMemo<LatLng[]>(
    () => groups.map((g) => ({ lat: g.lat, lon: g.lon })),
    [groups],
  );

  const nodeById = useMemo(() => new Map(nodes.map((n) => [n.id, n])), [nodes]);
  const groupById = useMemo(() => new Map(groups.map((g) => [g.id, g])), [groups]);

  const maxMembers = clusters.reduce(
    (max, c) => Math.max(max, c.groupIds.reduce((n, gid) => n + (groupById.get(gid)?.members.length ?? 0), 0)),
    0,
  );
  const placedNodes = groups.reduce((n, g) => n + g.members.length, 0);

  const memberSummary = useCallback(
    (clusterId: number): string => {
      const cluster = clusters.find((c) => c.id === clusterId);
      if (!cluster) return '';
      const lines = cluster.groupIds.flatMap((gid) => {
        const group = groupById.get(gid);
        if (!group) return [];
        // 사람 이름이 아니라 건물(단지) 이름으로 부른다(현장 피드백 3).
        const rep = nodeById.get(group.rep);
        const label = groupBuildingLabel(group.members.map((id) => nodeById.get(id)?.address ?? ''));
        const count = group.members.length > 1 ? ` · ${group.members.length}건` : '';
        return [
          `${escapeHtml(label || `그룹 ${gid}`)}${count} · ${escapeHtml(rep?.address ?? '')}`,
        ];
      });
      return `<div class="ro-info"><b>클러스터 ${cluster.id + 1}</b><br/>${lines.join('<br/>')}</div>`;
    },
    [clusters, groupById, nodeById],
  );

  /** 마커 툴팁 — 건물(단지) 이름. */
  const tooltipOf = useCallback(
    (groupId: number) => {
      const group = groupById.get(groupId);
      if (!group) return undefined;
      return groupBuildingLabel(group.members.map((id) => nodeById.get(id)?.address ?? ''));
    },
    [groupById, nodeById],
  );

  const noOrder = useCallback(() => undefined, []);

  return (
    <div className="ro-mapscreen">
      <div className="ro-mapscreen__canvas">
        <MapCanvas center={origin ?? undefined}>
          <MapFit points={fitPoints} />
          <ClusterLayer
            clusters={clusters}
            mode="info"
            orderOf={noOrder}
            memberSummary={memberSummary}
          />
          <MarkerLayer groups={groups} origin={origin} tooltipOf={tooltipOf} />
        </MapCanvas>
      </div>

      <SidePanel
        className="ro-panel--s3"
        title="클러스터링"
        note="2차 클러스터 · Union-Find"
        footer={
          <button
            type="button"
            className="ro-btn ro-btn--md ro-btn--primary ro-btn--block"
            disabled={clusters.length === 0}
            onClick={() => {
              // 방금 계산한 값과 저장된 값을 견줘 본다.
              //  - 다르면 반드시 덮어쓴다. 주소를 고쳐 노드가 바뀌었는데 임계값만 같으면
              //    옛 클러스터가 살아남아 S4~S6가 없는 그룹 id를 다루게 된다(H6).
              //  - 같으면 쓰지 않는다. setClusters는 순서·진입·이탈을 모두 지우므로,
              //    S3를 확인차 들렀다 나오는 것만으로 확정한 작업이 날아간다(NEW-2).
              const stored = useSessionStore.getState();
              setThreshold(thresholdM);
              if (!sameClustering(stored.groups, stored.clusters, groups, clusters)) {
                setClusters(groups, clusters);
              }
              setStep(4);
            }}
          >
            순서배정
            <ArrowRight size={18} />
          </button>
        }
      >
        <div className="ro-field">
          <div className="ro-progress__head">
            <span>임계값</span>
            <span className="ro-num" style={{ fontWeight: 700 }}>
              {thresholdM} m
            </span>
          </div>
          <input
            className="ro-range"
            type="range"
            min={MIN_THRESHOLD_M}
            max={MAX_THRESHOLD_M}
            step={THRESHOLD_STEP_M}
            value={thresholdM}
            aria-label="2차 클러스터 임계값(m)"
            onChange={(e) => setLocalThreshold(Number(e.target.value))}
          />
          <div className="ro-range-scale">
            <span>{MIN_THRESHOLD_M}m</span>
            <span>기본 {DEFAULT_THRESHOLD_M}m</span>
            <span>{MAX_THRESHOLD_M}m</span>
          </div>
        </div>

        <div className="ro-stats">
          <div className="ro-stat">
            <div className="ro-stat__label">클러스터</div>
            <div className="ro-stat__value">{clusters.length}</div>
          </div>
          <div className="ro-stat">
            <div className="ro-stat__label">최대 멤버</div>
            <div className="ro-stat__value">{maxMembers}</div>
          </div>
          <div className="ro-stat">
            <div className="ro-stat__label">배송지</div>
            <div className="ro-stat__value">{placedNodes}</div>
          </div>
        </div>

        <p className="ro-hint">
          클러스터가 많으면 다음 단계에서 클릭이 늘어납니다. 20~30개를 권장합니다.
        </p>

        <div className="ro-legend">
          <span>
            <span className="ro-legend__marker" />
            배송지(지점)
          </span>
          <span>
            <span className="ro-legend__poly" />
            클러스터
          </span>
        </div>
      </SidePanel>
    </div>
  );
}
