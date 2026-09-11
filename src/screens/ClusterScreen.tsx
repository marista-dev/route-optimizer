import { useCallback, useEffect, useMemo, useState } from 'react';
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

/** 기본값 눈금이 트랙에서 놓일 위치(%). 라벨을 실제 400m 자리에 앉히려고 쓴다. */
const DEFAULT_MARK_PCT =
  ((DEFAULT_THRESHOLD_M - MIN_THRESHOLD_M) / (MAX_THRESHOLD_M - MIN_THRESHOLD_M)) * 100;

/**
 * 클러스터 수의 권장 범위.
 * 밖이라고 틀린 것은 아니다 — 400m에서 30~40개가 보통이고 상한 500m로도 들어가지
 * 못할 수 있다. 그래서 범위 밖에서는 지적 대신 결과(다음 단계의 클릭 횟수)를 말한다.
 */
const RECOMMEND_MIN = 20;
const RECOMMEND_MAX = 30;

/** 인포윈도우에 그대로 적을 최대 줄 수. 넘치면 "외 n곳"으로 줄인다. */
const INFO_MAX_LINES = 12;

/** 슬라이더에서 손을 뗀 것으로 보고 스토어에 값을 넣기까지 기다리는 시간(ms). */
const THRESHOLD_SAVE_DELAY_MS = 300;

/** S3 클러스터링 — 임계값 슬라이더 + 다각형 미리보기. */
export function ClusterScreen() {
  const nodes = useSessionStore((s) => s.nodes);
  const origin = useSessionStore((s) => s.origin);
  const storedThreshold = useSessionStore((s) => s.thresholdM);
  const setThreshold = useSessionStore((s) => s.setThreshold);
  const setClusters = useSessionStore((s) => s.setClusters);
  const setStep = useSessionStore((s) => s.setStep);
  // 저장된 구성·작업량. "이 버튼이 무엇을 지우는지"를 누르기 전에 말하려고 읽는다.
  const storedGroups = useSessionStore((s) => s.groups);
  const storedClusters = useSessionStore((s) => s.clusters);
  const orderedCount = useSessionStore((s) => s.clusterOrder.length);
  const pickCount = useSessionStore((s) => Object.keys(s.clusterPicks).length);

  // 슬라이더는 즉시 반응해야 하므로 로컬 상태로 두고, 조작이 멎으면 스토어에 옮긴다
  // (아래 effect). 예전 세션이 500m를 넘는 값을 들고 있을 수 있어 범위 안으로 잘라서 시작한다.
  const [thresholdM, setLocalThreshold] = useState(() =>
    Math.min(Math.max(storedThreshold, MIN_THRESHOLD_M), MAX_THRESHOLD_M),
  );

  const groups = useMemo(() => buildPrimaryGroups(nodes), [nodes]);
  const clusters = useMemo(() => buildClusters(groups, thresholdM), [groups, thresholdM]);
  const fitPoints = useMemo<LatLng[]>(
    () => groups.map((g) => ({ lat: g.lat, lon: g.lon })),
    [groups],
  );

  // 임계값 자체는 파괴적이지 않다(thresholdM만 바꾼다). 조작이 멎으면 바로 저장해
  // 다른 단계에 갔다 와도 값이 남게 한다. 파괴적인 setClusters는 버튼에서만 부른다.
  useEffect(() => {
    if (thresholdM === storedThreshold) return;
    const timer = window.setTimeout(() => setThreshold(thresholdM), THRESHOLD_SAVE_DELAY_MS);
    return () => window.clearTimeout(timer);
  }, [thresholdM, storedThreshold, setThreshold]);

  const nodeById = useMemo(() => new Map(nodes.map((n) => [n.id, n])), [nodes]);
  const groupById = useMemo(() => new Map(groups.map((g) => [g.id, g])), [groups]);

  /** 클러스터 id → 배송 건수(그룹 수가 아니다). 지도 라벨과 통계가 같은 수를 쓴다. */
  const sizeById = useMemo(
    () =>
      new Map(
        clusters.map((c) => [
          c.id,
          c.groupIds.reduce((n, gid) => n + (groupById.get(gid)?.members.length ?? 0), 0),
        ]),
      ),
    [clusters, groupById],
  );
  const countOf = useCallback((clusterId: number) => sizeById.get(clusterId) ?? 0, [sizeById]);

  /** 클러스터 id → 부를 이름. 대표 그룹의 건물명에 나머지 수를 붙인다. */
  const nameById = useMemo(() => {
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
  }, [clusters, groupById, nodeById]);
  const nameOf = useCallback((clusterId: number) => nameById.get(clusterId), [nameById]);

  const maxMembers = clusters.reduce((max, c) => Math.max(max, sizeById.get(c.id) ?? 0), 0);
  const placedNodes = groups.reduce((n, g) => n + g.members.length, 0);
  // 좌표를 못 얻은 노드는 그룹에 들어가지 못해 통계에서 조용히 빠진다(core/grouping).
  const excludedNodes = useMemo(() => nodes.filter((n) => n.lat === null).length, [nodes]);

  // 지금 임계값으로 만든 구성이 저장된 것과 다르면, 넘어가는 순간 S4~S6가 지워진다.
  const changesClustering = useMemo(
    () => !sameClustering(storedGroups, storedClusters, groups, clusters),
    [storedGroups, storedClusters, groups, clusters],
  );
  const savedWork = orderedCount > 0 || pickCount > 0;

  const clusterHint =
    clusters.length < RECOMMEND_MIN
      ? `다음 단계에서 지도 클릭이 ${clusters.length}번 필요합니다. 한 클러스터가 너무 크면 임계값을 내려 나누세요.`
      : clusters.length > RECOMMEND_MAX
        ? `다음 단계에서 지도 클릭이 ${clusters.length}번 필요합니다. 줄이려면 임계값을 올리세요.`
        : `권장 범위(${RECOMMEND_MIN}~${RECOMMEND_MAX}개) 안입니다. 다음 단계에서 지도 클릭이 ${clusters.length}번 필요합니다.`;

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
      // 500m까지 올리면 한 클러스터에 20~30곳이 묶인다. 인포윈도우는 지도 안에
      // 갇혀 있어 넘친 줄은 읽을 수도 스크롤할 수도 없으므로 내용 쪽에서 막는다.
      const shown = lines.slice(0, INFO_MAX_LINES);
      const rest = lines.length - shown.length;
      if (rest > 0) shown.push(`외 ${rest}곳`);
      return `<div class="ro-info" style="max-height:300px;overflow:auto"><b>클러스터 ${cluster.id + 1}</b><br/>${shown.join('<br/>')}</div>`;
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
            countOf={countOf}
            // 1건짜리 클러스터는 중심 히트 원이 마커를 덮어 마커 툴팁이 뜨지 않는다.
            // 히트 원 라벨에도 같은 건물명을 넣어 어디를 가리키는지 알 수 있게 한다.
            nameOf={nameOf}
          />
          <MarkerLayer groups={groups} origin={origin} tooltipOf={tooltipOf} />
        </MapCanvas>
      </div>

      <SidePanel
        className="ro-panel--s3"
        title="클러스터링"
        note="가까운 배송지끼리 묶기"
        footer={
          clusters.length === 0 ? (
            // 좌표가 하나도 없으면 묶을 것이 없다. 회색 버튼만 두지 말고 이유와 돌아갈 길을 준다.
            <>
              <p className="ro-hint">
                좌표를 얻은 배송지가 없어 묶을 것이 없습니다. 주소검증으로 돌아가 좌표를 채워
                주세요.
              </p>
              <button
                type="button"
                className="ro-btn ro-btn--md ro-btn--block"
                onClick={() => setStep(2)}
              >
                주소검증으로
              </button>
            </>
          ) : (
            <>
              {changesClustering && savedWork ? (
                <p className="ro-hint ro-warn-text">
                  임계값을 바꿔 클러스터가 달라집니다. 저장된 순번 {orderedCount}개와 진입·이탈{' '}
                  {pickCount}개는 넘어가는 순간 지워집니다.
                </p>
              ) : null}
              <button
                type="button"
                className="ro-btn ro-btn--md ro-btn--primary ro-btn--block"
                onClick={() => {
                  // 방금 계산한 값과 저장된 값을 견줘 본다.
                  //  - 다르면 반드시 덮어쓴다. 주소를 고쳐 노드가 바뀌었는데 임계값만 같으면
                  //    옛 클러스터가 살아남아 S4~S6가 없는 그룹 id를 다루게 된다(H6).
                  //  - 같으면 쓰지 않는다. setClusters는 순서·진입·이탈을 모두 지우므로,
                  //    S3를 확인차 들렀다 나오는 것만으로 확정한 작업이 날아간다(NEW-2).
                  const stored = useSessionStore.getState();
                  setThreshold(thresholdM);
                  if (!sameClustering(stored.groups, stored.clusters, groups, clusters)) {
                    // 지우기 직전에 한 번 묻는다. 되돌릴 방법이 없는 조작이다.
                    if (
                      savedWork &&
                      !window.confirm(
                        `클러스터가 달라집니다. 저장된 순번 ${orderedCount}개와 진입·이탈 ${pickCount}개가 지워집니다. 계속할까요?`,
                      )
                    ) {
                      return;
                    }
                    setClusters(groups, clusters);
                  }
                  setStep(4);
                }}
              >
                순서배정
                <ArrowRight size={18} />
              </button>
            </>
          )
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
          {/*
            눈금은 space-between이라 가운데 칸이 트랙 한가운데(=350m)에 놓인다.
            기본값 라벨이 400m가 아닌 자리를 가리키면 눈금을 한 칸 속이는 셈이라,
            실제 400m 지점에 직접 앉힌다.
          */}
          <div className="ro-range-scale" style={{ position: 'relative' }}>
            <span>{MIN_THRESHOLD_M}m</span>
            <span
              style={{
                position: 'absolute',
                left: `${DEFAULT_MARK_PCT}%`,
                transform: 'translateX(-50%)',
              }}
            >
              기본 {DEFAULT_THRESHOLD_M}m
            </span>
            <span>{MAX_THRESHOLD_M}m</span>
          </div>
        </div>

        <div className="ro-stats">
          <div className="ro-stat">
            <div className="ro-stat__label">클러스터</div>
            <div className="ro-stat__value">{clusters.length}</div>
          </div>
          <div className="ro-stat">
            <div className="ro-stat__label">가장 많은 배송지</div>
            <div className="ro-stat__value">{maxMembers}</div>
          </div>
          <div className="ro-stat">
            <div className="ro-stat__label">전체 배송지</div>
            <div className="ro-stat__value">{placedNodes}</div>
            {/* 좌표를 못 얻은 행은 여기 숫자에 들어 있지 않다. 마지막으로 되돌아볼 기회를 준다. */}
            {excludedNodes > 0 ? (
              <div className="ro-stat__label ro-warn-text">제외 {excludedNodes}건</div>
            ) : null}
          </div>
        </div>

        <p className="ro-hint">{clusterHint}</p>

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
