import { useCallback, useMemo, useState } from 'react';
import type { CSSProperties } from 'react';
import { ArrowRight } from 'lucide-react';

import { MapFit, ProgressBar, RateLimitModal, SidePanel } from '../components';
import { ClusterLayer, MapCanvas, MarkerLayer } from '../map';
import { useSessionStore } from '../store/session';
import type { LatLng } from '../types';
import { firstUnconfirmedIndex, groupBuildingLabel, makeOrderOf } from './helpers';
import { useAutoRoute } from './useAutoRoute';
import type { AutoRouteStage } from './useAutoRoute';

/** 초 단위 대략치를 "약 N초" / "약 N분" 문구로. 라운딩된 추정치라 "약"을 붙인다. */
function formatEstimatedTime(seconds: number): string {
  if (seconds < 60) return `약 ${seconds}초`;
  const minutes = Math.round(seconds / 60);
  return `약 ${minutes}분`;
}

/** 단계별 한 줄 설명. */
function stageLabel(stage: AutoRouteStage): string {
  switch (stage) {
    case 'matrix':
      return '클러스터 간 도로시간을 받는 중입니다.';
    case 'clusters':
      return '클러스터별 진입·이탈과 내부 순서를 정하는 중입니다.';
    case 'done':
      return '계산이 끝났습니다.';
    case 'stopped':
      // H1: 진행분을 들고 새로 선 화면도 이 문구를 보여준다 — 빈 패널로 두지 않는다.
      return '지난 계산이 멈춰 있습니다.';
    default:
      return '';
  }
}

/** 목록 행 이름. 길면 잘라 한 줄로 둔다(전체는 title에 남는다) — `OrderScreen`과 같은 규칙. */
const ROW_NAME_STYLE: CSSProperties = {
  display: 'block',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
};

/** 확정된 클러스터 한 줄. */
interface ConfirmedRow {
  clusterId: number;
  orderIndex: number;
  name: string;
  count: number;
  /** 실제로 도로시간을 부른 칸 수 */
  calls: number;
  /** 그중 직선거리로 대체된 칸 수 */
  fallbacks: number;
}

/**
 * S4-자동 "완전 자동 경로 모드" 계산 화면.
 * 지도에는 클러스터 다각형만 띄운다(개별 진입·이탈 선택이 없어 마커 강조는 필요 없다).
 * 진행·중단·rate-limit 모달 패턴은 `EntryExitScreen`(S5)을 그대로 따른다.
 */
export function AutoRouteScreen() {
  const origin = useSessionStore((s) => s.origin);
  const nodes = useSessionStore((s) => s.nodes);
  const groups = useSessionStore((s) => s.groups);
  const clusters = useSessionStore((s) => s.clusters);
  const clusterOrder = useSessionStore((s) => s.clusterOrder);
  const clusterPicks = useSessionStore((s) => s.clusterPicks);
  // M3: 클러스터 간(대표 사이) 행렬의 직선거리 대체 칸 — 원시 배열이라 그대로 구독해도
  // 안전하다(`.map`/`Object.keys`로 매 렌더 새 배열을 만드는 셀렉터만 무한 렌더 위험이 있다).
  const clusterHaversineKeys = useSessionStore((s) => s.clusterHaversineKeys);
  const setStep = useSessionStore((s) => s.setStep);

  const {
    stage,
    running,
    progress,
    clusterProgress,
    error,
    stopReason,
    rateLimited,
    rateLimitSeen,
    haversineMode,
    estimate,
    start,
    stop,
    retryWithKey,
    continueWithHaversine,
    disableHaversineMode,
    closeRateLimitModal,
    reopenRateLimitModal,
    viewResult,
  } = useAutoRoute();

  const nodeById = useMemo(() => new Map(nodes.map((n) => [n.id, n])), [nodes]);
  const groupById = useMemo(() => new Map(groups.map((g) => [g.id, g])), [groups]);

  /** 마커 툴팁 — 건물(단지) 이름(다른 화면과 같은 규칙). */
  const tooltipOf = useCallback(
    (groupId: number) => {
      const group = groupById.get(groupId);
      if (!group) return undefined;
      return groupBuildingLabel(group.members.map((id) => nodeById.get(id)?.address ?? ''));
    },
    [groupById, nodeById],
  );

  const orderOf = useMemo(() => makeOrderOf(clusterOrder), [clusterOrder]);
  const confirmedCount = useMemo(
    () => clusterOrder.filter((id) => clusterPicks[id]?.innerOrder !== undefined).length,
    [clusterOrder, clusterPicks],
  );

  // 전체 클러스터가 한 화면에 들어와야 무엇을 계산하는지 가늠할 수 있다.
  const fitPoints = useMemo<LatLng[]>(() => {
    const points = clusters.flatMap((c) => c.hull);
    return origin ? [...points, { lat: origin.lat, lon: origin.lon }] : points;
  }, [clusters, origin]);

  const clusterById = useMemo(() => new Map(clusters.map((c) => [c.id, c])), [clusters]);

  /** 클러스터 id → 부를 이름. `ClusterScreen`/`OrderScreen`과 같은 규칙(대표 그룹 건물명 + 나머지 수). */
  const nameOf = useCallback(
    (clusterId: number): string => {
      const cluster = clusterById.get(clusterId);
      if (!cluster) return '';
      const head = groupById.get(cluster.groupIds[0]);
      const label = head
        ? groupBuildingLabel(head.members.map((id) => nodeById.get(id)?.address ?? ''))
        : '';
      const rest = cluster.groupIds.length - 1;
      return label && rest > 0 ? `${label} 외 ${rest}곳` : label;
    },
    [clusterById, groupById, nodeById],
  );

  /** 클러스터 id → 배송지 건수(그룹 수가 아니다). */
  const countOf = useCallback(
    (clusterId: number): number => {
      const cluster = clusterById.get(clusterId);
      if (!cluster) return 0;
      return cluster.groupIds.reduce((n, gid) => n + (groupById.get(gid)?.members.length ?? 0), 0);
    },
    [clusterById, groupById],
  );

  /**
   * 확정된 클러스터 한 줄씩. "실제로 도로시간을 몇 번 불렀는지"는 `ResultScreen`의
   * `timeStats`와 같은 계산이다 — `timeMatrix` 칸 수에서 직선거리로 메운 칸(`haversineFallbacks`)을
   * 뺀 값이 실제 호출 수다.
   */
  const confirmedRows = useMemo<ConfirmedRow[]>(() => {
    const rows: ConfirmedRow[] = [];
    clusterOrder.forEach((id, i) => {
      const pick = clusterPicks[id];
      if (!pick || pick.innerOrder === undefined) return;
      const cells = Object.keys(pick.timeMatrix ?? {}).length;
      const fallbacks = pick.haversineFallbacks ?? 0;
      rows.push({
        clusterId: id,
        orderIndex: i + 1,
        name: nameOf(id),
        count: countOf(id),
        calls: cells - fallbacks,
        fallbacks,
      });
    });
    return rows;
  }, [clusterOrder, clusterPicks, nameOf, countOf]);

  /*
   * 정렬 규칙:
   *  - 계산이 아직 진행 중(또는 멈춘 상태)이면 최신(순번이 큰 것)을 위로 둔다 —
   *    방금 확정한 클러스터가 스크롤 없이 바로 보여야 "지금 어디까지 됐는지"를
   *    맨 위에서 확인할 수 있다.
   *  - 전부 끝나면 방문 순번대로(1번이 위) 되돌린다 — 다 끝난 뒤에는 위에서
   *    아래로 읽는 최종 경로 순서가 더 유용하다.
   */
  const sortedRows = useMemo(
    () => (stage === 'done' ? confirmedRows : [...confirmedRows].reverse()),
    [confirmedRows, stage],
  );

  // 지금 처리 중(또는 중단·rate-limit으로 멈췄다면 다음에 이어서 할) 클러스터.
  // 스토어 값만으로 계산되므로 화면을 나갔다 돌아와도 그대로 복원된다(중단 후 재진입).
  // 전부 확정했거나 순서가 비어 있으면 -1 — "지금 처리 중인 클러스터 없음"으로 그대로 쓴다
  // (useAutoRoute의 루프 종료 판정과는 반대 해석 — 이 화면은 이어서 계산이 아니라 표시용).
  const currentIndex = useMemo(
    () => firstUnconfirmedIndex(clusterOrder, clusterPicks),
    [clusterOrder, clusterPicks],
  );
  const currentClusterId =
    currentIndex >= 0 && currentIndex < clusterOrder.length ? clusterOrder[currentIndex] : null;

  // 목록 행에 마우스를 올리면 지도의 그 다각형을 강조한다(`OrderScreen`과 같은 패턴).
  // `ClusterLayer`가 prop을 이미 지원하므로 그대로 쓴다.
  const [hoverClusterId, setHoverClusterId] = useState<number | undefined>(undefined);

  if (clusters.length === 0) {
    return (
      <div className="ro-screen-scroll">
        <div className="ro-s1">
          <p className="ro-hint">클러스터가 없습니다. 클러스터링을 먼저 해 주세요.</p>
          <button type="button" className="ro-btn ro-btn--md" onClick={() => setStep(3)}>
            클러스터링으로
          </button>
        </div>
      </div>
    );
  }

  const idle = stage === 'idle' && !running && !error && !stopReason;
  const paused = !running && (stopReason !== null || error !== null);
  // step 6에서 스테퍼로 되돌아왔을 때(이미 다 확정된 채로 이 화면에 다시 섬).
  // 조용히 재계산하지 않고, 실제로 끝났다는 사실과 되돌아갈 버튼을 보여준다.
  const doneAlready = stage === 'done' && !running;

  return (
    <div className="ro-mapscreen">
      <div className="ro-mapscreen__canvas">
        <MapCanvas center={origin ?? undefined}>
          <MapFit points={fitPoints} />
          <ClusterLayer
            clusters={clusters}
            mode="order"
            orderOf={orderOf}
            highlightClusterId={hoverClusterId}
          />
          <MarkerLayer groups={groups} origin={origin} tooltipOf={tooltipOf} />
        </MapCanvas>
      </div>

      <SidePanel
        className="ro-panel--s4"
        title="자동 계산"
        note={
          clusterOrder.length > 0 ? (
            <span className="ro-num">
              확정 {confirmedCount} / {clusterOrder.length}
            </span>
          ) : (
            <span className="ro-num">클러스터 {clusters.length}개</span>
          )
        }
        subtitle={
          stage === 'matrix' || stage === 'clusters' || stage === 'stopped' || doneAlready
            ? stageLabel(stage)
            : undefined
        }
        footer={
          <>
            {idle ? (
              <button
                type="button"
                className="ro-btn ro-btn--md ro-btn--primary ro-btn--block"
                onClick={start}
              >
                자동 계산 시작
                <ArrowRight size={18} />
              </button>
            ) : null}

            {running ? (
              <button
                type="button"
                className="ro-btn ro-btn--md ro-btn--danger ro-btn--block"
                onClick={stop}
              >
                중단
              </button>
            ) : null}

            {paused ? (
              <>
                <p className="ro-hint ro-warn-text">{error ?? stopReason}</p>
                <button
                  type="button"
                  className="ro-btn ro-btn--md ro-btn--primary ro-btn--block"
                  onClick={start}
                >
                  이어서 계산
                  <ArrowRight size={18} />
                </button>
              </>
            ) : null}

            {doneAlready ? (
              <button
                type="button"
                className="ro-btn ro-btn--md ro-btn--dark ro-btn--block"
                onClick={viewResult}
              >
                결과 보기
                <ArrowRight size={18} />
              </button>
            ) : null}

            {rateLimitSeen && !rateLimited ? (
              <div className="ro-calls">
                <span className="ro-warn-text">
                  API 한도 감지됨 · {rateLimitSeen.saved}/{rateLimitSeen.total} 쌍 저장됨
                </span>
                <button type="button" className="ro-btn ro-btn--xs" onClick={reopenRateLimitModal}>
                  선택지 다시 열기
                </button>
              </div>
            ) : null}

            {haversineMode && !running ? (
              <div className="ro-calls">
                <span className="ro-warn-text">직선거리 추정으로 진행 중</span>
                <button type="button" className="ro-btn ro-btn--xs" onClick={disableHaversineMode}>
                  도로시간 다시 쓰기
                </button>
              </div>
            ) : null}
          </>
        }
      >
        {idle ? (
          <>
            <p className="ro-hint">
              클러스터 {estimate.clusterCount}개를 도로시간 기준으로 방문 순서와 클러스터별
              진입·이탈, 내부 순서까지 한 번에 정합니다.
            </p>
            <div className="ro-stats">
              <div className="ro-stat">
                <div className="ro-stat__label">예상 호출</div>
                <div className="ro-stat__value">{estimate.totalCalls}</div>
              </div>
              <div className="ro-stat">
                <div className="ro-stat__label">클러스터 간</div>
                <div className="ro-stat__value">{estimate.interClusterCalls}</div>
              </div>
              <div className="ro-stat">
                <div className="ro-stat__label">클러스터 내부</div>
                <div className="ro-stat__value">{estimate.intraClusterCalls}</div>
              </div>
            </div>
            <p className="ro-hint">
              동시 3개 기준 {formatEstimatedTime(estimate.estimatedSeconds)} 걸립니다.
            </p>
            {/* L7: ClusterScreen의 문구와 맞춘다 — 이미 받아 둔 몫은 이 수에서 빠지지 않는다. */}
            <p className="ro-hint ro-hint--small">
              이미 받아 둔 도로시간은 이 값에서 빼지 않았습니다. 재실행이면 실제로는 더 적게
              호출합니다.
            </p>
          </>
        ) : null}

        {stage === 'clusters' && clusterProgress ? (
          <div className="ro-calls">
            <span>
              클러스터 {clusterProgress.index} / {clusterProgress.total}
            </span>
          </div>
        ) : null}

        {running && progress ? (
          <ProgressBar
            label={stage === 'matrix' ? '클러스터 간 도로시간' : '클러스터 내부 도로시간'}
            done={progress.done}
            total={progress.total}
          />
        ) : null}

        {!idle && !running ? (
          <p className="ro-hint">
            {stage === 'matrix'
              ? '클러스터 간 도로시간 단계에서 멈췄습니다.'
              : stage === 'clusters'
                ? `클러스터별 순서 단계 · 확정 ${confirmedCount} / ${clusterOrder.length}`
                : stage === 'stopped'
                  ? // H1: 진행분을 들고 새로 선 화면(clusterOrder가 비어 있으면 클러스터 간
                    // 단계에서, 있으면 클러스터별 단계에서 멈춘 것이다).
                    clusterOrder.length > 0
                    ? `클러스터별 순서 단계 · 확정 ${confirmedCount} / ${clusterOrder.length}`
                    : '클러스터 간 도로시간 단계에서 멈췄습니다.'
                  : ''}
          </p>
        ) : null}

        {/*
          M3: 클러스터 간(대표 사이) 행렬이 직선거리로 메워진 칸이 있으면 바로 여기서 밝힌다.
          지금까지는 이 사실이 결과 화면까지 가야만 드러났다 — 방문 순서 전체가 도로시간이
          아니라 직선거리로 정해졌을 수 있는데, 그 신뢰도를 계산 도중에는 알 길이 없었다.
        */}
        {clusterOrder.length > 0 && clusterHaversineKeys.length > 0 ? (
          <p className="ro-hint ro-warn-text">
            클러스터 간 방문 순서 중 {clusterHaversineKeys.length}칸은 도로시간을 받지 못해
            직선거리로 대체했습니다.
          </p>
        ) : null}

        {/*
          A: 확정될 때마다 쌓이는 목록. `clusterPicks`(스토어)에서 그대로 파생하므로
          중단 후 재진입해도 그대로 복원된다 — 정렬 규칙은 `sortedRows` 주석 참고.
        */}
        {clusterOrder.length > 0 ? (
          <div className="ro-autorows">
            {currentClusterId !== null ? (
              <div
                className="ro-orderitem ro-orderitem--current"
                onMouseEnter={() => setHoverClusterId(currentClusterId)}
                onMouseLeave={() => setHoverClusterId(undefined)}
              >
                <span className="ro-orderitem__no">{currentIndex + 1}</span>
                <span className="ro-orderitem__name" style={{ minWidth: 0 }}>
                  <span style={ROW_NAME_STYLE} title={nameOf(currentClusterId) || undefined}>
                    {nameOf(currentClusterId) || '이름 없는 클러스터'}
                  </span>
                  <span className="ro-orderitem__count">배송지 {countOf(currentClusterId)}건</span>
                </span>
                <span className="ro-autorows__badge">진행 중</span>
              </div>
            ) : null}
            {sortedRows.map((row) => (
              <div
                key={row.clusterId}
                className="ro-orderitem"
                onMouseEnter={() => setHoverClusterId(row.clusterId)}
                onMouseLeave={() => setHoverClusterId(undefined)}
              >
                <span className="ro-orderitem__no">{row.orderIndex}</span>
                <span className="ro-orderitem__name" style={{ minWidth: 0 }}>
                  <span style={ROW_NAME_STYLE} title={row.name || undefined}>
                    {row.name || '이름 없는 클러스터'}
                  </span>
                  <span className="ro-orderitem__count">배송지 {row.count}건</span>
                </span>
                <span className="ro-autorows__stats">
                  도로시간 {row.calls}회
                  {row.fallbacks > 0 ? (
                    <span className="ro-warn-text"> · 직선거리 {row.fallbacks}칸</span>
                  ) : null}
                </span>
              </div>
            ))}
          </div>
        ) : null}
      </SidePanel>

      {rateLimited ? (
        <RateLimitModal
          progress={
            stage === 'matrix'
              ? '클러스터 간 도로시간'
              : `클러스터 ${clusterProgress?.index ?? confirmedCount + 1} / ${clusterOrder.length || clusters.length}`
          }
          savedPairs={rateLimited.saved}
          totalPairs={rateLimited.total}
          onRetry={retryWithKey}
          onFallback={continueWithHaversine}
          onClose={closeRateLimitModal}
        />
      ) : null}
    </div>
  );
}
