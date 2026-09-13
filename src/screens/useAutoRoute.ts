/**
 * useAutoRoute.ts — "완전 자동 경로 모드"의 계산 파이프라인(S4-자동).
 *
 * 순서: 클러스터 대표 고르기(동기) → 클러스터 간 도로시간(비동기) → 방문 순서 결정(동기,
 * `orderClusters`) → 클러스터마다 진입·이탈 제안 + 내부 도로시간 + 내부 순서(비동기 반복)
 * → 최종 순서 조립. 중단·rate-limit 처리와 재개 패턴은 `EntryExitScreen`(S5)을 그대로 따른다.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import {
  RateLimitExceededError,
  RateLimitTracker,
  fetchTimeMatrix,
} from '../api';
import type { TimePair } from '../api';
import {
  haversineFallbackSec,
  haversineKm,
  orderClusters,
  pairsNeeded,
  orderWithinCluster,
  suggestEntryExit,
} from '../core';
import { useAbortable } from '../hooks/useAbortable';
import { ORIGIN_GROUP_ID, kakaoHeaders, useSessionStore } from '../store/session';
import type { SessionStore } from '../store/session';
import { showInfo } from '../store/toast';
import { useVolatileStore } from '../store/volatile';
import type { Cluster, LatLng, PrimaryGroup } from '../types';
import {
  assembleFinalOrder,
  firstUnconfirmedIndex,
  makeTimeSec,
  reusableTimes,
  showApiError,
  withPicks,
} from './helpers';

/** 도로시간 동시 호출 수. 다른 화면(S5)과 같은 값을 쓴다. */
const CONCURRENCY = 3;

/**
 * 호출 1건의 대략적인 소요 시간(초) 추정치. 실측이 아니라 "시작 전에 대충 얼마나
 * 걸리는지" 감을 주기 위한 값이다 — 재시도·backoff가 섞이면 실제로는 더 걸릴 수 있다.
 */
const ASSUMED_CALL_SECONDS = 1;

/** 파이프라인 단계. `matrix` = 클러스터 간 도로시간, `clusters` = 클러스터별 내부 순서. */
export type AutoRouteStage = 'idle' | 'matrix' | 'clusters' | 'done' | 'stopped';

/** 호출 진행률(호출 단위). */
export interface AutoRouteProgress {
  done: number;
  total: number;
}

/** 지금 몇 번째 클러스터를 처리 중인지(1부터). */
export interface AutoRouteClusterProgress {
  index: number;
  total: number;
}

/** rate-limit·부분 수집 배너에 쓰는 요약. */
export interface AutoRouteSavedState {
  saved: number;
  total: number;
}

/** 시작 전에 보여줄 비용 추정. */
export interface AutoRouteEstimate {
  /** 클러스터 수 */
  clusterCount: number;
  /** 클러스터 간(출발지 포함) 호출 수 */
  interClusterCalls: number;
  /** 클러스터 내부 호출 수 합계 */
  intraClusterCalls: number;
  /** 합계 */
  totalCalls: number;
  /** 동시 {@link CONCURRENCY}개 기준 대략의 소요 시간(초) */
  estimatedSeconds: number;
}

/**
 * 시작 전 비용 추정.
 *
 * 클러스터 간 호출은 대표 N개면 `N(N-1)`쌍이고, 출발지가 있으면 `대표 → 출발지는
 * 부르지 않는다`(마지막 대표에서 돌아가지 않으므로) 대신 `출발지 → 대표` N쌍만 더한다.
 * 클러스터 내부 호출은 `pairsNeeded`(블록 대표 쌍)를 그대로 합산한다 — 실제 파이프라인이
 * 부르는 쌍 수와 정확히 같은 계산이다.
 *
 * n=1이어도 출발지가 있으면 실제로 `출발지 → 대표` 1쌍을 부른다(클러스터 간 순서를
 * 정할 필요가 없어도 이 한 쌍만은 부른다) — `ClusterScreen`의 계산과 맞춘다.
 */
export function estimateAutoRoute(
  clusters: readonly Cluster[],
  groups: readonly PrimaryGroup[],
  hasOrigin: boolean,
): AutoRouteEstimate {
  const n = clusters.length;
  const interClusterCalls = n === 0 ? 0 : hasOrigin ? n + n * (n - 1) : n * (n - 1);
  const intraClusterCalls = clusters.reduce(
    (sum, c) => sum + pairsNeeded(c, groups as PrimaryGroup[]).length,
    0,
  );
  const totalCalls = interClusterCalls + intraClusterCalls;
  const estimatedSeconds = Math.ceil(totalCalls / CONCURRENCY) * ASSUMED_CALL_SECONDS;
  return { clusterCount: n, interClusterCalls, intraClusterCalls, totalCalls, estimatedSeconds };
}

/** rate limit·중단 전까지 모아 둔 클러스터 내부 도로시간(재개 때 이미 받은 쌍은 건너뛴다). */
interface Collected {
  times: Record<string, number>;
  fallbacks: number;
  fallbackKeys: string[];
}

/** 두 순서열이 값까지 같은지(M5 — 재조립 결과와 저장된 finalOrder를 비교할 때 쓴다). */
function sameOrderArray(a: readonly number[], b: readonly number[]): boolean {
  return a.length === b.length && a.every((id, i) => id === b[i]);
}

/** 그룹 id → 지도 좌표. */
function latLngOf(groups: readonly PrimaryGroup[], groupId: number | undefined): LatLng | null {
  if (groupId === undefined) return null;
  const group = groups.find((g) => g.id === groupId);
  return group ? { lat: group.lat, lon: group.lon } : null;
}

/**
 * 클러스터 대표 = centroid가 아니라 **실제 그룹 좌표 중 centroid에 가장 가까운 것**.
 *
 * centroid(평균점)는 강·논 한가운데처럼 도로에서 먼 지점에 떨어질 수 있어 카카오가
 * 어느 도로에 스냅할지 예측할 수 없다. 실제 배송지는 반드시 도로에 닿아 있으므로
 * 대표로 쓰면 도로시간 조회가 안정적이다. 부수 효과로 대표가 항상 실제 그룹 id를
 * 가리키게 되어, 행렬 키를 `"fromGroupId-toGroupId"`로 통일해 `makeTimeSec`을 그대로
 * 쓸 수 있다(가상의 centroid 좌표에 별도 id를 만들 필요가 없다).
 *
 * 동률이면 그룹 id가 작은 쪽(결정적) — `suggestEntryExit`/`orderClusters`와 같은 규칙.
 */
function pickRepresentative(cluster: Cluster, groups: readonly PrimaryGroup[]): number {
  const byId = new Map(groups.map((g) => [g.id, g]));
  let best: PrimaryGroup | null = null;
  let bestD = Infinity;
  for (const gid of cluster.groupIds) {
    const g = byId.get(gid);
    if (!g) continue;
    const d = haversineKm(cluster.centroid.lat, cluster.centroid.lon, g.lat, g.lon);
    if (best === null || d < bestD || (d === bestD && g.id < best.id)) {
      best = g;
      bestD = d;
    }
  }
  return best ? best.id : cluster.groupIds[0];
}

/** 한 라운드(클러스터 간 또는 클러스터 내부)의 결과. */
interface RoundResult {
  times: Record<string, number>;
  fallbacks: number;
  fallbackKeys: string[];
}

/**
 * 쌍 목록의 도로시간을 모으거나(직선거리 모드가 아니면), 직선거리 모드면 호출 없이
 * 남은 칸을 추정치로 채운 것처럼 회계만 한다(실제 채움은 `makeTimeSec`이 한다).
 * `RateLimitExceededError`는 그대로 던진다 — 호출부가 잡아서 모달을 띄운다.
 */
async function collectPairs(
  pairs: readonly TimePair[],
  known: Record<string, number>,
  haversineOnly: boolean,
  signal: AbortSignal,
  tracker: RateLimitTracker,
  onProgress: (done: number, total: number) => void,
): Promise<{ result: RoundResult; aborted: boolean }> {
  if (haversineOnly) {
    // D1: 남은 칸도 실제로 추정치를 채워 넣는다(빈 채로 두지 않는다) — 그래야 저장되는
    // timeMatrix가 항상 k(k-1)칸이 되어, 화면의 "칸 수 - 대체 수" 계산이 음수로 찍히지 않는다.
    const missing = pairs.filter((p) => known[p.key] === undefined);
    const estimated = Object.fromEntries(
      missing.map((p) => [p.key, haversineFallbackSec(p.from.lat, p.from.lon, p.to.lat, p.to.lon)]),
    );
    const times = { ...known, ...estimated };
    const fallbackKeys = missing.map((p) => p.key);
    return { result: { times, fallbacks: fallbackKeys.length, fallbackKeys }, aborted: false };
  }
  const res = await fetchTimeMatrix(pairs, kakaoHeaders(), {
    concurrency: CONCURRENCY,
    signal,
    tracker,
    onProgress,
    known,
  });
  return {
    result: { times: res.times, fallbacks: res.fallbacks, fallbackKeys: res.fallbackKeys },
    aborted: res.aborted,
  };
}

/** 재개 시 다시 계산해도 되는 초기 단계 판정. `clusters`가 없으면 화면이 따로 안내한다. */
function computeInitialStage(state: SessionStore): AutoRouteStage {
  if (state.clusters.length === 0) return 'idle';
  const allDone = state.clusters.every((c) => state.clusterPicks[c.id]?.innerOrder !== undefined);
  if (state.clusterOrder.length > 0 && allDone) return 'done';
  const hasProgress =
    state.clusterOrder.length > 0 || Object.keys(state.clusterTimeMatrix).length > 0;
  return hasProgress ? 'stopped' : 'idle';
}

export interface UseAutoRouteResult {
  stage: AutoRouteStage;
  /** 지금 실제로 호출을 기다리는 중인지(진행률 막대를 보일지 판단용). */
  running: boolean;
  /** 현재 라운드(클러스터 간 또는 클러스터 내부 한 클러스터)의 호출 진행률. */
  progress: AutoRouteProgress | null;
  /** `clusters` 단계에서 지금 몇 번째 클러스터인지. */
  clusterProgress: AutoRouteClusterProgress | null;
  /** 복구 불가능한 오류 메시지. */
  error: string | null;
  /** 사용자가 중단했거나 호출이 끊겨 멈춘 이유. */
  stopReason: string | null;
  /** 지금 뜬 rate-limit 모달의 상태. */
  rateLimited: AutoRouteSavedState | null;
  /** 모달을 닫아도 남는 배너용 상태. */
  rateLimitSeen: AutoRouteSavedState | null;
  /** 직선거리 추정으로 계속 진행 중인지. */
  haversineMode: boolean;
  /** 시작 전 비용 추정. */
  estimate: AutoRouteEstimate;
  /** 시작(또는 이어서 계산). */
  start: () => void;
  /** 지금 진행 중인 호출을 중단한다. */
  stop: () => void;
  /** 새 REST 키로 재시도. */
  retryWithKey: (newKey: string) => void;
  /** 남은 칸을 직선거리 추정으로 채우고 계속(이후 라운드도 같은 선택을 따른다). */
  continueWithHaversine: () => void;
  /** 직선거리 모드를 끄고 도로시간 호출로 되돌아간다. */
  disableHaversineMode: () => void;
  /** rate-limit 모달만 닫는다(배너는 남는다). */
  closeRateLimitModal: () => void;
  /** 닫아 둔 rate-limit 모달을 배너에서 다시 연다. */
  reopenRateLimitModal: () => void;
  /** 이미 다 끝난 상태(step 6에서 되돌아온 경우)에서 결과 화면으로 다시 넘어간다. */
  viewResult: () => void;
}

/** 완전 자동 경로 모드의 계산 파이프라인 훅. */
export function useAutoRoute(): UseAutoRouteResult {
  const origin = useSessionStore((s) => s.origin);
  const groups = useSessionStore((s) => s.groups);
  const clusters = useSessionStore((s) => s.clusters);
  const setClusterOrder = useSessionStore((s) => s.setClusterOrder);
  const setClusterEntryExit = useSessionStore((s) => s.setClusterEntryExit);
  const setClusterInnerOrder = useSessionStore((s) => s.setClusterInnerOrder);
  const setFinalOrder = useSessionStore((s) => s.setFinalOrder);
  const setStep = useSessionStore((s) => s.setStep);
  const mergeClusterTimes = useSessionStore((s) => s.mergeClusterTimes);
  const setRestKey = useVolatileStore((s) => s.setRestKey);

  const { start: startAbortable, abort } = useAbortable();

  const [stage, setStage] = useState<AutoRouteStage>(() =>
    computeInitialStage(useSessionStore.getState()),
  );
  const [progress, setProgress] = useState<AutoRouteProgress | null>(null);
  const [clusterProgress, setClusterProgress] = useState<AutoRouteClusterProgress | null>(null);
  const [error, setError] = useState<string | null>(null);
  // H1: `computeInitialStage`가 'stopped'를 돌려준 마운트(=진행분을 들고 새로 선 화면)는
  // stopReason도 같이 채워 둬야 footer의 idle/paused 판정이 셋 다 false로 떨어지지 않는다.
  // 그러지 않으면 "자동 계산 시작"도 "이어서 계산"도 없는 빈 패널에 갇힌다(검증 보고 H1).
  const [stopReason, setStopReason] = useState<string | null>(() =>
    stage === 'stopped' ? '지난 계산이 도중에 멈췄습니다. 이어서 계산할 수 있습니다.' : null,
  );
  const [rateLimited, setRateLimited] = useState<AutoRouteSavedState | null>(null);
  // 모달을 닫아도 한도에 걸렸다는 사실은 남아야 다시 열 길이 생긴다(S5와 같은 이유).
  const [rateLimitSeen, setRateLimitSeen] = useState<AutoRouteSavedState | null>(null);
  const [haversineMode, setHaversineModeState] = useState(false);
  // run()은 재생성되지 않는 콜백이라 최신 모드를 ref로도 들고 있어야 한다.
  const haversineModeRef = useRef(false);
  const setHaversineMode = useCallback((v: boolean) => {
    haversineModeRef.current = v;
    setHaversineModeState(v);
  }, []);

  // 여러 클러스터·여러 라운드에 걸쳐 rate-limit 카운터를 이어 세야 한다(S5와 같은 이유).
  const [tracker] = useState(() => new RateLimitTracker());
  // 클러스터별로 rate-limit·중단 전까지 모은 내부 도로시간. 화면을 떠나면 사라진다(S5와 같음).
  const collectedRef = useRef<Record<number, Collected>>({});
  // D4: run() 세대 토큰. 중단 직후 "이어서 계산"을 누르면 새 run이 시작되는데, 옛 run의
  // await가 그 뒤에 풀리며 옛 run만의 뒷정리(setStage('stopped') 등)로 새 run의 상태를
  // 덮어쓸 수 있다. 자신이 시작될 때 받은 세대와 지금 세대가 다르면 조용히 물러난다.
  const runIdRef = useRef(0);

  // M4: 부분 수집분은 ref라 이 화면을 떠나면 조용히 사라진다(S5와 같은 한계). S5는 떠날 때
  // 알리므로 여기도 같은 경고를 낸다 — 이미 쿼터를 쓴 값이라 잃는다는 사실은 말해야 한다.
  useEffect(
    () => () => {
      const count = Object.keys(collectedRef.current).length;
      if (count > 0) {
        showInfo(`부분 수집해 둔 클러스터 ${count}개의 도로시간은 화면을 떠나 사라졌습니다.`);
      }
    },
    [],
  );

  const finalize = useCallback(() => {
    const s = useSessionStore.getState();
    const finalOrder = assembleFinalOrder(
      withPicks(s.clusters, s.clusterPicks),
      s.clusterOrder,
      s.groups,
      s.nodes,
    );
    setFinalOrder(finalOrder);
    setStage('done');
    setStep(6);
  }, [setFinalOrder, setStep]);

  /**
   * M5: "결과 보기" 버튼(= 이미 다 확정된 채로 이 화면에 서 있을 때만 뜬다, `doneAlready`
   * 참고)은 새로 계산하지 않고 `finalOrder`를 다시 조립한다. 그런데 `ResultScreen`에서
   * 드래그로 고친 순서가 `finalOrder`에 저장돼 있을 수 있다 — 그걸 확인 없이 재조립 값으로
   * 덮으면 손으로 고친 순서가 말없이 사라진다. `finalize`(실제 계산 직후 호출)는 그대로
   * 두고, 이 버튼 전용 경로에서만 달라졌을 때 확인을 받는다(`ResultScreen`의
   * `revertToComputed`와 같은 톤).
   */
  const viewResult = useCallback(() => {
    const s = useSessionStore.getState();
    const computed = assembleFinalOrder(
      withPicks(s.clusters, s.clusterPicks),
      s.clusterOrder,
      s.groups,
      s.nodes,
    );
    if (s.finalOrder.length > 0 && !sameOrderArray(s.finalOrder, computed)) {
      const ok = window.confirm(
        '결과 화면에서 손으로 고친 순서가 있습니다. 다시 계산하면 그 순서가 사라집니다. 계속할까요?',
      );
      if (!ok) {
        setStep(6);
        return;
      }
    }
    setFinalOrder(computed);
    setStep(6);
  }, [setFinalOrder, setStep]);

  const run = useCallback(async () => {
    const myRun = ++runIdRef.current;
    setError(null);
    setStopReason(null);
    const signal = startAbortable();

    const s0 = useSessionStore.getState();
    const { origin: originNow, groups: groupsNow, clusters: clustersNow, nodes: nodesNow } = s0;
    if (clustersNow.length === 0) {
      setError('클러스터가 없습니다. 클러스터링으로 돌아가 먼저 만들어 주세요.');
      return;
    }

    const groupById = new Map(groupsNow.map((g) => [g.id, g]));

    // 1) 클러스터 대표(실제 그룹 좌표). 매 호출 재계산하지만 순수 함수라 비용이 작다.
    const repOfCluster = new Map<number, number>();
    for (const cluster of clustersNow) repOfCluster.set(cluster.id, pickRepresentative(cluster, groupsNow));
    const clusterIdByRep = new Map<number, number>();
    for (const [cid, gid] of repOfCluster) clusterIdByRep.set(gid, cid);
    const reps = clustersNow.map((c) => repOfCluster.get(c.id) as number);

    let order = s0.clusterOrder;

    if (order.length === 0) {
      setStage('matrix');

      const pairs: TimePair[] = [];
      if (originNow) {
        for (const gid of reps) {
          const g = groupById.get(gid);
          if (!g) continue;
          pairs.push({
            key: `${ORIGIN_GROUP_ID}-${gid}`,
            from: { lat: originNow.lat, lon: originNow.lon },
            to: { lat: g.lat, lon: g.lon },
          });
        }
      }
      for (const from of reps) {
        for (const to of reps) {
          if (from === to) continue;
          const a = groupById.get(from);
          const b = groupById.get(to);
          if (!a || !b) continue;
          pairs.push({ key: `${from}-${to}`, from: { lat: a.lat, lon: a.lon }, to: { lat: b.lat, lon: b.lon } });
        }
      }
      // 대표 → 출발지 방향은 부르지 않는다. `orderClusters`가 찾는 것은 열린 경로라
      // 마지막 대표에서 출발지로 돌아가는 간선을 쓰지 않는다 — 과거 데스크톱판은 이
      // 쌍까지 사서 낭비했다.

      if (pairs.length > 0) {
        const s1 = useSessionStore.getState();
        const haversineSet = new Set(s1.clusterHaversineKeys);
        // 직선거리로 메운 칸은 물려받지 않는다 — 실제 도로시간을 받을 기회를 지켜야 한다.
        const known = Object.fromEntries(
          Object.entries(s1.clusterTimeMatrix).filter(([k]) => !haversineSet.has(k)),
        );
        try {
          // M1: 첫 응답이 오기 전에도 "중단"이 있어야 한다. `running`은 `progress !== null`
          // 로만 판단하므로, 호출을 시작하기 **전에** 0/전체로 한 번 세워 둔다(S5와 같은 처리).
          const pending = pairs.filter((p) => known[p.key] === undefined).length;
          setProgress({ done: 0, total: pending });
          const { result, aborted } = await collectPairs(
            pairs,
            known,
            haversineModeRef.current,
            signal,
            tracker,
            (done, total) => setProgress({ done, total }),
          );
          // 끝나든 중단되든 rate-limit이든 받은 것은 반드시 저장한다 — 비싸게 산 값이다.
          mergeClusterTimes(result.times, result.fallbackKeys);
          setProgress(null);
          if (!haversineModeRef.current) setRateLimitSeen(null);
          if (aborted) {
            // D4: 새 run이 이미 시작됐으면 이 옛 run의 뒷정리로 그 상태를 덮지 않는다.
            if (myRun !== runIdRef.current) return;
            setStage('stopped');
            setStopReason('클러스터 간 도로시간 호출을 중단했습니다. 받은 값은 저장되었습니다.');
            return;
          }
        } catch (err) {
          setProgress(null);
          if (err instanceof RateLimitExceededError) {
            mergeClusterTimes(err.partial.times, err.partial.fallbackKeys);
            const saved = Object.keys(err.partial.times).length;
            const state = { saved, total: pairs.length };
            setRateLimited(state);
            setRateLimitSeen(state);
            // H2: rate-limit도 "이어서 계산"이 나와야 한다 — 배너의 "선택지 다시 열기"뿐이면
            // "잠깐 기다렸다가 같은 키로 다시"가 불가능하다(검증 보고 H2). stopReason을 세워
            // footer의 paused 판정을 켠다. 재시도는 `known` 덕에 이미 받은 쌍을 다시 사지 않는다.
            setStopReason('API 한도로 멈췄습니다. 받은 값은 저장되었습니다.');
            return;
          }
          showApiError(err, '클러스터 간 도로시간을 불러오지 못했습니다.');
          setError('클러스터 간 도로시간을 불러오지 못했습니다.');
          return;
        }
      }

      // 3) 순서 결정. makeTimeSec이 빠진 칸을 직선거리로 메우므로 일부만 받았어도 순서는 나온다.
      // origin은 `groups`(1차 그룹)에 없으므로, 여기서만 쓰는 가상 그룹으로 얹어
      // makeTimeSec의 자동 추정이 출발지 관련 칸도 채우게 한다(그룹 id 통일 트릭).
      const s2 = useSessionStore.getState();
      const groupsForTimeSec: PrimaryGroup[] = originNow
        ? [
            ...groupsNow,
            { id: ORIGIN_GROUP_ID, rep: -1, members: [], complexKey: '', lat: originNow.lat, lon: originNow.lon },
          ]
        : groupsNow;
      const timeSec = makeTimeSec(s2.clusterTimeMatrix, groupsForTimeSec);
      const repOrder = orderClusters(reps, timeSec, originNow ? ORIGIN_GROUP_ID : undefined);
      order = repOrder.map((gid) => clusterIdByRep.get(gid) as number);
      setClusterOrder(order);
    }

    // 4) 클러스터별 진입·이탈 + 내부 순서.
    setStage('clusters');
    const clusterById = new Map(clustersNow.map((c) => [c.id, c]));
    const startAtIdx = firstUnconfirmedIndex(order, useSessionStore.getState().clusterPicks);
    // 전부 확정했으면(-1) 루프를 끝내라는 뜻으로 order.length를 쓴다 — EntryExitScreen이
    // 같은 -1을 "첫 클러스터로 돌아가라"로 읽는 것과는 반대 해석이다(재진입 vs 완료).
    const startAt = startAtIdx < 0 ? order.length : startAtIdx;

    for (let i = startAt; i < order.length; i += 1) {
      if (signal.aborted) {
        setStage('stopped');
        setStopReason('중단했습니다. 여기까지는 저장되었습니다.');
        setClusterProgress(null);
        return;
      }
      const clusterId = order[i];
      setClusterProgress({ index: i + 1, total: order.length });
      const cluster = clusterById.get(clusterId);
      if (!cluster) continue;

      const prevClusterId = i > 0 ? order[i - 1] : undefined;
      const nextClusterId = i < order.length - 1 ? order[i + 1] : undefined;
      const prevExitGroupId =
        prevClusterId === undefined ? undefined : useSessionStore.getState().clusterPicks[prevClusterId]?.exit;
      const prevPoint: LatLng | null =
        prevExitGroupId !== undefined
          ? latLngOf(groupsNow, prevExitGroupId)
          : prevClusterId === undefined && originNow
            ? { lat: originNow.lat, lon: originNow.lon }
            : null;
      const nextRepGid = nextClusterId === undefined ? undefined : repOfCluster.get(nextClusterId);
      const nextPoint = latLngOf(groupsNow, nextRepGid);

      const suggestion = suggestEntryExit(cluster, groupsNow, prevPoint, nextPoint);
      if (!suggestion) continue; // 빈 클러스터(있을 수 없지만 방어적으로)
      setClusterEntryExit(clusterId, suggestion.entry, suggestion.exit);

      const pairs = pairsNeeded(cluster, groupsNow);
      const timePairs: TimePair[] = pairs.flatMap(([from, to]) => {
        const a = groupById.get(from);
        const b = groupById.get(to);
        if (!a || !b) return [];
        return [{ key: `${from}-${to}`, from: { lat: a.lat, lon: a.lon }, to: { lat: b.lat, lon: b.lon } }];
      });

      const existingPick = useSessionStore.getState().clusterPicks[clusterId];
      const confirmedKnown = reusableTimes(existingPick);
      const collected = collectedRef.current[clusterId] ?? { times: {}, fallbacks: 0, fallbackKeys: [] };
      // D3: 재개할 때 부분 수집분 중 추정으로 메운 칸은 다시 산다 — 클러스터 간 경로
      // (위 401-405)와 같은 판단이다. `collected.times`에서 그 칸을 빼면 다음
      // collectPairs 호출이 known으로 보지 않고 실제로 다시 부른다.
      const collectedKnown = reusableTimes({
        timeMatrix: collected.times,
        haversineKeys: collected.fallbackKeys,
      });
      const known = { ...confirmedKnown, ...collectedKnown };

      let times: Record<string, number> = known;
      let fallbacks = 0;
      let fallbackKeys: string[] = [];

      if (timePairs.length > 0) {
        try {
          // M1과 같은 이유 — 첫 응답 전에도 중단할 수 있어야 한다.
          const pending = timePairs.filter((p) => known[p.key] === undefined).length;
          setProgress({ done: 0, total: pending });
          const { result, aborted } = await collectPairs(
            timePairs,
            known,
            haversineModeRef.current,
            signal,
            tracker,
            (done, total) => setProgress({ done, total }),
          );
          times = result.times;
          fallbacks = result.fallbacks;
          fallbackKeys = [...result.fallbackKeys];
          setProgress(null);
          if (!haversineModeRef.current) setRateLimitSeen(null);
          if (aborted) {
            // 중단은 확정이 아니다 — 받은 만큼만 다음 재개를 위해 남겨 둔다.
            collectedRef.current[clusterId] = { times, fallbacks, fallbackKeys };
            // D4: 중단 직후 새 run이 이미 시작됐으면(사용자가 "이어서 계산"을 바로 눌렀다)
            // 이 옛 run의 뒷정리로 새 run의 상태를 덮지 않는다.
            if (myRun !== runIdRef.current) return;
            setStage('stopped');
            setStopReason('도로시간 호출을 중단했습니다. 이 클러스터는 아직 확정되지 않았습니다.');
            setClusterProgress(null);
            return;
          }
        } catch (err) {
          setProgress(null);
          if (err instanceof RateLimitExceededError) {
            const merged = { ...known, ...err.partial.times };
            collectedRef.current[clusterId] = {
              times: merged,
              fallbacks: err.partial.fallbacks,
              fallbackKeys: [...err.partial.fallbackKeys],
            };
            const saved = Object.keys(merged).length;
            const state = { saved, total: timePairs.length };
            setRateLimited(state);
            setRateLimitSeen(state);
            // H2와 같은 이유 — rate-limit 후에도 "이어서 계산"이 있어야 한다.
            setStopReason('API 한도로 멈췄습니다. 받은 값은 저장되었습니다.');
            setClusterProgress(null);
            return;
          }
          showApiError(err, '클러스터 내부 도로시간을 불러오지 못했습니다.');
          setError('클러스터 내부 도로시간을 불러오지 못했습니다.');
          setClusterProgress(null);
          return;
        }
      }

      const { innerOrder } = orderWithinCluster(
        cluster,
        groupsNow,
        nodesNow,
        suggestion.entry,
        suggestion.exit,
        makeTimeSec(times, groupsNow),
      );
      setClusterInnerOrder(clusterId, innerOrder, times, fallbacks, fallbackKeys);
      delete collectedRef.current[clusterId];
    }

    setClusterProgress(null);
    finalize();
  }, [
    startAbortable,
    tracker,
    mergeClusterTimes,
    setClusterOrder,
    setClusterEntryExit,
    setClusterInnerOrder,
    finalize,
  ]);

  const start = useCallback(() => {
    void run();
  }, [run]);

  const stop = useCallback(() => {
    abort();
  }, [abort]);

  const retryWithKey = useCallback(
    (newKey: string) => {
      setRestKey(newKey);
      // 키를 바꿨으니 연속·누적 카운터도 새로 센다.
      tracker.reset();
      setHaversineMode(false);
      setRateLimited(null);
      setRateLimitSeen(null);
      void run();
    },
    [run, setRestKey, tracker, setHaversineMode],
  );

  const continueWithHaversine = useCallback(() => {
    // 이 실행은 끝까지 직선거리로 진행한다 — 남은 클러스터마다 같은 모달을 반복시키지 않는다.
    setHaversineMode(true);
    setRateLimited(null);
    setRateLimitSeen(null);
    void run();
  }, [run, setHaversineMode]);

  const disableHaversineMode = useCallback(() => {
    setHaversineMode(false);
    tracker.reset();
  }, [setHaversineMode, tracker]);

  const closeRateLimitModal = useCallback(() => setRateLimited(null), []);
  const reopenRateLimitModal = useCallback(() => {
    setRateLimited((current) => current ?? rateLimitSeen);
  }, [rateLimitSeen]);

  // L6: 다른 파생값은 전부 memo되어 있는데 이것만 매 렌더 클러스터 수만큼 다시 돈다.
  const estimate = useMemo(
    () => estimateAutoRoute(clusters, groups, origin !== null),
    [clusters, groups, origin],
  );
  const running = progress !== null;

  return {
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
  };
}
