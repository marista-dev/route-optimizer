import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { ArrowRight } from 'lucide-react';

import { RateLimitExceededError, RateLimitTracker, fetchTimeMatrix } from '../api';
import type { TimePair } from '../api';
import { haversineFallbackSec, orderWithinCluster, pairsNeeded, suggestEntryExit } from '../core';
import { MapFit, ProgressTrack, RateLimitModal, SidePanel } from '../components';
import { ClusterLayer, MAP_COLOR, MapCanvas, MarkerLayer, RefPointLayer, RouteLayer } from '../map';
import type { RefPoint } from '../map';
import { useAbortable } from '../hooks/useAbortable';
import { kakaoHeaders, useSessionStore } from '../store/session';
import type { ClusterPick } from '../store/session';
import { showInfo } from '../store/toast';
import { useVolatileStore } from '../store/volatile';
import type { Cluster, LatLng, PrimaryGroup } from '../types';
import {
  assembleFinalOrder,
  firstUnconfirmedIndex,
  groupBuildingLabel,
  groupPath,
  makeOrderOf,
  makeTimeSec,
  reusableTimes,
  showApiError,
  withPicks,
} from './helpers';

/** 도로시간 동시 호출 수. */
const CONCURRENCY = 3;

/** 한 클러스터의 진입·이탈 선택 상태. */
interface Selection {
  entry?: number;
  exit?: number;
}

/** rate limit·중단 전까지 모아 둔 도로시간(재시도 때 이미 받은 쌍은 건너뛴다). */
interface Collected {
  times: Record<string, number>;
  fallbacks: number;
  /** 그중 Haversine 추정으로 메운 칸의 키 */
  fallbackKeys: string[];
}

/** 그룹 id → 지도 좌표. */
function latLngOf(groups: PrimaryGroup[], groupId: number | undefined): LatLng | null {
  if (groupId === undefined) return null;
  const group = groups.find((g) => g.id === groupId);
  return group ? { lat: group.lat, lon: group.lon } : null;
}

/**
 * S5 진입 · 이탈 지점.
 * 핸드오프의 기본 변형인 "A: 우측 카드"를 구현했다
 * (B: 하단 시트, C: 마커 팝오버는 미구현).
 */
export function EntryExitScreen() {
  const origin = useSessionStore((s) => s.origin);
  const nodes = useSessionStore((s) => s.nodes);
  const groups = useSessionStore((s) => s.groups);
  const clusters = useSessionStore((s) => s.clusters);
  const clusterOrder = useSessionStore((s) => s.clusterOrder);
  const clusterPicks = useSessionStore((s) => s.clusterPicks);
  const setClusterEntryExit = useSessionStore((s) => s.setClusterEntryExit);
  const setClusterInnerOrder = useSessionStore((s) => s.setClusterInnerOrder);
  const clearClusterPick = useSessionStore((s) => s.clearClusterPick);
  const setFinalOrder = useSessionStore((s) => s.setFinalOrder);
  const setStep = useSessionStore((s) => s.setStep);
  const setRestKey = useVolatileStore((s) => s.setRestKey);

  const { start, abort } = useAbortable();
  // 새로고침·재진입 시 이어서 할 수 있도록 첫 미확정 클러스터에서 시작한다.
  const [index, setIndex] = useState(() => {
    const state = useSessionStore.getState();
    const i = firstUnconfirmedIndex(state.clusterOrder, state.clusterPicks);
    // 전부 확정했으면(-1) 첫 클러스터로 — 재진입 시 처음부터 다시 훑어볼 수 있게 한다.
    return i < 0 ? 0 : i;
  });
  const [selections, setSelections] = useState<Record<number, Selection>>({});
  const [progress, setProgress] = useState<{ done: number; total: number } | null>(null);
  const [rateLimited, setRateLimited] = useState<{ saved: number; total: number } | null>(null);
  // 모달을 닫아도 남는 흔적. 닫고 나면 한도에 걸렸다는 사실이 화면에서 통째로 사라지고,
  // 선택지를 다시 띄우는 길이 "또 한도에 걸리기"(쌍당 최대 90초)뿐이었다.
  const [rateLimitSeen, setRateLimitSeen] = useState<{ saved: number; total: number } | null>(
    null,
  );
  // 연속·누적 rate limit 카운터는 S5 세션 전체를 관통해야 한다(클러스터마다 리셋되면 안 된다).
  const [tracker] = useState(() => new RateLimitTracker());
  // "Haversine으로 계속"을 한 번 고르면 남은 클러스터도 같은 선택을 따른다.
  // (tracker의 누적 카운터는 그대로라 끄지 않으면 클러스터마다 같은 모달이 다시 뜬다.)
  const [haversineMode, setHaversineMode] = useState(false);
  // 클러스터 id → 이미 받아 둔 쌍. 재시도할 때 빠진 쌍만 다시 부른다.
  const collectedRef = useRef<Record<number, Collected>>({});
  /**
   * 부분 수집 현황(클러스터 id → 받아 둔 쌍 / 필요한 쌍).
   * `collectedRef`는 ref라 화면에 아무 흔적도 남기지 않는다 — 이미 쿼터를 쓴 데이터이므로
   * 최소한 얼마나 갖고 있는지는 보여야 한다.
   */
  const [partial, setPartial] = useState<Record<number, { saved: number; total: number }>>({});
  /**
   * "이탈만 바꾸기"를 켜 둔 클러스터 id. 기본 규칙(1번째 클릭 = 진입)을 건너뛴다.
   * 상태를 클러스터에 묶어 두면 다른 칸으로 옮길 때 저절로 풀린다(따로 지울 effect가 없다).
   */
  const [pickExitFor, setPickExitFor] = useState<number | null>(null);

  const clusterById = useMemo(() => new Map(clusters.map((c) => [c.id, c])), [clusters]);
  const groupById = useMemo(() => new Map(groups.map((g) => [g.id, g])), [groups]);
  const nodeById = useMemo(() => new Map(nodes.map((n) => [n.id, n])), [nodes]);

  /**
   * 그룹 표시 이름 — 수령인 이름이 아니라 건물(단지) 이름이다.
   * 진입·이탈은 "어느 건물로 들어가고 나오나"를 고르는 일이라 사람 이름은 쓸모가 없다.
   */
  const labelOf = useCallback(
    (groupId: number): string => {
      const group = groupById.get(groupId);
      if (!group) return '—';
      const label = groupBuildingLabel(
        group.members.map((id) => nodeById.get(id)?.address ?? ''),
      );
      const count = group.members.length > 1 ? ` · ${group.members.length}건` : '';
      return `${label || `그룹 ${groupId}`}${count}`;
    },
    [groupById, nodeById],
  );


  const currentId = clusterOrder[index];
  const current: Cluster | undefined = clusterById.get(currentId);
  const pick: ClusterPick | undefined = clusterPicks[currentId];
  const stored: Selection = { entry: pick?.entry, exit: pick?.exit };
  const single = (current?.groupIds.length ?? 0) === 1;
  // 마커가 하나뿐인 클러스터는 고를 여지가 없으므로 렌더 중에 바로 확정한다.
  const selection: Selection = single
    ? { entry: current?.groupIds[0], exit: current?.groupIds[0] }
    : (selections[currentId] ?? stored);
  const ready = selection.entry !== undefined && selection.exit !== undefined;
  const running = progress !== null;
  const pickingExit = pickExitFor === currentId;

  const confirmedCount = clusterOrder.filter(
    (id) => clusterPicks[id]?.innerOrder !== undefined,
  ).length;
  const nextUnconfirmedIdx = firstUnconfirmedIndex(clusterOrder, clusterPicks);
  // 전부 확정했으면(-1) 첫 클러스터를 가리킨다 — 위 초기 index와 같은 해석.
  const nextUnconfirmed = nextUnconfirmedIdx < 0 ? 0 : nextUnconfirmedIdx;

  // 단일 건물 클러스터는 고를 여지도, 부를 도로시간도 없다(쌍이 0개).
  // 사용자가 매번 "확정"을 누르게 할 이유가 없어 자동으로 확정하고 다음으로 넘긴다.
  // confirm은 매 렌더 새로 만들어지므로 최신 것을 ref에 담아 effect에서 부른다.
  const confirmRef = useRef<(haversineOnly?: boolean) => Promise<void>>(async () => {});
  const autoDoneRef = useRef<Set<number>>(new Set());
  /** 이번 연쇄에서 자동 확정하며 지나친 방문 순번들(0부터). 연쇄가 멈추면 한 번에 알린다. */
  const autoRunRef = useRef<number[]>([]);

  // 자동으로 확정된 수 — 단일 건물 클러스터는 자동 확정이 유일한 경로라 세어 보면 된다.
  const autoCount = useMemo(
    () =>
      clusterOrder.filter((id) => {
        const c = clusterById.get(id);
        return c?.groupIds.length === 1 && clusterPicks[id]?.innerOrder !== undefined;
      }).length,
    [clusterOrder, clusterById, clusterPicks],
  );

  useEffect(() => {
    // 순서가 바뀌면 확정도 전부 지워지므로 자동 확정 기록도 함께 비운다.
    autoDoneRef.current = new Set();
  }, [clusterOrder]);

  // 부분 수집 결과는 ref·로컬 상태라 이 화면을 떠나면 사라진다.
  // 이미 쿼터를 쓴 데이터이므로 조용히 잃지 않게 떠날 때 한 번 알린다.
  const partialRef = useRef<Record<number, { saved: number; total: number }>>({});
  useEffect(() => {
    partialRef.current = partial;
  }, [partial]);
  useEffect(
    () => () => {
      const count = Object.keys(partialRef.current).length;
      if (count > 0) {
        showInfo(`부분 수집해 둔 클러스터 ${count}개의 도로시간은 화면을 떠나 사라졌습니다.`);
      }
    },
    [],
  );
  // D12: 언마운트 뒤(= 신호가 자연히 끊긴 것) confirm()이 계속 실행되며 다른 화면에 서
  // 있는 사용자에게 "중단했습니다" 토스트를 띄우는 것을 막는다.
  const mountedRef = useRef(true);
  useEffect(() => () => {
    mountedRef.current = false;
  }, []);

  const allConfirmed = clusterOrder.length > 0 && confirmedCount === clusterOrder.length;

  const setSelection = useCallback(
    (patch: Selection) => {
      setSelections((prev) => ({ ...prev, [currentId]: { ...(prev[currentId] ?? {}), ...patch } }));
    },
    [currentId],
  );

  const onGroupClick = useCallback(
    (groupId: number) => {
      // 호출 중에 선택을 바꾸면 저장되는 값(시작 당시 선택)과 화면이 어긋난 채 남는다.
      if (running) return;
      if (single) {
        setSelection({ entry: groupId, exit: groupId });
        return;
      }
      // 이탈만 바꾸는 모드 — 진입을 다시 찍게 하지 않는다.
      if (pickingExit) {
        setSelection({ exit: groupId });
        setPickExitFor(null);
        return;
      }
      // 1회 클릭 = 진입, 2회째 = 이탈. 둘 다 정해진 뒤 클릭하면 진입부터 다시 고른다.
      if (selection.entry === undefined || selection.exit !== undefined) {
        setSelection({ entry: groupId, exit: undefined });
      } else {
        setSelection({ exit: groupId });
      }
    },
    [running, single, pickingExit, selection.entry, selection.exit, setSelection],
  );

  // ── 이웃 클러스터(이전 위치 → 현재 → 다음 중심) ───────────────────────────
  // 진입·이탈은 앞뒤 클러스터와의 연결을 보고 고르는 값이라 둘 다 보여야 한다.
  const prevClusterId = index > 0 ? clusterOrder[index - 1] : undefined;
  const nextClusterId = index < clusterOrder.length - 1 ? clusterOrder[index + 1] : undefined;
  const prevExitGroupId =
    prevClusterId === undefined ? undefined : clusterPicks[prevClusterId]?.exit;
  // 매 렌더 새 객체를 만들면 아래 refPoints memo가 매번 무효가 된다 → memo로 고정.
  const prevExitPoint = useMemo<LatLng | null>(
    () =>
      latLngOf(groups, prevExitGroupId) ??
      (origin && prevClusterId === undefined ? { lat: origin.lat, lon: origin.lon } : null),
    [groups, prevExitGroupId, origin, prevClusterId],
  );
  const nextCluster = nextClusterId === undefined ? undefined : clusterById.get(nextClusterId);
  const nextEntryGroupId =
    nextClusterId === undefined ? undefined : clusterPicks[nextClusterId]?.entry;
  /*
   * 다음 클러스터의 기준점. 진입 지점을 이미 골라 뒀으면 중심이 아니라 그 지점을 쓴다 —
   * 실제로 차가 향하는 곳이 거기라, 이탈 지점을 그쪽에 가깝게 고르는 판단이 맞아진다.
   */
  const nextPoint = useMemo<LatLng | null>(
    () => latLngOf(groups, nextEntryGroupId) ?? nextCluster?.centroid ?? null,
    [groups, nextEntryGroupId, nextCluster],
  );

  const applySuggestion = () => {
    if (!current) return;
    const suggestion = suggestEntryExit(
      current,
      groups,
      prevExitPoint,
      nextPoint,
    );
    if (suggestion) setSelection(suggestion);
  };

  const confirm = async (haversineOnly = haversineMode) => {
    const cluster = current;
    if (!cluster || selection.entry === undefined || selection.exit === undefined) return;

    const pairs = pairsNeeded(cluster, groups);
    const timePairs: TimePair[] = pairs.flatMap(([from, to]) => {
      const a = groupById.get(from);
      const b = groupById.get(to);
      if (!a || !b) return [];
      return [{ key: `${from}-${to}`, from: { lat: a.lat, lon: a.lon }, to: { lat: b.lat, lon: b.lon } }];
    });

    const collected = collectedRef.current[cluster.id] ?? { times: {}, fallbacks: 0, fallbackKeys: [] };
    // D3: 부분 수집분 중 추정으로 메운 칸은 다시 사야 한다 — 새 REST 키로 재시도하는
    // 이유가 바로 그 칸을 실제 도로시간으로 받으려는 것이다. known(그리고 아래 대체
    // 집계)에서 빼면 다음 호출이 그 칸만 다시 부른다. 클러스터 간 경로(useAutoRoute의
    // clusterTimeMatrix 재사용부)와 같은 판단이다.
    const collectedKnown = reusableTimes({
      timeMatrix: collected.times,
      haversineKeys: collected.fallbackKeys,
    });
    // 확정해 둔 행렬을 먼저 깔고 부분 수집분을 덮는다 → 진입·이탈만 바꾼 재계산은 호출 0회.
    let times: Record<string, number> = { ...reusableTimes(pick), ...collectedKnown };
    // D1: 직선거리 모드는 호출을 건너뛰되, 남은 칸도 실제로 추정치를 채워 timeMatrix에
    // 넣는다. 그러지 않으면 timeMatrix 칸 수 < haversineFallbacks가 되어 화면의
    // "칸 수 - 대체 수" 계산이 음수로 찍힌다.
    let fallbacks = 0;
    let fallbackKeys: string[] = [];
    if (haversineOnly) {
      const missing = timePairs.filter((pair) => times[pair.key] === undefined);
      const estimated = Object.fromEntries(
        missing.map((pair) => [
          pair.key,
          haversineFallbackSec(pair.from.lat, pair.from.lon, pair.to.lat, pair.to.lon),
        ]),
      );
      times = { ...times, ...estimated };
      fallbacks = missing.length;
      fallbackKeys = missing.map((pair) => pair.key);
    }

    try {
      if (!haversineOnly && timePairs.length > 0) {
        const signal = start();
        // fetchTimeMatrix의 onProgress는 이미 받아 둔 쌍(known)을 뺀 수를 기준으로 센다.
        // 분모를 timePairs.length로 잡으면 재시도 때 총량이 중간에 줄어든다.
        const pending = timePairs.filter((pair) => times[pair.key] === undefined).length;
        setProgress({ done: 0, total: pending });
        const result = await fetchTimeMatrix(timePairs, kakaoHeaders(), {
          concurrency: CONCURRENCY,
          signal,
          onProgress: (done, total) => setProgress({ done, total }),
          tracker,
          known: times,
        });
        times = { ...times, ...result.times };
        fallbacks = result.fallbacks;
        fallbackKeys = [...result.fallbackKeys];
        // 중단은 "확정"이 아니다. 상태를 하나도 쓰지 않고 그대로 머문다.
        if (result.aborted) {
          collectedRef.current[cluster.id] = { times, fallbacks, fallbackKeys };
          const nextPartial = {
            ...partialRef.current,
            [cluster.id]: { saved: Object.keys(times).length, total: timePairs.length },
          };
          // D12: 언마운트 정리 effect(아래)가 참조하는 ref를 setPartial과 함께 즉시
          // 갱신한다 — setPartial만 믿으면 이 직후 언마운트될 때 그 effect가 아직
          // 옛 값을 들고 있어 부분 수집 건수를 하나 적게 알린다.
          partialRef.current = nextPartial;
          setPartial(nextPartial);
          // D12: 신호가 끊긴 이유가 언마운트(화면을 떠남)라면 사용자는 이미 다른 화면에
          // 있다 — 그 토스트는 아래 언마운트 정리 effect가 대신 알린다. 사용자가 직접
          // 누른 "중단"일 때만(아직 이 화면에 있을 때만) 알린다.
          if (mountedRef.current) {
            showInfo('도로시간 호출을 중단했습니다. 이 클러스터는 확정되지 않았습니다.');
          }
          return;
        }
      }

      const { innerOrder } = orderWithinCluster(
        cluster,
        groups,
        nodes,
        selection.entry,
        selection.exit,
        makeTimeSec(times, groups),
      );
      setClusterEntryExit(cluster.id, selection.entry, selection.exit);
      setClusterInnerOrder(cluster.id, innerOrder, times, fallbacks, fallbackKeys);
      // 실제로 호출이 성공했다면 한도는 풀린 것이다 — 배너를 남겨 둘 이유가 없다.
      if (!haversineOnly && timePairs.length > 0) setRateLimitSeen(null);
      delete collectedRef.current[cluster.id];
      setPartial((prev) => {
        if (!(cluster.id in prev)) return prev;
        const { [cluster.id]: _done, ...rest } = prev;
        return rest;
      });
      if (index < clusterOrder.length - 1) setIndex(index + 1);
    } catch (err) {
      if (err instanceof RateLimitExceededError) {
        // 던지기 전까지 모은 결과를 지키고, 재시도 때는 빠진 쌍만 다시 부른다.
        const merged = { ...times, ...err.partial.times };
        collectedRef.current[cluster.id] = {
          times: merged,
          fallbacks: err.partial.fallbacks,
          fallbackKeys: [...err.partial.fallbackKeys],
        };
        const state = { saved: Object.keys(merged).length, total: timePairs.length };
        setPartial((prev) => ({ ...prev, [cluster.id]: state }));
        setRateLimited(state);
        setRateLimitSeen(state);
      } else {
        showApiError(err, '도로시간을 불러오지 못했습니다.');
      }
    } finally {
      setProgress(null);
    }
  };

  const finish = () => {
    const state = useSessionStore.getState();
    const finalOrder = assembleFinalOrder(
      withPicks(state.clusters, state.clusterPicks),
      state.clusterOrder,
      state.groups,
      state.nodes,
    );
    setFinalOrder(finalOrder);
    setStep(6);
  };

  // ── 확정 파이프라인에 딸린 effect(3-1) ──────────────────────────────────
  // 지도용 파생값(아래)보다 먼저 둔다 — confirm()을 어떻게 자동 호출하는지가
  // 한 덩어리로 이어져야 이 effect 셋의 흐름을 지도 memo에 가로막히지 않고 읽을 수 있다.
  // 렌더 중에 ref를 건드리지 않는다. 커밋 뒤에 최신 confirm으로 갈아 끼운다.
  useEffect(() => {
    confirmRef.current = confirm;
  });

  // 단일 건물 클러스터 자동 확정.
  // 반드시 위 effect(= confirmRef 갱신) 다음에 선언해야 한다. effect는 선언 순서대로
  // 실행되므로, 앞에 두면 첫 렌더에서 아직 비어 있는 ref를 불러 아무 일도 일어나지 않는다.
  useEffect(() => {
    const cluster = clusterById.get(clusterOrder[index]);
    if (!cluster || cluster.groupIds.length !== 1) return;
    if (clusterPicks[cluster.id]?.innerOrder !== undefined) return; // 이미 확정
    if (progress !== null || rateLimited !== null) return; // 다른 일이 진행 중
    if (autoDoneRef.current.has(cluster.id)) return; // StrictMode 이중 실행 방지
    autoDoneRef.current.add(cluster.id);
    autoRunRef.current.push(index);
    void confirmRef.current();
  }, [clusterById, clusterOrder, index, clusterPicks, progress, rateLimited]);

  // 단일 지점이 연속하면 확정→이동이 한 프레임 단위로 이어져 알약 숫자가 12에서 17로 튄다.
  // 연쇄가 멈춘 뒤(= 현재 칸이 더 이상 자동 확정 대상이 아닐 때) 건너뛴 구간을 알린다.
  useEffect(() => {
    const run = autoRunRef.current;
    if (run.length === 0) return;
    const cluster = clusterById.get(clusterOrder[index]);
    const chaining =
      cluster !== undefined &&
      cluster.groupIds.length === 1 &&
      clusterPicks[cluster.id]?.innerOrder === undefined;
    if (chaining) return;
    autoRunRef.current = [];
    // 한 칸만 건너뛴 것은 눈으로 따라갈 수 있다. 여러 칸일 때만 말한다.
    if (run.length < 2) return;
    showInfo(`${run[0] + 1}–${run[run.length - 1] + 1}번은 단일 지점이라 자동 확정했습니다.`);
  }, [clusterById, clusterOrder, index, clusterPicks]);

  // ── 지도에 넘길 배열들(정체성이 바뀌면 레이어가 재생성되므로 memo 필수) ──
  const visibleGroupIds = useMemo(() => current?.groupIds ?? [], [current]);
  /** 이전 위치(직전 클러스터의 이탈점) · 다음 클러스터 중심 — 참고용이라 클릭되지 않는다. */
  const refPoints = useMemo<RefPoint[]>(() => {
    const points: RefPoint[] = [];
    if (prevExitPoint) {
      points.push({
        id: 'prev',
        at: prevExitPoint,
        kind: 'prev',
        label:
          prevExitGroupId === undefined
            ? '출발지'
            : `이전 위치 · ${labelOf(prevExitGroupId)}`,
      });
    }
    if (nextPoint) {
      points.push({
        id: 'next',
        at: nextPoint,
        kind: 'next',
        label:
          nextEntryGroupId === undefined
            ? '다음 클러스터'
            : `다음 진입 · ${labelOf(nextEntryGroupId)}`,
      });
    }
    return points;
  }, [prevExitPoint, prevExitGroupId, nextPoint, nextEntryGroupId, labelOf]);
  // 이웃 참고점까지 한 화면에 들어와야 어디서 들어오고 어디로 나가는지 판단이 된다.
  const fitPoints = useMemo<LatLng[]>(
    () => [...(current?.hull ?? []), ...refPoints.map((p) => p.at)],
    [current, refPoints],
  );

  const currentPath = useMemo(
    () => (pick?.innerOrder ? [groupPath(pick.innerOrder, groups)] : []),
    [pick, groups],
  );
  /**
   * 이전 위치 → 방금 고른 진입 건물을 잇는 확인선.
   * 진입을 어디로 잡았는지, 직전 클러스터에서 어떻게 넘어오는지 한눈에 보라고 긋는다.
   * 참고용이므로 확정 결과(경로선)와는 다른 점선·색으로 구분한다.
   */
  const entryLinkPath = useMemo<LatLng[][]>(() => {
    if (!prevExitPoint || selection.entry === undefined) return [];
    const g = groupById.get(selection.entry);
    if (!g) return [];
    return [[prevExitPoint, { lat: g.lat, lon: g.lon }]];
  }, [prevExitPoint, selection.entry, groupById]);

  /**
   * 방금 고른 이탈 건물 → 다음 클러스터를 잇는 확인선.
   * 진입선과 짝이 되는 선이다. 들어오는 길만 보이고 나가는 길이 안 보이면
   * 이탈을 반대편에 찍어 놓고도 모른 채 확정하게 된다.
   * 색은 다음 클러스터 다각형과 같은 빨강으로 맞춰 어디로 나가는지 바로 읽히게 했다.
   */
  const exitLinkPath = useMemo<LatLng[][]>(() => {
    if (!nextPoint || selection.exit === undefined) return [];
    const g = groupById.get(selection.exit);
    if (!g) return [];
    return [[{ lat: g.lat, lon: g.lon }, nextPoint]];
  }, [nextPoint, selection.exit, groupById]);

  const donePaths = useMemo(
    () =>
      clusterOrder
        .filter((id) => id !== currentId)
        .map((id) => groupPath(clusterPicks[id]?.innerOrder, groups))
        .filter((path) => path.length >= 2),
    [clusterOrder, currentId, clusterPicks, groups],
  );
  const orderOf = useMemo(() => makeOrderOf(clusterOrder), [clusterOrder]);
  const isDone = useCallback(
    (clusterId: number) => clusterPicks[clusterId]?.innerOrder !== undefined,
    [clusterPicks],
  );

  const labelOrDash = (groupId: number | undefined): string =>
    groupId === undefined ? '— 선택 안 됨' : labelOf(groupId);

  if (!current) {
    return (
      <div className="ro-screen-scroll">
        <div className="ro-s1">
          <p className="ro-hint">클러스터 순서를 먼저 지정하세요.</p>
        </div>
      </div>
    );
  }

  const callCount = current.groupIds.length > 0 ? pairsNeeded(current, groups).length : 0;
  const confirmedHere = pick?.innerOrder !== undefined;
  const partialHere = partial[currentId];
  /** 이미 갖고 있는 칸 수 — 부분 수집이 있으면 그쪽이 확정 행렬까지 합친 값이다. */
  const knownCells = partialHere?.saved ?? Object.keys(reusableTimes(pick)).length;
  /** 누르면 실제로 나가는 호출 수. 유료 자원이므로 누르기 전에 보여야 한다. */
  const pendingCalls = Math.max(0, callCount - knownCells);
  /** 확정 뒤 표시할 실제 호출 수 — 행렬 칸 수에서 직선거리로 메운 칸을 뺀 값. */
  const actualCalls =
    Object.keys(pick?.timeMatrix ?? {}).length - (pick?.haversineFallbacks ?? 0);

  const confirmHead = confirmedHere ? '다시 계산' : '확정';
  const confirmLabel = haversineMode
    ? `${confirmHead} · 직선거리 (호출 없음)`
    : pendingCalls === 0
      ? `${confirmHead} · 호출 없음`
      : `${confirmHead} · 호출 ${pendingCalls}회`;

  return (
    <div className="ro-mapscreen">
      <div className="ro-mapscreen__canvas">
        <MapCanvas center={origin ?? undefined}>
          <MapFit points={fitPoints} />
          <ClusterLayer
            clusters={clusters}
            mode="dim"
            orderOf={orderOf}
            isDone={isDone}
            activeClusterId={current.id}
            prevClusterId={prevClusterId}
            nextClusterId={nextClusterId}
          />
          <RouteLayer paths={donePaths} style="solid" color={MAP_COLOR.done} />
          <RouteLayer paths={entryLinkPath} style="dashed" color={MAP_COLOR.done} />
          <RouteLayer paths={exitLinkPath} style="dashed" color={MAP_COLOR.next} />
          <RouteLayer paths={currentPath} style="solid" />
          <RefPointLayer points={refPoints} />
          <MarkerLayer
            groups={groups}
            visibleGroupIds={visibleGroupIds}
            entryGroupId={selection.entry}
            exitGroupId={selection.exit}
            tooltipOf={labelOf}
            onGroupClick={onGroupClick}
          />
        </MapCanvas>
      </div>

      <div className="ro-pill">
        <button
          type="button"
          className="ro-pill__nav"
          aria-label="이전 클러스터"
          disabled={index === 0 || running}
          onClick={() => setIndex(index - 1)}
        >
          ‹
        </button>
        {/*
          클러스터 내부 id(C7)는 더 보여 주지 않는다. S5 지도에는 아무 숫자도 찍히지
          않으므로 대조할 대상이 없고, 방문 순번과 나란히 두면 둘 중 무엇이 순서인지
          읽을 수 없다(S4가 같은 이유로 이미 없앤 표기다).
        */}
        <div className="ro-pill__text">
          클러스터 {index + 1} / {clusterOrder.length}{' '}
          <span className="ro-muted" style={{ fontWeight: 400 }}>
            · {current.groupIds.length}건
          </span>
        </div>
        <button
          type="button"
          className="ro-pill__nav"
          aria-label="다음 클러스터"
          disabled={index >= clusterOrder.length - 1 || running}
          onClick={() => setIndex(index + 1)}
        >
          ›
        </button>
      </div>

      <SidePanel
        className="ro-panel--s5"
        title="진입 · 이탈 지점"
        // 알약의 숫자는 진행률이 아니라 현재 위치다. 남은 일의 양은 따로 적어야 한다.
        note={`확정 ${confirmedCount} / ${clusterOrder.length}`}
        subtitle={
          pickingExit
            ? '마커를 누르면 이탈만 바뀝니다.'
            : autoCount > 0
              ? `마커 1번 클릭 = 진입, 2번째 = 이탈(둘 다 정해진 뒤 누르면 진입부터). 단일 지점 ${autoCount}개는 자동 확정.`
              : '마커 1번 클릭 = 진입, 2번째 = 이탈(둘 다 정해진 뒤 누르면 진입부터).'
        }
        bodyClassName="ro-panel__scroll"
        footer={
          <>
            {/* 모달을 닫아도 한도에 걸렸다는 사실과 세 선택지는 남아 있어야 한다. */}
            {rateLimitSeen && !rateLimited ? (
              <div className="ro-calls">
                <span className="ro-warn-text">
                  API 한도 감지됨 · {rateLimitSeen.saved}/{rateLimitSeen.total} 쌍 저장됨
                </span>
                <button
                  type="button"
                  className="ro-btn ro-btn--xs"
                  onClick={() => setRateLimited(rateLimitSeen)}
                >
                  선택지 다시 열기
                </button>
              </div>
            ) : null}
            {partialHere && !confirmedHere && !running ? (
              <div className="ro-calls">
                <span className="ro-warn-text">
                  부분 수집 {partialHere.saved} / {partialHere.total}
                </span>
                <span>떠나면 사라집니다</span>
              </div>
            ) : null}
            {running ? (
              <>
                <div className="ro-calls">
                  <span>
                    도로시간 호출 {progress.done} / {progress.total}
                  </span>
                  <button type="button" className="ro-btn ro-btn--sm ro-btn--danger" onClick={abort}>
                    중단
                  </button>
                </div>
                <ProgressTrack
                  thin
                  pct={progress.total ? Math.round((progress.done / progress.total) * 100) : 0}
                />
                <div className="ro-hint ro-hint--small">
                  호출 중에는 클러스터 이동과 선택 변경이 잠깁니다.
                </div>
              </>
            ) : null}
            {confirmedHere && !running ? (
              <>
                <div className="ro-calls">
                  {/* 행렬 칸 수가 아니라 실제로 나간 호출 수다. 직선거리로 확정했으면 0회다. */}
                  <span>도로시간 {actualCalls}회</span>
                  {/* 0건일 때는 굳이 알릴 것이 없다. 대체가 일어났을 때만 말한다. */}
                  {pick?.haversineFallbacks ? (
                    <span className="ro-warn-text">직선거리 대체 {pick.haversineFallbacks}칸</span>
                  ) : null}
                </div>
                <ProgressTrack thin tone="ok" pct={100} />
              </>
            ) : null}
            {haversineMode && !running ? (
              <div className="ro-calls">
                <span className="ro-warn-text">직선거리 추정으로 진행 중</span>
                <button
                  type="button"
                  className="ro-btn ro-btn--xs"
                  onClick={() => {
                    setHaversineMode(false);
                    tracker.reset();
                  }}
                >
                  도로시간 다시 쓰기
                </button>
              </div>
            ) : null}
            {single ? (
              // 단일 지점은 자동으로 확정된다. 누를 것이 없으므로 버튼 대신 결과만 알린다.
              <div className="ro-autodone">단일 지점 · 자동 확정</div>
            ) : (
              <>
                <button
                  type="button"
                  className="ro-btn ro-btn--md ro-btn--primary ro-btn--block"
                  disabled={!ready || running}
                  title={ready ? undefined : '진입·이탈을 먼저 고르세요'}
                  onClick={() => void confirm()}
                >
                  {confirmLabel}
                </button>
                {!ready ? (
                  <div className="ro-hint ro-hint--small">진입·이탈을 먼저 고르세요.</div>
                ) : null}
              </>
            )}
            {allConfirmed ? (
              <button
                type="button"
                className="ro-btn ro-btn--md ro-btn--dark ro-btn--block"
                onClick={finish}
              >
                결과 보기
                <ArrowRight size={18} />
              </button>
            ) : nextUnconfirmed !== index ? (
              // 비활성 버튼으로 막으면 어느 클러스터가 남았는지 찾을 길이 없다(S4의 선례).
              <button
                type="button"
                className="ro-btn ro-btn--md ro-btn--outline ro-btn--block"
                disabled={running}
                onClick={() => setIndex(nextUnconfirmed)}
              >
                미확정 {clusterOrder.length - confirmedCount}개 — 첫 미확정 보기
              </button>
            ) : (
              <div className="ro-hint ro-hint--small">
                미확정 {clusterOrder.length - confirmedCount}개 · 지금이 첫 미확정입니다.
              </div>
            )}
          </>
        }
      >
        <div className="ro-ee">
          <div className={`ro-ee__box${selection.entry !== undefined ? ' is-entry' : ''}`}>
            <div className="ro-ee__tag ro-ee__tag--entry">
              <span />
              진입
            </div>
            <div className="ro-ee__name">{labelOrDash(selection.entry)}</div>
          </div>
          <div className={`ro-ee__box${selection.exit !== undefined ? ' is-exit' : ''}`}>
            <div className="ro-ee__tag ro-ee__tag--exit">
              <span />
              이탈
            </div>
            <div className="ro-ee__name">
              {pickingExit ? '— 마커를 고르세요' : labelOrDash(selection.exit)}
            </div>
          </div>
        </div>

        <div className="ro-row" style={{ padding: '0 18px 8px' }}>
          <button
            type="button"
            className="ro-btn ro-btn--sm ro-btn--outline ro-btn--grow"
            disabled={running}
            onClick={applySuggestion}
          >
            제안 적용
          </button>
          {/*
            확정된 클러스터에서 로컬 선택만 비우면 "선택 안 됨"과 "확정 완료"가 동시에 뜨고
            버튼은 비활성이 된다. 확정도 함께 걷어 미확정 상태로 정확히 되돌린다.
          */}
          <button
            type="button"
            className="ro-btn ro-btn--sm ro-btn--quiet"
            disabled={running}
            onClick={() => {
              setSelection({ entry: undefined, exit: undefined });
              if (!confirmedHere) return;
              // 확정만 걷고 이미 값을 치른 도로시간은 넘겨받는다 — 다시 확정해도 호출 0회.
              const saved = reusableTimes(pick);
              const cells = Object.keys(saved).length;
              if (cells > 0) {
                // 넘겨받는 것은 실제 도로시간뿐이다(reusableTimes가 추정 칸을 이미 뺐다).
                collectedRef.current[currentId] = { times: saved, fallbacks: 0, fallbackKeys: [] };
                setPartial((prev) => ({
                  ...prev,
                  [currentId]: { saved: cells, total: callCount },
                }));
              }
              clearClusterPick(currentId);
            }}
          >
            {confirmedHere ? '확정 해제' : '지우기'}
          </button>
        </div>

        {/* 이탈만 옆 동으로 옮기는 것이 가장 흔한 수정인데 기본 규칙으로는 진입부터 다시 찍어야 한다. */}
        {!single ? (
          <div className="ro-row" style={{ padding: '0 18px 12px' }}>
            <button
              type="button"
              className="ro-btn ro-btn--sm ro-btn--grow"
              disabled={running || selection.entry === undefined}
              title={
                selection.entry === undefined ? '진입을 먼저 고르세요' : '다음 마커 클릭이 이탈이 됩니다'
              }
              onClick={() => setPickExitFor(pickingExit ? null : currentId)}
            >
              {pickingExit ? '이탈 고르는 중 — 취소' : '이탈만 바꾸기'}
            </button>
          </div>
        ) : null}

        <div className="ro-neighbors">
          <div className="ro-neighbor">
            <span className="ro-neighbor__tag is-prev">이전 위치</span>
            <span className="ro-neighbor__text">
              {prevClusterId === undefined
                ? origin
                  ? '출발지에서 시작'
                  : '없음'
                : `${index}번째 · ${
                    prevExitGroupId === undefined ? '미지정' : labelOf(prevExitGroupId)
                  }`}
            </span>
          </div>
          <div className="ro-neighbor">
            <span className="ro-neighbor__tag is-next">다음</span>
            <span className="ro-neighbor__text">
              {nextClusterId === undefined
                ? '마지막 클러스터'
                : nextEntryGroupId === undefined
                  ? `${index + 2}번째`
                  : `${index + 2}번째 · ${labelOf(nextEntryGroupId)}`}
            </span>
          </div>
        </div>

        <div className="ro-memberlist">
          {current.groupIds.map((groupId) => {
            const isEntry = selection.entry === groupId;
            const isExit = selection.exit === groupId;
            return (
              <button
                key={groupId}
                type="button"
                className="ro-member"
                disabled={running}
                onClick={() => onGroupClick(groupId)}
              >
                <span
                  className={`ro-member__dot${isEntry ? ' is-entry' : ''}${isExit && !isEntry ? ' is-exit' : ''}`}
                />
                <span className="ro-member__name">{labelOrDash(groupId)}</span>
                <span
                  className={`ro-member__role${isEntry ? ' is-entry' : isExit ? ' is-exit' : ''}`}
                >
                  {isEntry && isExit ? '진입·이탈' : isEntry ? '진입' : isExit ? '이탈' : ''}
                </span>
              </button>
            );
          })}
        </div>
      </SidePanel>

      {rateLimited ? (
        <RateLimitModal
          progress={`클러스터 ${index + 1} / ${clusterOrder.length}`}
          savedPairs={rateLimited.saved}
          totalPairs={rateLimited.total}
          onRetry={(newKey) => {
            setRestKey(newKey);
            // 키를 바꿨으니 연속·누적 카운터도 새로 센다.
            tracker.reset();
            setHaversineMode(false);
            setRateLimited(null);
            setRateLimitSeen(null);
            void confirm(false);
          }}
          onFallback={() => {
            // 이 세션은 직선거리로 진행한다 — 남은 클러스터에서 같은 모달을 반복시키지 않는다.
            setHaversineMode(true);
            setRateLimited(null);
            setRateLimitSeen(null);
            void confirm(true);
          }}
          // 닫아도 위 배너가 남아 세 선택지를 언제든 다시 열 수 있다.
          onClose={() => setRateLimited(null)}
        />
      ) : null}
    </div>
  );
}
