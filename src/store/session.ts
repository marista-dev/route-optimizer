import { create } from 'zustand';
import { persist, createJSONStorage } from 'zustand/middleware';

import { authHeaders } from '../api';
import {
  DEFAULT_THRESHOLD_M,
  type Cluster,
  type KakaoHeaders,
  type Node,
  type Origin,
  type PrimaryGroup,
  type RouteMode,
  type Row,
  type Session,
  type Step,
} from '../types';

/** localStorage 키. 스키마가 바뀌면 뒤에 버전을 붙인다. */
const STORAGE_KEY = 'route-optimizer-session';

/**
 * 출발지를 가리키는 가짜 1차 그룹 id. 출발지는 `buildPrimaryGroups`가 만드는
 * 그룹이 아니라서 실제 그룹 id(0부터)와 절대 겹치지 않는 음수를 쓴다.
 */
export const ORIGIN_GROUP_ID = -1;

/** 카카오 REST 키를 담는 sessionStorage 키. localStorage에는 절대 쓰지 않는다. */
const REST_KEY_STORAGE = 'kakao-rest-key';

/**
 * 저장된 세션을 버리는 기한(7일).
 * 원본 행에는 이름·연락처가 들어 있으므로 오래된 세션은 되살리지 않는다(계획 12절).
 */
export const SESSION_TTL_MS = 7 * 24 * 60 * 60 * 1000;

/** 저장 시각이 TTL을 넘겼는지. `savedAt`이 0이면(= 저장한 적 없음) 만료가 아니다. */
export function isSessionExpired(savedAt: number | undefined, now: number = Date.now()): boolean {
  if (!savedAt) return false;
  return now - savedAt > SESSION_TTL_MS;
}

/**
 * 한 클러스터에 대한 사용자 선택과 계산 결과.
 *
 * `Cluster`에도 같은 이름의 선택 필드가 있지만, 거기에 쓰면 확정할 때마다
 * `clusters` 배열 정체성이 바뀌어 지도 다각형이 전부 재생성된다.
 * 그래서 기하(`id`/`groupIds`/`centroid`/`hull`)와 분리해 여기에 모은다.
 */
export interface ClusterPick {
  /** 진입 그룹 id */
  entry?: number;
  /** 이탈 그룹 id */
  exit?: number;
  /** 계산된 내부 방문 순서(그룹 id 순서열) */
  innerOrder?: number[];
  /** 블록 쌍 도로시간(초) */
  timeMatrix?: Record<string, number>;
  /** Haversine 추정으로 채운 셀 수 */
  haversineFallbacks?: number;
  /**
   * 그 셀들의 키. 재계산할 때 실제 도로시간은 그대로 쓰고 이 칸만 다시 받는다.
   * 개수만 알면 행렬을 통째로 재사용하거나 통째로 버리는 수밖에 없다.
   */
  haversineKeys?: string[];
}

/** 세션 상태에 붙는 변경 액션들. */
interface SessionActions {
  /** 스테퍼 단계 이동 */
  setStep: (step: Step) => void;
  /** 경로 모드 전환. 이미 받아 둔 클러스터 도로시간 등은 건드리지 않는다(확인은 화면 쪽 책임) */
  setMode: (mode: RouteMode) => void;
  /** 출발지 확정(또는 null로 해제) */
  setOrigin: (origin: Origin | null) => void;
  /** 파일 파싱 결과 반영. 이후 단계 산출물은 모두 초기화한다 */
  setFile: (
    fileName: string,
    sheetName: string | null,
    addressColumn: string | null,
    headers: string[],
    rows: Row[],
  ) => void;
  /** 지오코딩·검증 결과 반영. 노드가 바뀌면 그룹·클러스터 산출물은 모두 무효가 된다 */
  setNodes: (nodes: Node[]) => void;
  /** 2차 클러스터 임계값 변경 */
  setThreshold: (thresholdM: number) => void;
  /** 그룹핑·클러스터링 결과 반영. 클러스터가 바뀌면 순서 선택은 초기화된다 */
  setClusters: (groups: PrimaryGroup[], clusters: Cluster[]) => void;
  /**
   * 사용자가 클릭한 클러스터 방문 순서 저장.
   * 진입·이탈은 "직전 클러스터 이탈 → 다음 클러스터 진입" 연결을 전제로 고른 값이므로
   * 순서가 바뀌면 모든 클러스터의 선택과 최종 순서를 함께 버린다.
   */
  setClusterOrder: (clusterOrder: number[]) => void;
  /** 한 클러스터의 진입·이탈 그룹 지정 */
  setClusterEntryExit: (clusterId: number, entry: number, exit: number) => void;
  /** 한 클러스터의 내부 순서·도로시간 행렬·Haversine 대체 건수 저장 */
  setClusterInnerOrder: (
    clusterId: number,
    innerOrder: number[],
    timeMatrix: Record<string, number>,
    fallbacks: number,
    fallbackKeys: string[],
  ) => void;
  /** 한 클러스터의 진입·이탈·내부 순서를 지운다(확정 해제) */
  clearClusterPick: (clusterId: number) => void;
  /** 최종 배송 순서 저장 */
  setFinalOrder: (finalOrder: number[]) => void;
  /**
   * 자동 모드가 새로 받아 온 클러스터 대표 1차 그룹 간 도로시간을 기존 행렬 위에 얹는다.
   * 재실행 때 이미 값을 치른 쌍을 다시 사지 않으려고 세션에 누적한다.
   */
  mergeClusterTimes: (times: Record<string, number>, fallbackKeys: readonly string[]) => void;
  /** 출발지를 제외한 모든 작업 상태를 지운다("처음부터") */
  reset: () => void;
}

/** 세션 스토어 타입 = 데이터(Session) + 액션. */
export type SessionStore = Session & SessionMeta & SessionActions;

/**
 * `Session`(= `types.ts`의 공용 계약)에 없지만 화면이 필요로 하는 부가 정보.
 * 계약 타입을 건드리지 않으려고 스토어 쪽에만 둔다.
 */
interface SessionMeta {
  /** 마지막 저장 시각(epoch ms). "이어서 하기" 배너와 TTL 판정에 쓴다 */
  savedAt: number;
  /** 클러스터 id → 진입·이탈·내부 순서. `clusters` 배열과 분리해 둔다 */
  clusterPicks: Record<number, ClusterPick>;
  /**
   * 클러스터 대표 1차 그룹 사이의 도로시간(초). 키는 `"fromGroupId-toGroupId"`.
   * 출발지는 그룹이 아니므로 id `ORIGIN_GROUP_ID`(-1)를 쓴다.
   * 자동 모드가 쓰고, 비싸게 산 값이라 세션에 보관해 재실행 때 재사용한다.
   */
  clusterTimeMatrix: Record<string, number>;
  /** 그중 도로시간을 못 받아 직선거리로 메운 칸의 키. 재실행 때 이 칸만 다시 받는다 */
  clusterHaversineKeys: string[];
}

/** 새 작업의 초기 상태. 출발지는 `reset` 시에도 유지하려고 따로 다룬다. */
const initialSession: Session = {
  step: 1,
  mode: 'manual',
  origin: null,
  fileName: '',
  sheetName: null,
  addressColumn: null,
  headers: [],
  rows: [],
  nodes: [],
  thresholdM: DEFAULT_THRESHOLD_M,
  groups: [],
  clusters: [],
  clusterOrder: [],
  finalOrder: [],
};

/**
 * `SessionMeta`에서 "새 세션(= `savedAt`이 갱신되지 않는 것) 시작 시 어떤 값으로
 * 되돌아가는가"를 규정하는 부분. `savedAt`만 빼면 `SessionMeta` 전체다.
 *
 * 이 타입에 필드를 하나 추가했는데 아래 {@link initialMeta}를 채우지 않으면
 * **타입 오류가 난다**(누락된 프로퍼티) — `setNodes`/`reset`처럼 여러 액션이
 * 손으로 나열하던 것과 달리, 여기 하나만 고치면 store 초기화·`reset`·
 * {@link clearedByNodes}에 자동으로 반영된다(F5).
 */
type ClearableMeta = Omit<SessionMeta, 'savedAt'>;
const initialMeta: ClearableMeta = {
  clusterPicks: {},
  clusterTimeMatrix: {},
  clusterHaversineKeys: [],
};

/**
 * 노드가 바뀌면(`setFile`/`setNodes`) 지워야 하는 모든 산출물.
 * 그룹 id가 통째로 달라지므로 그 id에 의존하는 모든 것을 지운다.
 * `nodes` 자체는 호출부마다 값이 다르므로(하나는 `[]`, 하나는 새 배열) 여기 없다.
 */
const clearedByNodes = (): { groups: PrimaryGroup[]; clusters: Cluster[] } & Pick<
  Session,
  'clusterOrder' | 'finalOrder'
> &
  ClearableMeta => ({
  groups: [],
  clusters: [],
  clusterOrder: [],
  finalOrder: [],
  ...initialMeta,
});

/**
 * 클러스터(2차 묶음)가 바뀌면(`setClusters`) 지워야 하는 것.
 * `clusterTimeMatrix`/`clusterHaversineKeys`는 **일부러 뺀다** — 그룹 id는
 * 그대로라 여전히 유효하고, 비싸게 산 값이라 남긴다(`setClusters` 주석 참고).
 */
const clearedByClusters = (): Pick<Session, 'clusterOrder' | 'finalOrder'> &
  Pick<ClearableMeta, 'clusterPicks'> => ({
  clusterOrder: [],
  finalOrder: [],
  clusterPicks: {},
});

/** 방문 순서(진입·이탈 포함)가 바뀌면 지워야 하는 것. */
const clearedByOrder = (): Pick<Session, 'finalOrder'> & Pick<ClearableMeta, 'clusterPicks'> => ({
  finalOrder: [],
  clusterPicks: {},
});

/**
 * 작업 세션 스토어. localStorage에 저장되어 새로고침·재접속 시 "이어서 하기"를 제공한다.
 * REST 키는 여기 들어가지 않는다(`restKey` 참고).
 */
/** 두 방문 순서가 같은지. 같으면 하위 산출물을 버리지 않는다. */
function sameOrder(a: readonly number[], b: readonly number[]): boolean {
  return a.length === b.length && a.every((id, i) => id === b[i]);
}

/** 모든 변경 액션이 함께 갱신하는 저장 시각. */
const touch = (): Pick<SessionMeta, 'savedAt'> => ({ savedAt: Date.now() });

/**
 * persist가 복원한 값을 현재(초기) 상태에 합친다.
 * TTL이 지난 세션은 되살리지 않고 저장본도 지운다(원본 행에 이름·연락처가 들어 있다).
 */
export function mergePersistedSession(
  persisted: unknown,
  current: SessionStore,
  now: number = Date.now(),
): SessionStore {
  const saved = persisted as Partial<SessionStore> | null | undefined;
  if (!saved || isSessionExpired(saved.savedAt, now)) {
    try {
      localStorage.removeItem(STORAGE_KEY);
    } catch {
      // 프라이빗 모드 등에서 던질 수 있다. 복원을 막는 것만으로 충분하다.
    }
    return current;
  }
  return { ...current, ...saved };
}

export const useSessionStore = create<SessionStore>()(
  persist(
    (set) => ({
      ...initialSession,
      savedAt: 0,
      ...initialMeta,

      setStep: (step) => set({ step, ...touch() }),

      setMode: (mode) => set({ mode, ...touch() }),

      setOrigin: (origin) => set({ origin, ...touch() }),

      // 행렬 키는 1차 그룹 id 쌍이고, 그룹 id는 `nodes` 순서로 매겨진다(grouping.ts).
      // 파일을 새로 읽으면 노드 자체가 갈리므로 행렬도 함께 버린다.
      setFile: (fileName, sheetName, addressColumn, headers, rows) =>
        set({
          fileName,
          sheetName,
          addressColumn,
          headers,
          rows,
          nodes: [],
          ...clearedByNodes(),
          ...touch(),
        }),

      // 주소를 고치면 노드 좌표·단지 키가 달라지므로 그룹 이후 산출물은 모두 버린다.
      // 행렬 키(1차 그룹 id 쌍)도 옛 그룹 id를 가리키게 되므로 함께 버린다.
      setNodes: (nodes) =>
        set({
          nodes,
          ...clearedByNodes(),
          ...touch(),
        }),

      setThreshold: (thresholdM) => set({ thresholdM, ...touch() }),

      // clusterTimeMatrix/clusterHaversineKeys는 여기서 지우지 않는다.
      // 임계값을 바꿔도 buildPrimaryGroups(nodes)의 결과(= 1차 그룹과 그 id)는
      // 그대로다 — 달라지는 건 그 그룹들을 어떻게 묶느냐(2차 클러스터)뿐이다.
      // 행렬 키는 그룹 id 쌍이라 여전히 유효하고, 비싸게 산 값이니 남긴다.
      // (그래서 clearedByNodes()가 아니라 clusterTimeMatrix를 뺀 clearedByClusters()를 쓴다.)
      setClusters: (groups, clusters) =>
        set({
          groups,
          clusters,
          ...clearedByClusters(),
          ...touch(),
        }),

      /*
       * 순서가 바뀌면 진입·이탈과 도로시간 행렬은 더 이상 맞지 않으므로 버린다.
       * 다만 **실제로 달라졌을 때만** 버린다 — S4의 주 조작이 지도 클릭이라, 지도를
       * 끌다 다각형을 스치거나 잘못 눌러 되돌리는 일이 잦다. 값이 같은데도 버리면
       * 그 한 번의 미스클릭이 이미 값을 치른 도로시간까지 전부 날린다.
       *
       * 이 `sameOrder` 분기를 "단순화"하며 지우고 싶다면, 그 전에
       * `session.test.ts`의 '같은 순서를 다시 넣으면…' 테스트부터 보라.
       * 이 분기 하나가 자동 모드 기준 약 1,800회의 유료 호출을 지킨다.
       */
      setClusterOrder: (clusterOrder) =>
        set((state) =>
          sameOrder(state.clusterOrder, clusterOrder)
            ? { clusterOrder, ...touch() }
            : { clusterOrder, ...clearedByOrder(), ...touch() },
        ),

      clearClusterPick: (clusterId) =>
        set((state) => {
          const { [clusterId]: _dropped, ...rest } = state.clusterPicks;
          return { clusterPicks: rest, finalOrder: [], ...touch() };
        }),

      // 아래 두 액션은 `clusters`를 건드리지 않는다(지도 다각형 재생성 방지).
      setClusterEntryExit: (clusterId, entry, exit) =>
        set((state) => ({
          clusterPicks: {
            ...state.clusterPicks,
            [clusterId]: { ...state.clusterPicks[clusterId], entry, exit },
          },
          ...touch(),
        })),

      setClusterInnerOrder: (clusterId, innerOrder, timeMatrix, fallbacks, fallbackKeys) =>
        set((state) => ({
          clusterPicks: {
            ...state.clusterPicks,
            [clusterId]: {
              ...state.clusterPicks[clusterId],
              innerOrder,
              timeMatrix,
              haversineFallbacks: fallbacks,
              haversineKeys: fallbackKeys,
            },
          },
          ...touch(),
        })),

      setFinalOrder: (finalOrder) => set({ finalOrder, ...touch() }),

      mergeClusterTimes: (times, fallbackKeys) =>
        set((state) => {
          const clusterTimeMatrix = { ...state.clusterTimeMatrix, ...times };
          // 예전엔 추정치였던 칸이라도 이번에 실제 도로시간을 받았으면 더 이상
          // 추정 칸이 아니다. 그래서 "예전 추정 칸 ∪ 이번 추정 칸"에서 이번에
          // 실제 값을 받은 키(= times에는 있지만 이번 fallbackKeys엔 없는 키)를 뺀다.
          const fallbackSet = new Set(fallbackKeys);
          const nextHaversine = new Set([...state.clusterHaversineKeys, ...fallbackKeys]);
          for (const key of Object.keys(times)) {
            if (!fallbackSet.has(key)) nextHaversine.delete(key);
          }
          return {
            clusterTimeMatrix,
            clusterHaversineKeys: [...nextHaversine],
            ...touch(),
          };
        }),

      /** 출발지는 다음 작업에서도 그대로 쓰므로 남긴다(계획 5절 settings.py 대응). */
      reset: () =>
        set((state) => ({
          ...initialSession,
          origin: state.origin,
          savedAt: 0,
          ...initialMeta,
        })),
    }),
    {
      name: STORAGE_KEY,
      storage: createJSONStorage(() => localStorage),
      merge: (persisted, current) => mergePersistedSession(persisted, current as SessionStore),
    },
  ),
);

/** sessionStorage 접근은 프라이빗 모드·SSR에서 던질 수 있어 항상 감싼다. */
function safeSessionStorage(): Storage | null {
  try {
    return typeof sessionStorage === 'undefined' ? null : sessionStorage;
  } catch {
    return null;
  }
}

/**
 * 카카오 REST 키 저장소. 탭을 닫으면 사라지는 sessionStorage만 쓴다.
 * localStorage·세션 스토어·저장소에 절대 남기지 않는다(계획 2절).
 */
export const restKey = {
  /** 저장된 REST 키. 없으면 빈 문자열 */
  get(): string {
    return safeSessionStorage()?.getItem(REST_KEY_STORAGE) ?? '';
  },
  /** REST 키 저장 */
  set(key: string): void {
    safeSessionStorage()?.setItem(REST_KEY_STORAGE, key);
  },
  /** REST 키 삭제(rate-limit·유효하지 않은 키로 재입력이 필요할 때) */
  clear(): void {
    safeSessionStorage()?.removeItem(REST_KEY_STORAGE);
  },
};

/** 카카오 REST API 호출용 Authorization 헤더를 만든다. */
export function kakaoHeaders(): KakaoHeaders {
  return authHeaders(restKey.get());
}
