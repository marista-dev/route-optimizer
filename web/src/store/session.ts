import { create } from 'zustand';
import { persist, createJSONStorage } from 'zustand/middleware';

import {
  DEFAULT_THRESHOLD_M,
  type Cluster,
  type KakaoHeaders,
  type Node,
  type Origin,
  type PrimaryGroup,
  type Row,
  type Session,
  type Step,
} from '../types';

/** localStorage 키. 스키마가 바뀌면 뒤에 버전을 붙인다. */
const STORAGE_KEY = 'route-optimizer-session';

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
}

/** 세션 상태에 붙는 변경 액션들. */
interface SessionActions {
  /** 스테퍼 단계 이동 */
  setStep: (step: Step) => void;
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
  ) => void;
  /** 최종 배송 순서 저장 */
  setFinalOrder: (finalOrder: number[]) => void;
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
}

/** 새 작업의 초기 상태. 출발지는 `reset` 시에도 유지하려고 따로 다룬다. */
const initialSession: Session = {
  step: 1,
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
 * 작업 세션 스토어. localStorage에 저장되어 새로고침·재접속 시 "이어서 하기"를 제공한다.
 * REST 키는 여기 들어가지 않는다(`restKey` 참고).
 */
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
      clusterPicks: {},

      setStep: (step) => set({ step, ...touch() }),

      setOrigin: (origin) => set({ origin, ...touch() }),

      setFile: (fileName, sheetName, addressColumn, headers, rows) =>
        set({
          fileName,
          sheetName,
          addressColumn,
          headers,
          rows,
          nodes: [],
          groups: [],
          clusters: [],
          clusterOrder: [],
          finalOrder: [],
          clusterPicks: {},
          ...touch(),
        }),

      // 주소를 고치면 노드 좌표·단지 키가 달라지므로 그룹 이후 산출물은 모두 버린다.
      setNodes: (nodes) =>
        set({
          nodes,
          groups: [],
          clusters: [],
          clusterOrder: [],
          finalOrder: [],
          clusterPicks: {},
          ...touch(),
        }),

      setThreshold: (thresholdM) => set({ thresholdM, ...touch() }),

      setClusters: (groups, clusters) =>
        set({
          groups,
          clusters,
          clusterOrder: [],
          finalOrder: [],
          clusterPicks: {},
          ...touch(),
        }),

      setClusterOrder: (clusterOrder) =>
        set({ clusterOrder, finalOrder: [], clusterPicks: {}, ...touch() }),

      // 아래 두 액션은 `clusters`를 건드리지 않는다(지도 다각형 재생성 방지).
      setClusterEntryExit: (clusterId, entry, exit) =>
        set((state) => ({
          clusterPicks: {
            ...state.clusterPicks,
            [clusterId]: { ...state.clusterPicks[clusterId], entry, exit },
          },
          ...touch(),
        })),

      setClusterInnerOrder: (clusterId, innerOrder, timeMatrix, fallbacks) =>
        set((state) => ({
          clusterPicks: {
            ...state.clusterPicks,
            [clusterId]: {
              ...state.clusterPicks[clusterId],
              innerOrder,
              timeMatrix,
              haversineFallbacks: fallbacks,
            },
          },
          ...touch(),
        })),

      setFinalOrder: (finalOrder) => set({ finalOrder, ...touch() }),

      /** 출발지는 다음 작업에서도 그대로 쓰므로 남긴다(계획 5절 settings.py 대응). */
      reset: () =>
        set((state) => ({
          ...initialSession,
          origin: state.origin,
          savedAt: 0,
          clusterPicks: {},
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
  return { Authorization: `KakaoAK ${restKey.get()}` };
}
