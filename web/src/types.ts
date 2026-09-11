/**
 * 웹 전환판 공용 계약(contract) 타입.
 *
 * 모든 웨이브(core / api / io / map / screens)가 이 파일에만 의존한다.
 * 계획 문서 6절(데이터 모델)을 그대로 옮긴 것이므로, 변경은 계획 갱신과 함께 한다.
 */

/** 위경도 한 쌍. 카카오 SDK의 `LatLng`와 구분되는 순수 데이터 구조다. */
export interface LatLng {
  /** 위도(WGS84) */
  lat: number;
  /** 경도(WGS84) */
  lon: number;
}

/**
 * 업로드한 엑셀·CSV의 원본 행.
 * 모든 열을 그대로 보존하고(값 타입은 알 수 없음) 원본 행 번호만 덧붙인다.
 */
export type Row = Record<string, unknown> & {
  /** 원본 시트에서의 행 번호(0부터, 헤더 제외) */
  rowIndex: number;
};

/**
 * 주소 검증 판정.
 * - `일치`: 역지오코딩 주소가 원본과 같다
 * - `요확인`: 부분 불일치라 사용자 확인이 필요하다
 * - `확인불가`: 역지오코딩 실패 등으로 비교할 수 없다
 * - `위치없음`: 지오코딩 자체가 실패해 좌표가 없다
 * - `수정됨`: 사용자가 주소를 직접 고쳐 다시 지오코딩했다
 */
export type Verdict = '일치' | '요확인' | '확인불가' | '위치없음' | '수정됨';

/** 배송지 한 건. 원본 행 하나에 대응한다. */
export interface Node {
  /** 노드 고유 id(0부터 순번) */
  id: number;
  /** 대응하는 원본 행 번호 */
  rowIndex: number;
  /** 수령인 이름 */
  name: string;
  /** 원본 주소 문자열 */
  address: string;
  /** 지오코딩 위도. 실패 시 null */
  lat: number | null;
  /** 지오코딩 경도. 실패 시 null */
  lon: number | null;
  /** 카카오 로컬 API가 돌려준 확인 주소 */
  kakaoAddr: string;
  /** 좌표를 되돌려 얻은 역지오코딩 주소 */
  reverseAddr: string;
  /** 주소 검증 판정 */
  verdict: Verdict;
}

/**
 * 1차 그룹 = 같은 건물(단지).
 * 지도 마커 1개가 1차 그룹 1개이며, 진입·이탈 지점도 이 단위로 고른다.
 */
export interface PrimaryGroup {
  /** 그룹 고유 id(0부터 순번) */
  id: number;
  /** 대표 노드 id */
  rep: number;
  /** 소속 노드 id 목록(대표 포함) */
  members: number[];
  /** 동·호를 제거한 단지 키(`core/address.ts`의 complexKey) */
  complexKey: string;
  /** 대표 좌표 위도 */
  lat: number;
  /** 대표 좌표 경도 */
  lon: number;
}

/** 2차 클러스터 = 임계값(기본 400m) 안에서 Union-Find로 묶인 1차 그룹 덩어리. */
export interface Cluster {
  /** 클러스터 고유 id(0부터 순번) */
  id: number;
  /** 소속 1차 그룹 id 목록 */
  groupIds: number[];
  /** 소속 그룹 좌표의 평균점. 지도 라벨·거리 제안에 쓴다 */
  centroid: LatLng;
  /** 지도에 그릴 볼록껍질 다각형 좌표(닫히지 않은 순서열) */
  hull: LatLng[];
  /** 사용자가 고른 진입 그룹 id */
  entry?: number;
  /** 사용자가 고른 이탈 그룹 id */
  exit?: number;
  /** 계산된 클러스터 내부 방문 순서(그룹 id 순서열) */
  innerOrder?: number[];
  /** 블록 쌍 도로시간(초). 키는 `"fromGroupId-toGroupId"` 형식 */
  timeMatrix?: Record<string, number>;
  /** 도로시간 호출 실패로 Haversine 추정으로 채운 셀 수 */
  haversineFallbacks?: number;
}

/** 출발지(기사 시작 지점). 직전 값을 localStorage에 기억한다. */
export interface Origin {
  /** 확정된 주소 문자열 */
  address: string;
  /** 위도 */
  lat: number;
  /** 경도 */
  lon: number;
}

/** 6단계 스테퍼의 현재 단계. 1=시작 … 6=결과 */
export type Step = 1 | 2 | 3 | 4 | 5 | 6;

/** localStorage에 저장되는 작업 세션 전체. REST 키는 여기 포함되지 않는다. */
export interface Session {
  /** 현재 단계 */
  step: Step;
  /** 출발지. 미선택이면 null */
  origin: Origin | null;
  /** 업로드한 파일명 */
  fileName: string;
  /** 주소 열을 찾은 시트명. 미확정이면 null */
  sheetName: string | null;
  /** 주소가 들어 있는 열 이름. 미확정이면 null */
  addressColumn: string | null;
  /** 원본 헤더(열 순서 보존) */
  headers: string[];
  /** 원본 행 전체 */
  rows: Row[];
  /** 지오코딩·검증을 거친 배송지 목록 */
  nodes: Node[];
  /** 2차 클러스터 임계값(m) */
  thresholdM: number;
  /** 1차 그룹 목록 */
  groups: PrimaryGroup[];
  /** 2차 클러스터 목록 */
  clusters: Cluster[];
  /** 사용자가 클릭한 클러스터 방문 순서(클러스터 id 순서열) */
  clusterOrder: number[];
  /** 최종 배송 순서(노드 id 순서열) */
  finalOrder: number[];
}

/** 카카오 REST API 호출용 헤더. 값은 `KakaoAK {REST 키}` 형식이다. */
export type KakaoHeaders = { Authorization: string };

/** 2차 클러스터 기본 임계값(m). 계획 12절 참고 — 200m는 클러스터가 과하게 많다. */
export const DEFAULT_THRESHOLD_M = 400;
