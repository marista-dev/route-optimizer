/**
 * helpers.ts — 화면들이 공유하는 순수 계산 함수.
 * React·DOM·스토어에 의존하지 않으므로 그대로 단위 테스트한다.
 */

import { KakaoAuthError } from '../api';
import {
  buildingDong,
  complexName,
  haversineFallbackSec,
  orderWithinCluster,
  parenBuildingName,
} from '../core';
import type { TimeSecFn } from '../core';
import { NAME_COLUMN } from '../io';
import type { NodeSeed } from '../io';
import { showError } from '../store/toast';
import type {
  Cluster,
  LatLng,
  Node,
  Origin,
  PrimaryGroup,
  Row,
  Verdict,
} from '../types';

/** 표·필터에서 쓰는 판정 목록(표시 순서). */
export const VERDICTS: Verdict[] = ['일치', '요확인', '확인불가', '위치없음', '수정됨'];

/** 사용자가 직접 고칠 수 있는 판정. */
const FIXABLE: ReadonlySet<Verdict> = new Set<Verdict>(['요확인', '확인불가', '위치없음']);

/** 해당 판정의 행에 "수정" 버튼을 붙일지. */
export function isFixable(verdict: Verdict): boolean {
  return FIXABLE.has(verdict);
}

/**
 * 저장된 원본 행에서 지오코딩 씨앗을 되살린다.
 * (`ParsedFile.nodesSeed`는 localStorage에 담지 않으므로 행에서 다시 만든다.)
 */
export function seedsFromRows(rows: readonly Row[], addressColumn: string | null): NodeSeed[] {
  if (!addressColumn) return [];
  const text = (value: unknown): string => (value == null ? '' : String(value).trim());
  return rows.map((row) => ({
    rowIndex: row.rowIndex,
    name: text(row[NAME_COLUMN]),
    address: text(row[addressColumn]),
  }));
}


/** 판정 필터·`verdictCounts`에서 "전체"를 뜻하는 값. */
export const ALL_VERDICTS = '전체' as const;

/** 판정별 건수. `ALL_VERDICTS`는 전체 행 수다. */
export function verdictCounts(
  nodes: readonly Node[],
): Record<Verdict | typeof ALL_VERDICTS, number> {
  const counts = { [ALL_VERDICTS]: nodes.length } as Record<Verdict | typeof ALL_VERDICTS, number>;
  for (const verdict of VERDICTS) counts[verdict] = 0;
  for (const node of nodes) counts[node.verdict] = (counts[node.verdict] ?? 0) + 1;
  return counts;
}

/**
 * API 호출 실패를 한 줄 토스트로 보여준다. 인증 실패는 항상 같은 문구를 쓴다.
 * `KakaoAuthError`가 아니면 `err.message`(있으면)를, 없으면 `fallback`을 보여준다.
 */
export function showApiError(err: unknown, fallback: string): void {
  if (err instanceof KakaoAuthError) {
    showError('REST 키가 잘못되었습니다.');
    return;
  }
  showError(err instanceof Error ? err.message : fallback);
}

/**
 * from → to로 옮긴 새 배열.
 * 범위를 벗어나거나 제자리(from === to)면 원본을 그대로 돌려준다.
 */
export function moveItem<T>(list: readonly T[], from: number, to: number): T[] {
  if (to < 0 || to >= list.length || from === to) return list as T[];
  const next = list.slice();
  const [item] = next.splice(from, 1);
  next.splice(to, 0, item);
  return next;
}

/** 클러스터 id → 방문 순번(1부터). 미지정이면 undefined. */
export function makeOrderOf(clusterOrder: readonly number[]): (clusterId: number) => number | undefined {
  const at = new Map(clusterOrder.map((id, i) => [id, i + 1]));
  return (clusterId: number) => at.get(clusterId);
}

/**
 * 저장된 도로시간 행렬을 {@link TimeSecFn}으로 감싼다.
 * 빠진 칸은 Haversine 40km/h 추정치로 메운다(계획 9절 5번).
 */
export function makeTimeSec(
  times: Readonly<Record<string, number>>,
  groups: readonly PrimaryGroup[],
): TimeSecFn {
  const byId = new Map(groups.map((g) => [g.id, g]));
  return (from, to) => {
    const known = times[`${from}-${to}`];
    if (typeof known === 'number') return known;
    const a = byId.get(from);
    const b = byId.get(to);
    if (!a || !b) return 0;
    return haversineFallbackSec(a.lat, a.lon, b.lat, b.lon);
  };
}

/**
 * 최종 배송 순서 = 방문 순서대로 각 클러스터의 내부 순서를 이어 붙인 것.
 * 진입·이탈이 정해지지 않은 클러스터는 건너뛴다.
 */
export function assembleFinalOrder(
  clusters: readonly Cluster[],
  clusterOrder: readonly number[],
  groups: readonly PrimaryGroup[],
  nodes: readonly Node[],
): number[] {
  const byId = new Map(clusters.map((c) => [c.id, c]));
  const groupList = groups as PrimaryGroup[];
  const nodeList = nodes as Node[];
  const out: number[] = [];
  for (const clusterId of clusterOrder) {
    const cluster = byId.get(clusterId);
    if (!cluster || cluster.entry === undefined || cluster.exit === undefined) continue;
    const timeSec = makeTimeSec(cluster.timeMatrix ?? {}, groups);
    const { finalNodeOrder } = orderWithinCluster(
      cluster,
      groupList,
      nodeList,
      cluster.entry,
      cluster.exit,
      timeSec,
    );
    out.push(...finalNodeOrder);
  }
  return out;
}

/** 클러스터 id → 사용자의 진입·이탈 선택과 계산 결과(스토어의 `clusterPicks` 모양). */
export type ClusterPicks = Readonly<
  Record<number, Pick<Cluster, 'entry' | 'exit' | 'innerOrder' | 'timeMatrix' | 'haversineFallbacks'>>
>;

/**
 * 기하만 담긴 클러스터 배열에 선택 결과를 얹어 `Cluster[]`로 되돌린다.
 * 스토어는 둘을 분리해 두므로(지도 재생성 방지), 둘 다 필요한 계산에서만 합친다.
 */
export function withPicks(clusters: readonly Cluster[], picks: ClusterPicks): Cluster[] {
  return clusters.map((cluster) => {
    const pick = picks[cluster.id];
    return pick ? { ...cluster, ...pick } : cluster;
  });
}

/**
 * 1차 그룹(= 지도 마커 1개)을 사람 이름 대신 건물 이름으로 부른다(현장 피드백 3).
 *
 * 1차 그룹은 "같은 단지"라서 여러 동이 한 그룹에 묶일 수 있다.
 * 멤버의 건물 이름이 모두 같으면 그대로 쓰고, 동이 갈리면 단지 이름만 쓴다.
 *
 * ```
 * groupBuildingLabel(['... 207동 403호(두암동, 주공2단지@)'])          → '주공2단지@ 207동'
 * groupBuildingLabel(['...102동 106호(매곡동,아남@)', '...201동 ...']) → '아남@'
 * ```
 */
export function groupBuildingLabel(addresses: readonly string[]): string {
  const usable = addresses.filter((address) => address.trim().length > 0);
  if (usable.length === 0) return '';
  // 같은 단지라도 어떤 행은 괄호에 단지명이 있고 어떤 행은 도로명뿐이다.
  // 단지명이 적힌 행이 하나라도 있으면 그 이름을 쓴다.
  const base = usable.map(parenBuildingName).find(Boolean) ?? complexName(usable[0]);
  const dongs = new Set(usable.map(buildingDong));
  const dong = dongs.size === 1 ? [...dongs][0] : '';
  return dong ? `${base} ${dong}` : base;
}

/** 노드 id → 소속 1차 그룹 id. */
export function groupOfNode(groups: readonly PrimaryGroup[]): Map<number, number> {
  const map = new Map<number, number>();
  for (const group of groups) {
    for (const member of group.members) map.set(member, group.id);
  }
  return map;
}

/** 노드 id → 소속 클러스터 id. */
export function clusterOfNode(
  groups: readonly PrimaryGroup[],
  clusters: readonly Cluster[],
): Map<number, number> {
  const clusterOfGroup = new Map<number, number>();
  for (const cluster of clusters) {
    for (const groupId of cluster.groupIds) clusterOfGroup.set(groupId, cluster.id);
  }
  const map = new Map<number, number>();
  for (const [nodeId, groupId] of groupOfNode(groups)) {
    const clusterId = clusterOfGroup.get(groupId);
    if (clusterId !== undefined) map.set(nodeId, clusterId);
  }
  return map;
}

/** 최종 순서대로 이은 전체 경로 좌표(출발지가 있으면 맨 앞에 붙인다). */
export function fullPathPoints(
  origin: Origin | null,
  finalOrder: readonly number[],
  nodes: readonly Node[],
): LatLng[] {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const points: LatLng[] = [];
  if (origin) points.push({ lat: origin.lat, lon: origin.lon });
  for (const id of finalOrder) {
    const node = byId.get(id);
    if (node && node.lat !== null && node.lon !== null) {
      points.push({ lat: node.lat, lon: node.lon });
    }
  }
  return points;
}

/** 그룹 id 순서열을 지도에 그릴 좌표열로. */
export function groupPath(
  groupIds: readonly number[] | undefined,
  groups: readonly PrimaryGroup[],
): LatLng[] {
  if (!groupIds) return [];
  const byId = new Map(groups.map((g) => [g.id, g]));
  const points: LatLng[] = [];
  for (const id of groupIds) {
    const group = byId.get(id);
    if (group) points.push({ lat: group.lat, lon: group.lon });
  }
  return points;
}

/** 카카오(다음) 우편번호 서비스가 `oncomplete`로 넘기는 값 중 쓰는 것만. */
export interface PostcodeData {
  userSelectedType?: string;
  roadAddress?: string;
  jibunAddress?: string;
  bname?: string;
  buildingName?: string;
  apartment?: string;
}

/**
 * 우편번호 선택 결과를 데스크톱판(`app.py`)과 같은 규칙으로 한 줄 주소로 만든다.
 * 도로명 선택일 때만 괄호 안에 법정동·건물명을 덧붙인다.
 */
export function buildPostcodeAddress(data: PostcodeData): string {
  const road = data.userSelectedType === 'R';
  const base = (road ? data.roadAddress : data.jibunAddress) ?? '';
  if (!road) return base;

  let extra = '';
  const bname = data.bname ?? '';
  if (bname && /[동로가]$/.test(bname)) extra += bname;
  const building = data.buildingName ?? '';
  if (building && data.apartment === 'Y') extra += (extra ? ', ' : '') + building;
  return extra ? `${base} (${extra})` : base;
}

/** HTML 문자열에 그대로 넣기 위한 최소 이스케이프. 작은따옴표 속성값까지 안전하게 덮는다. */
export function escapeHtml(text: string): string {
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

/** 0-based 열 번호를 엑셀 열 문자로(0 → A, 25 → Z, 26 → AA). 범위를 벗어나면 빈 문자열. */
export function columnLetter(index: number): string {
  if (!Number.isInteger(index) || index < 0) return '';
  let n = index;
  let out = '';
  while (n >= 0) {
    out = String.fromCharCode(65 + (n % 26)) + out;
    n = Math.floor(n / 26) - 1;
  }
  return out;
}

/**
 * 주소가 비어 걸러진 행 수.
 * 파서가 따로 알려주지 않으므로 남은 행의 원본 행 번호에서 되짚는다
 * (마지막 유효 행까지의 원본 행 수 - 남은 행 수).
 */
export function emptyAddressCount(rows: readonly Row[]): number {
  if (rows.length === 0) return 0;
  const lastIndex = rows[rows.length - 1].rowIndex;
  return Math.max(0, lastIndex + 1 - rows.length);
}


/**
 * 저장된 그룹·클러스터가 방금 계산한 것과 값까지 같은지.
 *
 * `setClusters`는 순서·진입·이탈·최종 순서를 모두 지운다. 그래서 S3에 잠깐 들렀다
 * 그대로 나오는 것만으로 확정해 둔 작업이 사라지면 안 된다. 값이 같으면 쓰지 않는다.
 * 좌표는 비교하지 않는다 — 노드가 바뀌면 `setNodes`가 이미 클러스터를 비우기 때문에
 * 여기까지 같은 id·구성으로 오려면 같은 노드 집합이어야 한다.
 */
export function sameClustering(
  storedGroups: readonly PrimaryGroup[],
  storedClusters: readonly Cluster[],
  groups: readonly PrimaryGroup[],
  clusters: readonly Cluster[],
): boolean {
  if (storedGroups.length !== groups.length) return false;
  if (storedClusters.length !== clusters.length) return false;

  for (let i = 0; i < groups.length; i += 1) {
    const a = storedGroups[i];
    const b = groups[i];
    if (a.id !== b.id || a.rep !== b.rep || a.complexKey !== b.complexKey) return false;
    if (!sameNumbers(a.members, b.members)) return false;
  }
  for (let i = 0; i < clusters.length; i += 1) {
    const a = storedClusters[i];
    const b = clusters[i];
    if (a.id !== b.id) return false;
    if (!sameNumbers(a.groupIds, b.groupIds)) return false;
  }
  return true;
}

function sameNumbers(a: readonly number[], b: readonly number[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i += 1) if (a[i] !== b[i]) return false;
  return true;
}
