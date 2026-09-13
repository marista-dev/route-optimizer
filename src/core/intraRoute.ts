/**
 * intraRoute.ts — 클러스터 내부 순서(계획 9절).
 *
 * 데스크톱판 `optimizer._nearest_within_cluster`를 대체한다. 달라진 점은
 * 진입·이탈 1차 그룹을 사용자가 지도에서 고정한다는 것과, 거리(Haversine) 대신
 * 도로시간을 쓴다는 것이다. 같은 단지를 한 덩어리(블록)로 유지하고 블록 내부를
 * 동 → 호 오름차순으로 세우는 규칙은 그대로다.
 *
 * 1. 클러스터 멤버(1차 그룹)를 `complexKey`로 블록화. 블록 대표는 첫 멤버.
 * 2. 블록 대표 쌍 전부의 도로시간이 필요하다({@link pairsNeeded}).
 * 3. 양 끝을 뺀 중간 블록이 6개 이하면 완전탐색(720가지), 넘으면 진입에서 NN 후
 *    이탈을 끝에 붙인다. 같은 판정을 세 갈래(단일 블록·진입=이탈·일반)에 똑같이 쓴다.
 * 4. 블록 내부·1차 그룹 멤버는 `unitSortKey`로 동 → 호 오름차순.
 * 5. 그 뒤 **그룹 단위**로 진입·이탈을 고정한다:
 *    `innerOrder[0] === entryGroupId`, `innerOrder.at(-1) === exitGroupId`.
 *    진입 그룹의 블록은 `[진입, ...나머지 동→호]`, 이탈 그룹의 블록은
 *    `[...나머지 동→호, 이탈]`로 쪼개진다(블록이 하나뿐이어도 똑같이 적용).
 */

import type { Cluster, LatLng, Node, PrimaryGroup, TimeSecFn } from '../types';
import { compareUnitAddr } from './address';
import { haversineKm } from './geo';

/**
 * 완전탐색에 넣을 수 있는 중간 블록 수 상한. 6개 = 720가지 순열.
 * 양 끝(진입·이탈)을 뺀 개수로 재기 때문에 어느 갈래에서도 720가지를 넘지 않는다.
 */
const EXACT_SEARCH_MAX_MIDDLE = 6;

/** 같은 단지(complexKey) 1차 그룹 덩어리. */
export interface Block {
  /** 블록 키 — `complexKey`, 주소가 없으면 `#<그룹id>` */
  key: string;
  /** 소속 1차 그룹 id (클러스터 멤버 순서 그대로) */
  groupIds: number[];
  /** 대표 1차 그룹 id = 첫 멤버. 도로시간 호출은 이 대표끼리만 한다 */
  rep: number;
}

/** {@link orderWithinCluster}의 결과. */
export interface IntraRouteResult {
  /** 클러스터 내부 방문 순서 (1차 그룹 id 순서열) */
  innerOrder: number[];
  /** 1차 그룹 멤버까지 동 → 호 순으로 펼친 최종 노드 id 순서열 */
  finalNodeOrder: number[];
}

function indexGroups(groups: PrimaryGroup[]): Map<number, PrimaryGroup> {
  const m = new Map<number, PrimaryGroup>();
  for (const g of groups) m.set(g.id, g);
  return m;
}

/**
 * 클러스터 멤버를 같은 단지끼리 블록으로 묶는다.
 * 블록 순서·블록 안 멤버 순서 모두 `cluster.groupIds` 순서를 따른다(결정적).
 */
export function buildBlocks(cluster: Cluster, groups: PrimaryGroup[]): Block[] {
  const byId = indexGroups(groups);
  const blocks: Block[] = [];
  const byKey = new Map<string, Block>();
  for (const gid of cluster.groupIds) {
    const g = byId.get(gid);
    if (!g) continue;
    const key = g.complexKey || `#${g.id}`;
    const found = byKey.get(key);
    if (found) {
      found.groupIds.push(gid);
    } else {
      const block: Block = { key, groupIds: [gid], rep: gid };
      byKey.set(key, block);
      blocks.push(block);
    }
  }
  return blocks;
}

/**
 * 도로시간이 필요한 블록 대표 쌍 목록 — `k`개 블록이면 `k(k-1)`개.
 * API 계층(`api/kakaoMobility.ts`)이 무엇을 받아와야 하는지 알려주는 용도다.
 */
export function pairsNeeded(cluster: Cluster, groups: PrimaryGroup[]): [number, number][] {
  const reps = buildBlocks(cluster, groups).map((b) => b.rep);
  const pairs: [number, number][] = [];
  for (const from of reps) {
    for (const to of reps) {
      if (from !== to) pairs.push([from, to]);
    }
  }
  return pairs;
}

/** 중간 블록 전체 순열을 만든다(≤ 6개라 720가지). */
function permutations<T>(items: T[]): T[][] {
  if (items.length <= 1) return [items.slice()];
  const out: T[][] = [];
  for (let i = 0; i < items.length; i++) {
    const rest = items.slice(0, i).concat(items.slice(i + 1));
    for (const p of permutations(rest)) out.push([items[i], ...p]);
  }
  return out;
}

/** 고정된 시작(·끝) 사이 중간 블록을 완전탐색으로 최소 비용 순서로 놓는다. */
function exactMiddle(
  start: Block, end: Block | null, middle: Block[], timeSec: TimeSecFn,
): Block[] {
  if (middle.length === 0) return [];
  let best: Block[] = middle;
  let bestCost = Infinity;
  for (const perm of permutations(middle)) {
    let cost = timeSec(start.rep, perm[0].rep);
    for (let i = 0; i + 1 < perm.length; i++) cost += timeSec(perm[i].rep, perm[i + 1].rep);
    if (end) cost += timeSec(perm[perm.length - 1].rep, end.rep);
    if (cost < bestCost) {
      bestCost = cost;
      best = perm;
    }
  }
  return best;
}

/** 블록을 떠나는 시점의 '현재 위치' = 블록의 마지막 멤버(`optimizer.py:307` 파이썬 원본). */
function lastMemberOf(block: Block): number {
  return block.groupIds[block.groupIds.length - 1];
}

/**
 * 시작 블록에서 출발해 가장 가까운(=도로시간 짧은) 블록을 차례로 잇는다.
 *
 * 다음 홉의 기준점은 방금 들른 블록의 **마지막 멤버**다(대표가 아니다).
 * 데스크톱판 `_nearest_within_cluster`와 같은 규칙이며, 대표끼리만 받아 둔
 * 도로시간표에 없는 쌍은 호출부(`makeTimeSec`)가 Haversine 추정으로 메운다.
 *
 * 동률 처리는 `nnConstruct`/`suggestEntryExit`과 같은 규칙(비용이 같으면
 * 대표 그룹 id가 작은 쪽)을 명시적으로 둔다. 전에는 `remaining` 배열의 앞쪽이
 * 이겼는데, 그건 `buildClusters`/`buildBlocks`가 우연히 id 오름차순으로 넘겨
 * 줘서 결과만 같았을 뿐이다 — 그 우연에 기대지 않도록 코드로 고정한다.
 */
function nearestMiddle(start: Block, middle: Block[], timeSec: TimeSecFn): Block[] {
  const remaining = middle.slice();
  const ordered: Block[] = [];
  let currentId = lastMemberOf(start);
  while (remaining.length > 0) {
    let bestIdx = 0;
    let bestCost = Infinity;
    for (let i = 0; i < remaining.length; i++) {
      const t = timeSec(currentId, remaining[i].rep);
      if (t < bestCost || (t === bestCost && remaining[i].rep < remaining[bestIdx].rep)) {
        bestCost = t;
        bestIdx = i;
      }
    }
    const [picked] = remaining.splice(bestIdx, 1);
    ordered.push(picked);
    currentId = lastMemberOf(picked);
  }
  return ordered;
}

/** `list`에서 `value`를 뽑아 맨 앞/맨 뒤로 옮긴다. 없으면 그대로 둔다. */
function moveTo(list: number[], value: number, where: 'front' | 'back'): void {
  const at = list.indexOf(value);
  if (at < 0) return;
  list.splice(at, 1);
  if (where === 'front') list.unshift(value);
  else list.push(value);
}

/**
 * 진입·이탈 1차 그룹을 고정한 채 클러스터 내부 순서를 정한다.
 *
 * 불변식(계획 9절 — 사용자가 지도에서 찍은 진입·이탈을 그대로 지킨다):
 *   - `innerOrder[0] === entryGroupId`
 *   - `innerOrder.at(-1) === exitGroupId`
 *   - 단 `entryGroupId === exitGroupId`이고 그룹이 2개 이상이면 앞만 고정하고
 *     뒤는 자유다(한 점에서 나갔다 들어올 수는 없으므로).
 *
 * 블록(같은 단지) 안은 동 → 호 오름차순을 유지하되, 진입 그룹은 자기 블록의
 * 맨 앞으로, 이탈 그룹은 자기 블록의 맨 뒤로 뽑아낸다. 블록이 하나뿐인
 * 클러스터(아파트 단지 하나짜리 — 가장 흔한 모양)도 예외가 아니다.
 *
 * `entryGroupId`/`exitGroupId`가 이 클러스터에 없으면 각각 첫/마지막 블록으로
 * 폴백하고, 그 경우에만 앞뒤 고정이 적용되지 않는다.
 */
export function orderWithinCluster(
  cluster: Cluster,
  groups: PrimaryGroup[],
  nodes: Node[],
  entryGroupId: number,
  exitGroupId: number,
  timeSec: TimeSecFn,
): IntraRouteResult {
  const byId = indexGroups(groups);
  const nodeById = new Map<number, Node>();
  for (const n of nodes) nodeById.set(n.id, n);

  const blocks = buildBlocks(cluster, groups);
  if (blocks.length === 0) return { innerOrder: [], finalNodeOrder: [] };

  const blockIndexOf = (gid: number): number =>
    blocks.findIndex((b) => b.groupIds.includes(gid));

  let si = blockIndexOf(entryGroupId);
  if (si < 0) si = 0;
  let ei = blockIndexOf(exitGroupId);
  if (ei < 0) ei = blocks.length - 1;

  // 중간 블록이 6개 이하일 때만 완전탐색. 세 갈래 모두 같은 잣대를 쓴다.
  const arrange = (start: Block, end: Block | null, middle: Block[]): Block[] =>
    middle.length <= EXACT_SEARCH_MAX_MIDDLE
      ? exactMiddle(start, end, middle, timeSec)
      : nearestMiddle(start, middle, timeSec);

  let orderedBlocks: Block[];
  if (blocks.length === 1) {
    orderedBlocks = blocks;
  } else if (si === ei) {
    // 진입·이탈이 같은 블록 → 그 블록으로 시작하고 블록 순서의 끝은 고정하지 않는다.
    const start = blocks[si];
    const middle = blocks.filter((_, i) => i !== si);
    orderedBlocks = [start, ...arrange(start, null, middle)];
  } else {
    const start = blocks[si];
    const end = blocks[ei];
    const middle = blocks.filter((_, i) => i !== si && i !== ei);
    orderedBlocks = [start, ...arrange(start, end, middle), end];
  }

  const addrOf = (nodeId: number): string => nodeById.get(nodeId)?.address ?? '';

  const innerOrder: number[] = [];
  for (const block of orderedBlocks) {
    const sorted = block.groupIds.slice().sort((a, b) => {
      const ga = byId.get(a) as PrimaryGroup;
      const gb = byId.get(b) as PrimaryGroup;
      return compareUnitAddr(addrOf(ga.rep), ga.rep, addrOf(gb.rep), gb.rep);
    });
    innerOrder.push(...sorted);
  }

  // 그룹 단위 진입·이탈 고정. 블록을 쪼개는 유일한 지점이다.
  // (블록이 하나뿐이면 `[진입, ...동→호, 이탈]`이 된다.)
  moveTo(innerOrder, entryGroupId, 'front');
  if (exitGroupId !== entryGroupId) moveTo(innerOrder, exitGroupId, 'back');

  const finalNodeOrder: number[] = [];
  for (const gid of innerOrder) {
    const g = byId.get(gid);
    if (!g) continue;
    const members = g.members.slice().sort(
      (a, b) => compareUnitAddr(addrOf(a), a, addrOf(b), b),
    );
    finalNodeOrder.push(...members);
  }

  return { innerOrder, finalNodeOrder };
}

/** {@link suggestEntryExit}의 결과. */
export interface EntryExitSuggestion {
  /** 제안 진입 1차 그룹 id */
  entry: number;
  /** 제안 이탈 1차 그룹 id */
  exit: number;
}

/**
 * 진입·이탈 지점 자동 제안(계획 7절 S5).
 *
 * - 진입 = 직전 클러스터 이탈 지점에서 가장 가까운 그룹. 직전이 없으면 첫 그룹
 * - 이탈 = 다음 클러스터 중심에 가장 가까운 그룹(진입 제외).
 *   다음이 없으면 진입에서 가장 먼 그룹
 *
 * 거리가 같으면 그룹 id가 작은 쪽을 고른다(결정적).
 */
export function suggestEntryExit(
  cluster: Cluster,
  groups: PrimaryGroup[],
  prevExitLatLng: LatLng | null,
  nextCentroid: LatLng | null,
): EntryExitSuggestion | null {
  const byId = indexGroups(groups);
  const members = cluster.groupIds
    .map((gid) => byId.get(gid))
    .filter((g): g is PrimaryGroup => g !== undefined);
  if (members.length === 0) return null;
  if (members.length === 1) return { entry: members[0].id, exit: members[0].id };

  const pick = (
    candidates: PrimaryGroup[],
    target: LatLng,
    farthest: boolean,
  ): PrimaryGroup => {
    let best = candidates[0];
    let bestD = haversineKm(target.lat, target.lon, best.lat, best.lon);
    for (const g of candidates.slice(1)) {
      const d = haversineKm(target.lat, target.lon, g.lat, g.lon);
      const better = farthest ? d > bestD : d < bestD;
      if (better || (d === bestD && g.id < best.id)) {
        best = g;
        bestD = d;
      }
    }
    return best;
  };

  const entry = prevExitLatLng ? pick(members, prevExitLatLng, false) : members[0];
  const rest = members.filter((g) => g.id !== entry.id);
  const exit = nextCentroid
    ? pick(rest, nextCentroid, false)
    : pick(rest, { lat: entry.lat, lon: entry.lon }, true);

  return { entry: entry.id, exit: exit.id };
}
