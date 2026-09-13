/**
 * clusterRoute.ts — 2차 클러스터 대표 방문 순서(계획 5-1 단계, 웹판 대응).
 *
 * 데스크톱판 `optimizer.py`의 5-1 단계(`git show 158d81b:src/core/optimizer.py`
 * 495~580행 부근)를 대체한다. 원본은 `local_nodes = [출발지] + 2차 대표`에
 * dummy 노드를 붙여 OR-Tools `RoutingIndexManager(n+1, 1, [0], [dummy])`로
 * "출발지에서 시작하되 끝은 자유(open path, 미복귀)"인 TSP를 풀고
 * (`PATH_CHEAPEST_ARC` + `GUIDED_LOCAL_SEARCH`, 실패 시 Nearest Neighbor
 * 폴백), OR-Tools를 못 쓰는 브라우저에서는 그 근사를 직접 구현해야 한다.
 *
 * 여기서는 **NN(최근접 이웃) 초기해 → 2-opt → Or-1(한 점 재배치)**를
 * 개선이 없을 때까지 번갈아 반복하는 지역 탐색으로 대체한다. 대표 수가
 * 많아야 수십~100개 수준이라 완전탐색은 못 쓰지만, 이 조합이면 OR-Tools의
 * PATH_CHEAPEST_ARC(=NN류 초기해) + GUIDED_LOCAL_SEARCH(=지역 탐색)가
 * 노리는 것과 같은 방향으로 수렴한다.
 *
 * 주의할 점 두 가지(원본과 다르게 틀리기 쉬운 지점):
 *
 * 1. **열린 경로**다. 마지막 대표에서 출발지로 돌아오는 간선은 원가에 넣지
 *    않는다(OR-Tools의 `ext[i][dummy] = 0`과 동치). 닫힌 TSP용 2-opt를
 *    그대로 쓰면 이 복귀 간선까지 계산에 들어가 버려 순서가 틀어진다.
 * 2. `timeSec`은 **방향성이 있다**(`timeSec(a,b) !== timeSec(b,a)`일 수
 *    있다). 2-opt는 구간을 뒤집으므로 뒤집힌 구간 내부 간선들의 방향도
 *    같이 뒤집힌다 — 델타(변경된 간선만)로 비용을 갱신하면 이 방향 전환을
 *    놓치기 쉽다. 그래서 구간을 뒤집은 뒤 **경로 전체 비용을 다시 잰다**.
 *    대표가 최대 100개라 해도 전체 재계산은 O(n)이라 부담이 없다.
 */

import type { TimeSecFn } from '../types';

/**
 * 2-opt/Or-opt 개선 라운드 상한. 대표가 100개여도 한 라운드는
 * 2-opt(전체 O(n^2)쌍 × 재계산 O(n) = O(n^3) ≈ 1e6회)와
 * Or-opt(재배치 대상 O(n) × 삽입 위치 O(n) × 재계산 O(n) = O(n^3) ≈ 1e6회)를
 * 합쳐도 수백만 회 연산에 그친다. 실제로는 몇 라운드 안에 더 나아지지 않아
 * 멈추지만, 병적인 입력에서도 20라운드(총 수천만 회 연산, 브라우저에서
 * 수십 ms)를 넘기지 않도록 상한을 둔다.
 */
const MAX_IMPROVE_ROUNDS = 20;

/** 열린 경로 전체 비용 = 시작 간선 + 대표 사이 간선들의 합(복귀 간선 없음). */
function pathCost(
  order: readonly number[],
  startCost: (id: number) => number,
  timeSec: TimeSecFn,
): number {
  if (order.length === 0) return 0;
  let cost = startCost(order[0]);
  for (let i = 0; i + 1 < order.length; i++) cost += timeSec(order[i], order[i + 1]);
  return cost;
}

/** `arr`에서 `[i, j]` 구간을 뒤집은 새 배열(원본 불변). */
function reversedBetween(arr: readonly number[], i: number, j: number): number[] {
  const out = arr.slice();
  const seg = out.slice(i, j + 1).reverse();
  out.splice(i, seg.length, ...seg);
  return out;
}

/**
 * 최근접 이웃으로 초기해를 만든다. `suggestEntryExit`/`nearestMiddle`과 같은
 * 규칙: 비용이 같으면 그룹 id가 작은 쪽(결정적).
 */
function nnConstruct(
  nodes: readonly number[],
  timeSec: TimeSecFn,
  startCost: (id: number) => number,
): number[] {
  const remaining = nodes.slice();

  let firstIdx = 0;
  for (let i = 1; i < remaining.length; i++) {
    const a = remaining[i];
    const b = remaining[firstIdx];
    const ca = startCost(a);
    const cb = startCost(b);
    if (ca < cb || (ca === cb && a < b)) firstIdx = i;
  }
  const order = [remaining.splice(firstIdx, 1)[0]];

  while (remaining.length > 0) {
    const current = order[order.length - 1];
    let bestIdx = 0;
    for (let i = 1; i < remaining.length; i++) {
      const a = remaining[i];
      const b = remaining[bestIdx];
      const ca = timeSec(current, a);
      const cb = timeSec(current, b);
      if (ca < cb || (ca === cb && a < b)) bestIdx = i;
    }
    order.push(remaining.splice(bestIdx, 1)[0]);
  }
  return order;
}

/**
 * 열린 경로 2-opt 한 스윕. 모든 `(i, j)` 쌍에 대해 `[i, j]` 구간을 뒤집어
 * 봐서(끝점 j = n-1 포함 — 복귀 간선이 없으니 마지막 위치도 자유롭게 뒤집을
 * 수 있다) 전체 비용이 줄면 즉시 반영한다. 델타 계산 대신 매번
 * {@link pathCost}로 전체를 다시 재는 이유는 파일 머리 주석 2번 참고.
 */
function twoOptPass(
  order: number[],
  timeSec: TimeSecFn,
  startCost: (id: number) => number,
): boolean {
  let improvedAny = false;
  let currentCost = pathCost(order, startCost, timeSec);
  const n = order.length;
  for (let i = 0; i < n - 1; i++) {
    for (let j = i + 1; j < n; j++) {
      const candidate = reversedBetween(order, i, j);
      const cost = pathCost(candidate, startCost, timeSec);
      if (cost < currentCost) {
        order.splice(0, order.length, ...candidate);
        currentCost = cost;
        improvedAny = true;
      }
    }
  }
  return improvedAny;
}

/**
 * Or-1 재배치 한 스윕. 대표 하나씩 뽑아 다른 모든 위치에 끼워 넣어 보고
 * 가장 비용이 낮아지는 자리로 옮긴다(개선이 없으면 그대로).
 */
function orOptPass(
  order: number[],
  timeSec: TimeSecFn,
  startCost: (id: number) => number,
): boolean {
  let improvedAny = false;
  const idsAtPassStart = order.slice();

  for (const node of idsAtPassStart) {
    const at = order.indexOf(node);
    if (at < 0) continue; // 이 시점엔 항상 있어야 하지만 방어적으로 처리

    const currentCost = pathCost(order, startCost, timeSec);
    const without = order.slice(0, at).concat(order.slice(at + 1));

    let bestCost = currentCost;
    let bestCandidate: number[] | null = null;
    for (let insertAt = 0; insertAt <= without.length; insertAt++) {
      const candidate = without.slice(0, insertAt).concat([node], without.slice(insertAt));
      const cost = pathCost(candidate, startCost, timeSec);
      if (cost < bestCost) {
        bestCost = cost;
        bestCandidate = candidate;
      }
    }
    if (bestCandidate) {
      order.splice(0, order.length, ...bestCandidate);
      improvedAny = true;
    }
  }
  return improvedAny;
}

/** 시작 비용이 같으면 id가 작은 쪽이 먼저 — 탐색 없이 바로 판정. */
function pickPairOrder(
  nodes: readonly [number, number],
  timeSec: TimeSecFn,
  startCost: (id: number) => number,
): number[] {
  const [a, b] = nodes;
  const costAB = startCost(a) + timeSec(a, b);
  const costBA = startCost(b) + timeSec(b, a);
  if (costAB < costBA) return [a, b];
  if (costBA < costAB) return [b, a];
  return a < b ? [a, b] : [b, a];
}

/**
 * `nodes`(가상 시작점에서 이어지는 열린 경로 대상)의 방문 순서를 정한다.
 * `startCost(id)` = 가상 시작점 → `id` 비용. 0·1·2개는 탐색 없이 바로
 * 정하고, 그 이상만 NN → (2-opt → Or-opt) 반복으로 다듬는다.
 */
function openPathOrder(
  nodes: readonly number[],
  timeSec: TimeSecFn,
  startCost: (id: number) => number,
): number[] {
  if (nodes.length === 0) return [];
  if (nodes.length === 1) return nodes.slice();
  if (nodes.length === 2) return pickPairOrder([nodes[0], nodes[1]], timeSec, startCost);

  const order = nnConstruct(nodes, timeSec, startCost);
  for (let round = 0; round < MAX_IMPROVE_ROUNDS; round++) {
    const improved2opt = twoOptPass(order, timeSec, startCost);
    const improvedOrOpt = orOptPass(order, timeSec, startCost);
    if (!improved2opt && !improvedOrOpt) break;
  }
  return order;
}

/**
 * 클러스터 대표(2차 그룹 대표 = 1차 그룹 id) 방문 순서를 정한다.
 *
 * `startId`를 주면(호출부는 출발지 sentinel로 보통 -1을 넘긴다) 그 지점에서
 * 시작해 `reps`를 전부 도는 열린 경로를 찾는다 — 마지막 대표에서 어디로도
 * 돌아가지 않는다(파일 머리 주석 참고). `startId`가 없으면 `reps[0]`을
 * 시작점으로 고정하고 나머지만 순서를 정한다(웹판은 출발지를 지도에 안
 * 찍고 건너뛸 수 있어서 이 경우가 생긴다).
 *
 * 결정적이다: 같은 입력엔 항상 같은 출력, 비용이 같으면 그룹 id가 작은
 * 쪽을 고른다(`nearestMiddle`/`suggestEntryExit`과 같은 규칙).
 */
export function orderClusters(
  reps: readonly number[],
  timeSec: TimeSecFn,
  startId?: number,
): number[] {
  if (reps.length === 0) return [];
  if (reps.length === 1) return [reps[0]];

  if (startId !== undefined) {
    return openPathOrder(reps, timeSec, (id) => timeSec(startId, id));
  }

  const [first, ...rest] = reps;
  if (rest.length === 0) return [first];
  const restOrder = openPathOrder(rest, timeSec, (id) => timeSec(first, id));
  return [first, ...restOrder];
}
