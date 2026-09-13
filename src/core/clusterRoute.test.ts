/**
 * clusterRoute.test.ts — 2차 클러스터 대표 방문 순서(계획 5-1 단계, 웹판 대응).
 *
 * 전부 합성 행렬(실제 배송 데이터 아님)로 검증한다. `timeSec`은
 * `(from, to) => 초`의 순수 조회 함수이므로 `Record<string, number>` 테이블로
 * 손으로 구성한다(`intraRoute.test.ts`의 `FOUR_TIMES` 방식과 동일).
 */

import { describe, expect, test } from 'vitest';

import type { TimeSecFn } from '../types';
import { orderClusters } from './clusterRoute';

/** "from,to" 문자열 키로 방향별 값을 찾는 조회 테이블 → TimeSecFn. */
function makeTimeSec(table: Record<string, number>): TimeSecFn {
  return (from, to) => {
    const v = table[`${from},${to}`];
    if (v === undefined) throw new Error(`시간 없음: ${from} -> ${to}`);
    return v;
  };
}

/** 결과가 `reps`의 순열인지(누락·중복 없이 정확히 한 번씩) 확인. */
function expectIsPermutationOf(result: number[], reps: readonly number[]): void {
  expect(result).toHaveLength(reps.length);
  expect([...result].sort((a, b) => a - b)).toEqual([...reps].sort((a, b) => a - b));
}

/** 테스트 안에서 쓰는 열린 경로 총 비용 헬퍼 — 복귀 간선을 절대 더하지 않는다. */
function openPathCost(order: readonly number[], startId: number, timeSec: TimeSecFn): number {
  if (order.length === 0) return 0;
  let cost = timeSec(startId, order[0]);
  for (let i = 0; i + 1 < order.length; i++) cost += timeSec(order[i], order[i + 1]);
  return cost;
}

const START = -1; // 호출부가 넘기는 출발지 sentinel

describe('orderClusters — 자명한 경우(탐색 없음)', () => {
  test('대표 0개 → 빈 배열', () => {
    const timeSec = makeTimeSec({});
    expect(orderClusters([], timeSec, START)).toEqual([]);
  });

  test('대표 1개 → 그대로', () => {
    const timeSec = makeTimeSec({});
    expect(orderClusters([42], timeSec, START)).toEqual([42]);
  });

  test('대표 2개(출발지 있음) → 출발지에서 가까운 쪽이 먼저', () => {
    const table: Record<string, number> = {
      [`${START},1`]: 10,
      [`${START},2`]: 3,
      '1,2': 1,
      '2,1': 1,
    };
    const timeSec = makeTimeSec(table);
    // 2는 출발지에서 3초, 1은 10초 — 2가 먼저 와야 한다.
    expect(orderClusters([1, 2], timeSec, START)).toEqual([2, 1]);
  });
});

describe('orderClusters — 출발지 고정', () => {
  // 10 -> 20 -> 30 순으로 늘어선 간단한 선형 배치. 출발지에서 가장 가까운
  // 10이 먼저 나와야 하고, 사슬 비용도 이미 최소라 2-opt/or-opt가 손댈
  // 여지가 없어야 한다(=이 배치 자체가 흔들리지 않는다는 것도 같이 확인).
  const LINE_TABLE: Record<string, number> = {
    [`${START},10`]: 1,
    [`${START},20`]: 5,
    [`${START},30`]: 9,
    '10,20': 1,
    '20,30': 1,
    '10,30': 2,
    '20,10': 1,
    '30,20': 1,
    '30,10': 2,
  };

  test('결과 첫 항목이 출발지에서 가장 가까운 대표', () => {
    const timeSec = makeTimeSec(LINE_TABLE);
    const result = orderClusters([30, 20, 10], timeSec, START);
    expect(result[0]).toBe(10);
    expect(result).toEqual([10, 20, 30]);
    expectIsPermutationOf(result, [30, 20, 10]);
  });

  test('닫힌 경로 비용을 재지 않는다: 마지막 대표 -> 출발지 값이 터무니없이 커도 순서가 같다', () => {
    // orderClusters/timeSec 호출부는 `timeSec(rep, START)`(복귀 방향)를 절대
    // 부르지 않는다 — 열린 경로라 복귀 간선이 없기 때문이다. 그 값을 정상
    // 범위와 비정상적으로 큰 값 두 가지로 바꿔서 결과가 똑같은지로 이를 증명한다.
    const cheapReturn: Record<string, number> = { ...LINE_TABLE, '30,-1': 1 };
    const hugeReturn: Record<string, number> = { ...LINE_TABLE, '30,-1': 999_999_999 };
    const resultCheap = orderClusters([30, 20, 10], makeTimeSec(cheapReturn), START);
    const resultHuge = orderClusters([30, 20, 10], makeTimeSec(hugeReturn), START);
    expect(resultHuge).toEqual(resultCheap);
    expect(resultHuge).toEqual([10, 20, 30]);
  });
});

describe('orderClusters — 2-opt/or-opt 개선', () => {
  // 무작위 탐색으로 찾은 5개 대표 행렬 — NN(최근접 이웃) 단독 구성은
  // [2,1,5,3,4](비용 39)를 고르지만, 2-opt/or-opt까지 적용하면 더 싸진다.
  // (탐색 스크립트로 확인: 실제 orderClusters 결과 비용은 28.)
  const START_COST: Record<number, number> = { 1: 2, 2: 1, 3: 6, 4: 16, 5: 18 };
  const EDGES: Record<string, number> = {
    '1,2': 8, '1,3': 5, '1,4': 20, '1,5': 2,
    '2,1': 1, '2,3': 5, '2,4': 10, '2,5': 11,
    '3,1': 11, '3,2': 13, '3,4': 19, '3,5': 5,
    '4,1': 15, '4,2': 16, '4,3': 11, '4,5': 5,
    '5,1': 15, '5,2': 11, '5,3': 16, '5,4': 16,
  };
  const table: Record<string, number> = { ...EDGES };
  for (const [id, cost] of Object.entries(START_COST)) table[`${START},${id}`] = cost;
  const timeSec = makeTimeSec(table);
  const REPS = [1, 2, 3, 4, 5];

  // NN 단독(최근접 이웃만, 개선 없이)이 고르는 순서를 손으로 미리 구해 둔 값.
  // (같은 tie-break 규칙 — 비용 같으면 작은 id — 으로 손 계산/스크립트 검증 완료.)
  const NN_ONLY_ORDER = [2, 1, 5, 3, 4];

  test('2-opt/or-opt 적용 결과가 NN 단독보다 총 비용이 낮다', () => {
    const result = orderClusters(REPS, timeSec, START);
    const nnOnlyCost = openPathCost(NN_ONLY_ORDER, START, timeSec);
    const resultCost = openPathCost(result, START, timeSec);
    expect(resultCost).toBeLessThan(nnOnlyCost);
    expect(result).not.toEqual(NN_ONLY_ORDER);
    expectIsPermutationOf(result, REPS);
  });

  test('결정성: 같은 입력을 두 번 돌리면 같은 결과', () => {
    const first = orderClusters(REPS, timeSec, START);
    const second = orderClusters(REPS, timeSec, START);
    expect(second).toEqual(first);
  });
});

describe('orderClusters — 동률·비대칭·출발지 없음', () => {
  test('동률: 비용이 같으면 작은 id가 먼저(대표 2개)', () => {
    const table: Record<string, number> = {
      [`${START},3`]: 5,
      [`${START},7`]: 5,
      '3,7': 2,
      '7,3': 2,
    };
    const timeSec = makeTimeSec(table);
    // 3->7 순서 비용 = 5+2 = 7, 7->3 순서 비용 = 5+2 = 7 — 완전히 같다.
    expect(orderClusters([7, 3], timeSec, START)).toEqual([3, 7]);
  });

  test('비대칭 행렬: 방향에 따라 값이 다를 때 싼 방향을 따라간다', () => {
    const table: Record<string, number> = {
      [`${START},1`]: 1,
      [`${START},2`]: 2,
      [`${START},3`]: 3,
      '1,2': 2, '2,1': 100,
      '2,3': 2, '3,2': 100,
      '1,3': 100, '3,1': 2,
    };
    const timeSec = makeTimeSec(table);
    // 1->2->3 정방향 사슬(비용 1+2+2=5)이 그 외 모든 순열보다 훨씬 싸다.
    const result = orderClusters([1, 2, 3], timeSec, START);
    expect(result).toEqual([1, 2, 3]);
    expectIsPermutationOf(result, [1, 2, 3]);
  });

  test('startId 없음: 첫 대표(reps[0])가 시작점으로 고정된다', () => {
    const table: Record<string, number> = {
      '5,2': 1, '2,5': 1,
      '5,9': 5, '9,5': 5,
      '2,9': 1, '9,2': 1,
    };
    const timeSec = makeTimeSec(table);
    const result = orderClusters([5, 2, 9], timeSec);
    expect(result[0]).toBe(5);
    expect(result).toEqual([5, 2, 9]);
    expectIsPermutationOf(result, [5, 2, 9]);
  });
});

describe('orderClusters — 규모(60~100개)에서도 빨라야 한다', () => {
  test('대표 80개, 결정적 의사난수 행렬에서도 순열이 보존되고 금방 끝난다', () => {
    const n = 80;
    const reps = Array.from({ length: n }, (_, i) => i + 1);

    // 시드 고정 LCG — 같은 결과가 재현되는 합성 데이터.
    let seed = 20240913;
    const next = (): number => {
      seed = (seed * 1103515245 + 12345) & 0x7fffffff;
      return seed / 0x7fffffff;
    };
    const startCost = new Map<number, number>();
    for (const id of reps) startCost.set(id, 1 + Math.floor(next() * 3600));
    const edge = new Map<string, number>();
    for (const a of reps) {
      for (const b of reps) {
        if (a === b) continue;
        edge.set(`${a},${b}`, 1 + Math.floor(next() * 3600));
      }
    }
    const timeSec: TimeSecFn = (from, to) => {
      if (from === START) return startCost.get(to) ?? 0;
      return edge.get(`${from},${to}`) ?? 0;
    };

    const startedAt = Date.now();
    const result = orderClusters(reps, timeSec, START);
    const elapsedMs = Date.now() - startedAt;

    expectIsPermutationOf(result, reps);
    expect(elapsedMs).toBeLessThan(2000);
  });
});
