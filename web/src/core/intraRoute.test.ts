/**
 * intraRoute.test.ts — 클러스터 내부 순서(계획 9절).
 */

import { describe, expect, test } from 'vitest';

import type { Cluster, Node, PrimaryGroup } from '../types';
import { bufferedHull } from './hull';
import {
  buildBlocks,
  orderWithinCluster,
  pairsNeeded,
  suggestEntryExit,
} from './intraRoute';

function makeNode(id: number, address: string, lat = 35.2, lon = 126.85): Node {
  return {
    id,
    rowIndex: id,
    name: `수령인${id}`,
    address,
    lat,
    lon,
    kakaoAddr: '',
    reverseAddr: '',
    verdict: '일치',
  };
}

function makeGroup(
  id: number, rep: number, members: number[], complexKey: string,
  lat = 35.2, lon = 126.85,
): PrimaryGroup {
  return { id, rep, members, complexKey, lat, lon };
}

function makeCluster(id: number, groupIds: number[], groups: PrimaryGroup[]): Cluster {
  const pts = groupIds.map((gid) => {
    const g = groups[gid];
    return { lat: g.lat, lon: g.lon };
  });
  return {
    id,
    groupIds,
    centroid: {
      lat: pts.reduce((s, p) => s + p.lat, 0) / pts.length,
      lon: pts.reduce((s, p) => s + p.lon, 0) / pts.length,
    },
    hull: bufferedHull(pts),
  };
}

// ── 블록 4개짜리 합성 클러스터 ────────────────────────────────────────────────
// 손으로 만든 도로시간표: 진입 0 → 이탈 3 사이에서 NN은 0→1→2→3(56초)을 고르지만
// 완전탐색은 0→2→1→3(9초)을 찾아야 한다.
const FOUR_TIMES: Record<string, number> = {
  '0-1': 5, '0-2': 6, '0-3': 99,
  '1-0': 5, '1-2': 50, '1-3': 1,
  '2-0': 6, '2-1': 2, '2-3': 1,
  '3-0': 99, '3-1': 1, '3-2': 1,
};

function fourBlockFixture() {
  const nodes = [
    makeNode(0, '광주 북구 가로 1, 101동 101호', 35.2000, 126.8500),
    makeNode(1, '광주 북구 나로 2, 101동 101호', 35.2009, 126.8500),
    makeNode(2, '광주 북구 다로 3, 101동 101호', 35.2018, 126.8500),
    makeNode(3, '광주 북구 라로 4, 101동 101호', 35.2027, 126.8500),
  ];
  const groups = [
    makeGroup(0, 0, [0], '광주북구가로1', 35.2000, 126.8500),
    makeGroup(1, 1, [1], '광주북구나로2', 35.2009, 126.8500),
    makeGroup(2, 2, [2], '광주북구다로3', 35.2018, 126.8500),
    makeGroup(3, 3, [3], '광주북구라로4', 35.2027, 126.8500),
  ];
  const cluster = makeCluster(0, [0, 1, 2, 3], groups);
  const timeSec = (from: number, to: number): number => FOUR_TIMES[`${from}-${to}`] ?? 0;
  return { nodes, groups, cluster, timeSec };
}

describe('buildBlocks / pairsNeeded', () => {
  test('단지키가 다르면 블록도 나뉜다', () => {
    const { groups, cluster } = fourBlockFixture();
    const blocks = buildBlocks(cluster, groups);
    expect(blocks.map((b) => b.rep)).toEqual([0, 1, 2, 3]);
    expect(blocks.map((b) => b.groupIds)).toEqual([[0], [1], [2], [3]]);
  });

  test('같은 단지는 한 블록으로 묶이고 대표는 첫 멤버', () => {
    const groups = [
      makeGroup(0, 0, [0], '광주북구삼정로7'),
      makeGroup(1, 1, [1], '광주북구삼정로7'),
      makeGroup(2, 2, [2], '광주북구매곡로92'),
    ];
    const blocks = buildBlocks(makeCluster(0, [0, 1, 2], groups), groups);
    expect(blocks).toHaveLength(2);
    expect(blocks[0]).toMatchObject({ rep: 0, groupIds: [0, 1] });
    expect(blocks[1]).toMatchObject({ rep: 2, groupIds: [2] });
  });

  test('주소가 없으면 단독 블록', () => {
    const groups = [makeGroup(0, 0, [0], ''), makeGroup(1, 1, [1], '')];
    const blocks = buildBlocks(makeCluster(0, [0, 1], groups), groups);
    expect(blocks.map((b) => b.key)).toEqual(['#0', '#1']);
  });

  test('필요한 도로시간 쌍은 k(k-1)개', () => {
    const { groups, cluster } = fourBlockFixture();
    const pairs = pairsNeeded(cluster, groups);
    expect(pairs).toHaveLength(12);
    expect(pairs).toContainEqual([0, 3]);
    expect(pairs).toContainEqual([3, 0]);
    expect(pairs.every(([a, b]) => a !== b)).toBe(true);
  });

  test('블록이 하나면 호출이 필요 없다', () => {
    const groups = [makeGroup(0, 0, [0], '같은단지'), makeGroup(1, 1, [1], '같은단지')];
    expect(pairsNeeded(makeCluster(0, [0, 1], groups), groups)).toEqual([]);
  });
});

describe('orderWithinCluster — 블록 ≤ 8이면 완전탐색', () => {
  test('진입·이탈을 고정한 최적 순서를 찾는다', () => {
    const { nodes, groups, cluster, timeSec } = fourBlockFixture();
    const { innerOrder } = orderWithinCluster(cluster, groups, nodes, 0, 3, timeSec);
    expect(innerOrder).toEqual([0, 2, 1, 3]);
  });

  test('완전탐색 결과가 NN(0→1→2→3)보다 빠르다', () => {
    const { nodes, groups, cluster, timeSec } = fourBlockFixture();
    const { innerOrder } = orderWithinCluster(cluster, groups, nodes, 0, 3, timeSec);
    const cost = (order: number[]): number => {
      let t = 0;
      for (let i = 0; i + 1 < order.length; i++) t += timeSec(order[i], order[i + 1]);
      return t;
    };
    expect(cost(innerOrder)).toBe(9);
    expect(cost([0, 1, 2, 3])).toBe(56);
  });

  test('진입·이탈을 바꾸면 순서도 바뀐다', () => {
    const { nodes, groups, cluster, timeSec } = fourBlockFixture();
    const { innerOrder } = orderWithinCluster(cluster, groups, nodes, 3, 0, timeSec);
    expect(innerOrder[0]).toBe(3);
    expect(innerOrder[innerOrder.length - 1]).toBe(0);
    expect([...innerOrder].sort((a, b) => a - b)).toEqual([0, 1, 2, 3]);
  });

  test('진입 = 이탈(마커 하나)도 허용', () => {
    const groups = [makeGroup(0, 0, [0], '단독단지')];
    const nodes = [makeNode(0, '광주 북구 가로 1, 101동 101호')];
    const cluster = makeCluster(0, [0], groups);
    const r = orderWithinCluster(cluster, groups, nodes, 0, 0, () => 0);
    expect(r.innerOrder).toEqual([0]);
    expect(r.finalNodeOrder).toEqual([0]);
  });

  test('진입 = 이탈이고 블록이 여럿이면 그 블록에서 출발한다', () => {
    const { nodes, groups, cluster, timeSec } = fourBlockFixture();
    const { innerOrder } = orderWithinCluster(cluster, groups, nodes, 2, 2, timeSec);
    expect(innerOrder[0]).toBe(2);
    expect([...innerOrder].sort((a, b) => a - b)).toEqual([0, 1, 2, 3]);
  });

  test('빈 클러스터', () => {
    const cluster: Cluster = { id: 0, groupIds: [], centroid: { lat: 0, lon: 0 }, hull: [] };
    expect(orderWithinCluster(cluster, [], [], 0, 0, () => 0))
      .toEqual({ innerOrder: [], finalNodeOrder: [] });
  });
});

describe('orderWithinCluster — 블록 > 8이면 NN', () => {
  const nodes: Node[] = [];
  const groups: PrimaryGroup[] = [];
  for (let i = 0; i < 10; i++) {
    nodes.push(makeNode(i, `광주 북구 ${i}번로 ${i + 1}, 101동 101호`, 35.2 + i * 0.0009, 126.85));
    groups.push(makeGroup(i, i, [i], `광주북구${i}번로${i + 1}`, 35.2 + i * 0.0009, 126.85));
  }
  const cluster = makeCluster(0, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], groups);
  const timeSec = (from: number, to: number): number => Math.abs(from - to);

  test('진입에서 NN, 이탈은 마지막에 붙는다', () => {
    const { innerOrder } = orderWithinCluster(cluster, groups, nodes, 3, 7, timeSec);
    expect(innerOrder[0]).toBe(3);
    expect(innerOrder[innerOrder.length - 1]).toBe(7);
    expect(innerOrder).toEqual([3, 2, 1, 0, 4, 5, 6, 8, 9, 7]);
  });

  test('모든 그룹이 정확히 한 번씩 들어간다', () => {
    const { innerOrder } = orderWithinCluster(cluster, groups, nodes, 0, 9, timeSec);
    expect([...innerOrder].sort((a, b) => a - b)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    expect(innerOrder[0]).toBe(0);
    expect(innerOrder[innerOrder.length - 1]).toBe(9);
  });
});

describe('orderWithinCluster — 동 → 호 펼침', () => {
  // 같은 단지(삼정로 7) 안 두 그룹. optimizer.py docstring 예제와 같은 순서가 나와야 한다:
  // 202동 406호 → 206동 311호 → 207동 403호
  const nodes = [
    makeNode(0, '광주 북구 삼정로 7, 101동 101호(두암동, 주공2단지@)'),
    makeNode(1, '광주 북구 삼정로 7, 207동 403호(두암동, 주공2단지@)'),
    makeNode(2, '광주 북구 삼정로 7, 202동 406호(두암동,주공2단지@)'),
    makeNode(3, '광주 북구 삼정로 7, 206동 311호 (두암동, 주공2단지@)'),
  ];
  const groups = [
    makeGroup(0, 0, [0], '광주북구삼정로7'),
    makeGroup(1, 1, [1, 2, 3], '광주북구삼정로7'),
  ];
  const cluster = makeCluster(0, [0, 1], groups);

  test('블록 안 그룹은 대표 주소의 동 → 호 순', () => {
    const { innerOrder } = orderWithinCluster(cluster, groups, nodes, 0, 1, () => 0);
    expect(innerOrder).toEqual([0, 1]); // 101동 < 207동
  });

  test('1차 그룹 멤버도 동 → 호 오름차순으로 펼쳐진다', () => {
    const { finalNodeOrder } = orderWithinCluster(cluster, groups, nodes, 0, 1, () => 0);
    expect(finalNodeOrder).toEqual([0, 2, 3, 1]);
    expect(finalNodeOrder.map((id) => nodes[id].address)).toEqual([
      '광주 북구 삼정로 7, 101동 101호(두암동, 주공2단지@)',
      '광주 북구 삼정로 7, 202동 406호(두암동,주공2단지@)',
      '광주 북구 삼정로 7, 206동 311호 (두암동, 주공2단지@)',
      '광주 북구 삼정로 7, 207동 403호(두암동, 주공2단지@)',
    ]);
  });
});

describe('suggestEntryExit', () => {
  const groups = [
    makeGroup(0, 0, [0], 'a', 35.2000, 126.8500),
    makeGroup(1, 1, [1], 'b', 35.2009, 126.8500),
    makeGroup(2, 2, [2], 'c', 35.2018, 126.8500),
  ];
  const cluster = makeCluster(0, [0, 1, 2], groups);

  test('직전 이탈점에서 가장 가까운 그룹을 진입으로', () => {
    const s = suggestEntryExit(cluster, groups, { lat: 35.2020, lon: 126.8500 }, null);
    expect(s?.entry).toBe(2);
  });

  test('다음 클러스터 중심에 가장 가까운 그룹을 이탈로', () => {
    const s = suggestEntryExit(
      cluster, groups,
      { lat: 35.2020, lon: 126.8500 },   // 직전 이탈 → 진입 2
      { lat: 35.1990, lon: 126.8500 },   // 다음 중심 → 이탈 0
    );
    expect(s).toEqual({ entry: 2, exit: 0 });
  });

  test('진입과 이탈은 겹치지 않는다(멤버가 2개 이상이면)', () => {
    const s = suggestEntryExit(
      cluster, groups,
      { lat: 35.2020, lon: 126.8500 },
      { lat: 35.2020, lon: 126.8500 },   // 진입과 같은 쪽을 가리켜도
    );
    expect(s?.entry).toBe(2);
    expect(s?.exit).not.toBe(2);
  });

  test('직전·다음이 없으면 첫 그룹 진입 + 가장 먼 그룹 이탈', () => {
    expect(suggestEntryExit(cluster, groups, null, null)).toEqual({ entry: 0, exit: 2 });
  });

  test('마커가 하나면 진입 = 이탈', () => {
    const one = [makeGroup(0, 0, [0], 'a', 35.2, 126.85)];
    expect(suggestEntryExit(makeCluster(0, [0], one), one, null, null))
      .toEqual({ entry: 0, exit: 0 });
  });

  test('빈 클러스터는 null', () => {
    const empty: Cluster = { id: 0, groupIds: [], centroid: { lat: 0, lon: 0 }, hull: [] };
    expect(suggestEntryExit(empty, [], null, null)).toBeNull();
  });
});

// ── C1: 진입·이탈은 블록이 아니라 그룹 단위로 지켜져야 한다 ──────────────────
describe('orderWithinCluster — 진입·이탈 불변식(C1)', () => {
  /** 같은 단지(= 블록 하나) 안 세 그룹: 101동 / 102동 / 103동. */
  function oneComplexFixture(count = 3) {
    const key = '광주북구삼정로7';
    const nodes: Node[] = [];
    const groups: PrimaryGroup[] = [];
    for (let i = 0; i < count; i++) {
      nodes.push(makeNode(i, `광주 북구 삼정로 7, 10${i + 1}동 101호`, 35.2 + i * 0.0003, 126.85));
      groups.push(makeGroup(i, i, [i], key, 35.2 + i * 0.0003, 126.85));
    }
    const cluster = makeCluster(0, groups.map((g) => g.id), groups);
    return { nodes, groups, cluster };
  }

  test('한 블록 3그룹 — 진입 103동, 이탈 101동이면 그 순서로 시작·끝난다', () => {
    const { nodes, groups, cluster } = oneComplexFixture();
    // 그룹 2 = 103동(진입), 그룹 0 = 101동(이탈). 동→호 정렬만 하면 [0,1,2]가 되어
    // 사용자가 지도에서 찍은 것과 정반대가 나오던 자리다.
    const { innerOrder } = orderWithinCluster(cluster, groups, nodes, 2, 0, () => 100);

    expect(innerOrder[0]).toBe(2);
    expect(innerOrder[innerOrder.length - 1]).toBe(0);
    expect(innerOrder).toEqual([2, 1, 0]);
  });

  test('한 블록 3그룹 — 가운데 그룹을 진입으로 찍어도 맨 앞에 온다', () => {
    const { nodes, groups, cluster } = oneComplexFixture();
    const { innerOrder } = orderWithinCluster(cluster, groups, nodes, 1, 0, () => 100);

    expect(innerOrder).toEqual([1, 2, 0]); // 진입 뒤 나머지는 동→호(102동 빠짐 → 103동), 끝은 이탈
  });

  test('한 블록 2그룹 — 단일 블록도 앞뒤가 고정된다', () => {
    const { nodes, groups, cluster } = oneComplexFixture(2);
    const { innerOrder } = orderWithinCluster(cluster, groups, nodes, 1, 0, () => 100);

    expect(innerOrder).toEqual([1, 0]);
  });

  test('진입 = 이탈이면 앞만 고정하고 뒤는 자유다', () => {
    const { nodes, groups, cluster } = oneComplexFixture();
    const { innerOrder } = orderWithinCluster(cluster, groups, nodes, 2, 2, () => 100);

    expect(innerOrder[0]).toBe(2);
    expect([...innerOrder].sort((a, b) => a - b)).toEqual([0, 1, 2]);
  });

  test('진입·이탈이 같은 블록이고 다른 블록도 있으면 블록을 쪼개서라도 지킨다', () => {
    const nodes = [
      makeNode(0, '광주 북구 삼정로 7, 101동 101호'),
      makeNode(1, '광주 북구 삼정로 7, 102동 101호'),
      makeNode(2, '광주 북구 삼정로 7, 103동 101호'),
      makeNode(3, '광주 북구 매곡로 92, 101동 101호'),
    ];
    const groups = [
      makeGroup(0, 0, [0], '광주북구삼정로7'),
      makeGroup(1, 1, [1], '광주북구삼정로7'),
      makeGroup(2, 2, [2], '광주북구삼정로7'),
      makeGroup(3, 3, [3], '광주북구매곡로92'),
    ];
    const cluster = makeCluster(0, [0, 1, 2, 3], groups);

    const { innerOrder } = orderWithinCluster(cluster, groups, nodes, 2, 0, () => 100);

    expect(innerOrder[0]).toBe(2);
    expect(innerOrder[innerOrder.length - 1]).toBe(0);
    expect([...innerOrder].sort((a, b) => a - b)).toEqual([0, 1, 2, 3]);
  });

  test('진입·이탈 모든 조합에서 불변식이 깨지지 않는다', () => {
    const { nodes, groups, cluster } = fourBlockFixture();
    const ids = cluster.groupIds;
    for (const entry of ids) {
      for (const exit of ids) {
        const { innerOrder } = orderWithinCluster(cluster, groups, nodes, entry, exit, () => 7);
        expect(innerOrder[0]).toBe(entry);
        if (entry !== exit) expect(innerOrder[innerOrder.length - 1]).toBe(exit);
        expect([...innerOrder].sort((a, b) => a - b)).toEqual([...ids].sort((a, b) => a - b));
      }
    }
  });

  test('finalNodeOrder도 진입 그룹 멤버로 시작하고 이탈 그룹 멤버로 끝난다', () => {
    const nodes = [
      makeNode(0, '광주 북구 삼정로 7, 101동 101호'),
      makeNode(1, '광주 북구 삼정로 7, 101동 202호'),
      makeNode(2, '광주 북구 삼정로 7, 103동 101호'),
    ];
    const groups = [
      makeGroup(0, 0, [0, 1], '광주북구삼정로7'),
      makeGroup(1, 2, [2], '광주북구삼정로7'),
    ];
    const cluster = makeCluster(0, [0, 1], groups);

    const { innerOrder, finalNodeOrder } = orderWithinCluster(
      cluster, groups, nodes, 1, 0, () => 100,
    );

    expect(innerOrder).toEqual([1, 0]);
    expect(finalNodeOrder).toEqual([2, 0, 1]); // 103동 → 101동 101호 → 101동 202호
  });
});

describe('orderWithinCluster — 완전탐색 상한(M13)', () => {
  /** 같은 단지 없이 블록 n개짜리 클러스터. */
  function blocksFixture(n: number) {
    const nodes: Node[] = [];
    const groups: PrimaryGroup[] = [];
    for (let i = 0; i < n; i++) {
      nodes.push(makeNode(i, `광주 북구 ${i}번로 ${i + 1}, 101동 101호`, 35.2 + i * 0.0005, 126.85));
      groups.push(makeGroup(i, i, [i], `광주북구${i}번로${i + 1}`, 35.2 + i * 0.0005, 126.85));
    }
    return { nodes, groups, cluster: makeCluster(0, groups.map((g) => g.id), groups) };
  }

  test('진입 = 이탈 + 블록 8개여도 7! = 5040을 돌지 않는다', () => {
    const { nodes, groups, cluster } = blocksFixture(8);
    let calls = 0;
    const timeSec = (from: number, to: number): number => {
      calls += 1;
      return Math.abs(from - to);
    };

    const { innerOrder } = orderWithinCluster(cluster, groups, nodes, 3, 3, timeSec);

    // 중간 7개 > 6 → NN. 호출 수는 7+6+…+1 = 28.
    expect(calls).toBe(28);
    expect(innerOrder[0]).toBe(3);
    expect([...innerOrder].sort((a, b) => a - b)).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);
  });

  test('중간이 6개면 완전탐색(720가지)을 쓴다', () => {
    const { nodes, groups, cluster } = blocksFixture(8);
    let calls = 0;
    const timeSec = (from: number, to: number): number => {
      calls += 1;
      return Math.abs(from - to);
    };

    orderWithinCluster(cluster, groups, nodes, 0, 7, timeSec);

    expect(calls).toBeGreaterThan(720); // 순열마다 7번씩 비용을 잰다
  });
});

describe('nearestMiddle — 다음 홉 기준은 블록의 마지막 멤버(LOW 5)', () => {
  test('블록 대표가 아니라 마지막 멤버에서 가장 가까운 블록을 고른다', () => {
    const nodes: Node[] = [
      makeNode(0, '광주 북구 삼정로 7, 101동 101호'),
      makeNode(1, '광주 북구 삼정로 7, 102동 101호'),
    ];
    const groups: PrimaryGroup[] = [
      makeGroup(0, 0, [0], '광주북구삼정로7'),
      makeGroup(1, 1, [1], '광주북구삼정로7'),
    ];
    for (let i = 2; i <= 9; i++) {
      nodes.push(makeNode(i, `광주 북구 ${i}번로 ${i}, 101동 101호`));
      groups.push(makeGroup(i, i, [i], `광주북구${i}번로${i}`));
    }
    // 블록 9개(첫 블록에 그룹 0·1) → 중간 7개 > 6 → NN 경로.
    const cluster = makeCluster(0, groups.map((g) => g.id), groups);
    const timeSec = (from: number, to: number): number => {
      if (from === 1) return to === 8 ? 1 : 50; // 마지막 멤버 기준이면 8번이 최근접
      if (from === 0) return to === 2 ? 1 : 50; // 대표 기준이면 2번이 최근접
      return Math.abs(from - to);
    };

    const { innerOrder } = orderWithinCluster(cluster, groups, nodes, 0, 9, timeSec);

    expect(innerOrder).toEqual([0, 1, 8, 7, 6, 5, 4, 3, 2, 9]);
  });
});
