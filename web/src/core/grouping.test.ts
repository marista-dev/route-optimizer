/**
 * grouping.test.ts — 1차 그룹핑·2차 클러스터링이 Python `optimizer.py`와 같은지 검증.
 *
 * fixture는 `scripts/gen_fixtures.py`가 `_build_location_groups` /
 * `_build_secondary_clusters`를 직접 돌려 떠낸 결과다(합성 좌표 15건, 임계값 200·400m).
 */

import { describe, expect, test } from 'vitest';

import type { Node } from '../types';
import { buildClusters, buildPrimaryGroups } from './grouping';
import { haversineKm } from './geo';
import groupingFixture from './__fixtures__/grouping.json';

interface Fixture {
  origin: { lat: number; lon: number; address: string };
  nodes: { id: number; lat: number; lon: number; address: string }[];
  groups: { id: number; rep: number; members: number[]; complexKey: string; lat: number; lon: number }[];
  clusters: Record<string, { id: number; groupIds: number[] }[]>;
}

const fx = groupingFixture as unknown as Fixture;

function toNode(src: { id: number; lat: number | null; lon: number | null; address: string }): Node {
  return {
    id: src.id,
    rowIndex: src.id,
    name: `수령인${src.id}`,
    address: src.address,
    lat: src.lat,
    lon: src.lon,
    kakaoAddr: '',
    reverseAddr: '',
    verdict: '일치',
  };
}

const nodes: Node[] = fx.nodes.map(toNode);

describe('buildPrimaryGroups', () => {
  const groups = buildPrimaryGroups(nodes);

  test('Python과 같은 그룹 구성', () => {
    expect(groups.map((g) => ({
      id: g.id,
      rep: g.rep,
      members: g.members,
      complexKey: g.complexKey,
      lat: g.lat,
      lon: g.lon,
    }))).toEqual(fx.groups);
  });

  test('모든 노드가 정확히 한 그룹에 속한다', () => {
    const seen = groups.flatMap((g) => g.members).sort((a, b) => a - b);
    expect(seen).toEqual(nodes.map((n) => n.id));
  });

  test('좌표가 같으면 같은 그룹', () => {
    const g = groups.find((x) => x.members.includes(0));
    expect(g?.members).toContain(1);
    expect(g?.members).toContain(2);
  });

  test('좌표가 달라도 단지키가 같으면 같은 그룹', () => {
    // 노드 5(35.20180,126.85000)와 6(35.20180,126.85110)은 좌표가 다르지만 삼정로 7로 같다
    expect(nodes[5].lon).not.toBe(nodes[6].lon);
    const g = groups.find((x) => x.members.includes(5));
    expect(g?.members).toEqual([5, 6]);
  });

  test('좌표 없는 노드는 제외한다', () => {
    const withMissing: Node[] = [
      ...nodes,
      toNode({ id: 99, lat: null, lon: null, address: '광주 북구 어딘가 1' }),
    ];
    const result = buildPrimaryGroups(withMissing);
    expect(result.flatMap((g) => g.members)).not.toContain(99);
    expect(result.map((g) => g.rep)).toEqual(groups.map((g) => g.rep));
  });

  test('빈 입력', () => {
    expect(buildPrimaryGroups([])).toEqual([]);
  });
});

describe('buildClusters', () => {
  const groups = buildPrimaryGroups(nodes);

  for (const threshold of ['200', '400']) {
    test(`임계값 ${threshold}m — Python과 같은 클러스터 구성`, () => {
      const clusters = buildClusters(groups, Number(threshold));
      expect(clusters.map((c) => ({ id: c.id, groupIds: c.groupIds })))
        .toEqual(fx.clusters[threshold]);
    });
  }

  test('200m에서 갈라진 덩어리가 400m에서는 합쳐진다', () => {
    const at200 = buildClusters(groups, 200);
    const at400 = buildClusters(groups, 400);
    expect(at400.length).toBeLessThan(at200.length);
  });

  test('임계값 이내 전이적 연결(transitive closure)', () => {
    // 0-1-2는 이웃 간 100m씩이라 0↔2가 200m를 넘어도 한 클러스터가 된다
    const at200 = buildClusters(groups, 200);
    const c = at200.find((x) => x.groupIds.includes(0));
    expect(c?.groupIds).toEqual([0, 1, 2]);
    const g0 = groups[0];
    const g2 = groups[2];
    expect(haversineKm(g0.lat, g0.lon, g2.lat, g2.lon) * 1000).toBeGreaterThan(200);
  });

  test('중심점과 다각형이 함께 계산된다', () => {
    const clusters = buildClusters(groups, 400);
    for (const c of clusters) {
      const members = c.groupIds.map((gid) => groups[gid]);
      expect(c.centroid.lat).toBeCloseTo(
        members.reduce((s, g) => s + g.lat, 0) / members.length, 10);
      expect(c.centroid.lon).toBeCloseTo(
        members.reduce((s, g) => s + g.lon, 0) / members.length, 10);
      expect(c.hull.length).toBeGreaterThanOrEqual(8);
    }
  });

  test('빈 입력', () => {
    expect(buildClusters([], 400)).toEqual([]);
  });
});

describe('haversineKm', () => {
  test('같은 점은 0', () => {
    expect(haversineKm(35.2, 126.85, 35.2, 126.85)).toBe(0);
  });

  test('위도 0.0009도 ≈ 100m', () => {
    const m = haversineKm(35.2, 126.85, 35.2009, 126.85) * 1000;
    expect(m).toBeGreaterThan(95);
    expect(m).toBeLessThan(105);
  });

  test('광주 시청 ↔ 광주역 대략 거리', () => {
    // 대칭성만 확인 (값 자체는 Python 공식과 동일)
    expect(haversineKm(35.15, 126.8, 35.22, 126.87))
      .toBeCloseTo(haversineKm(35.22, 126.87, 35.15, 126.8), 12);
  });
});
