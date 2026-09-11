/**
 * grouping.ts — `src/core/optimizer.py`의 `_build_location_groups`(1차) /
 * `_build_secondary_clusters`(2차) 이식.
 */

import type { Cluster, LatLng, Node, PrimaryGroup } from '../types';
import { complexKey } from './address';
import { haversineKm } from './geo';
import { bufferedHull, hullRadiusM } from './hull';

/**
 * 좌표 그룹핑 키 — 소수점 5자리(≈1m).
 *
 * Python은 `(round(lat, 5), round(lon, 5))` 튜플을 썼다. JS `toFixed(5)`는
 * 십진 반올림, Python `round`는 짝수 반올림이라 정확히 중간값일 때만 갈릴 수
 * 있는데, 위경도 실수에서는 사실상 나오지 않는 경우다.
 */
function coordKey(lat: number, lon: number): string {
  return `${lat.toFixed(5)},${lon.toFixed(5)}`;
}

/**
 * 같은 건물/주소 노드를 1차 그룹으로 묶는다.
 *
 * 기준(Python과 동일):
 *   1. 동일 좌표 (소수점 5자리 ≈ 1m 이내)
 *   2. 동일 기본 주소 (동호수 제거 후 비교 = `complexKey`)
 *
 * 좌표가 없는 노드(`lat`/`lon`이 null)는 제외한다.
 * 대표는 그룹을 처음 만든 노드이며, 그룹 id는 대표가 등장한 순서(0부터)라
 * 같은 입력이면 항상 같은 결과가 나온다.
 */
export function buildPrimaryGroups(nodes: Node[]): PrimaryGroup[] {
  const usable = nodes.filter(
    (n): n is Node & { lat: number; lon: number } => n.lat !== null && n.lon !== null,
  );

  const coordGroups = new Map<string, number>(); // 좌표키 → 대표 노드 id
  const addrGroups = new Map<string, number>();  // 단지키 → 대표 노드 id
  const assigned = new Map<number, number>();    // 노드 id → 대표 노드 id
  const repOrder: number[] = [];
  const repAddrKey = new Map<number, string>();

  for (const n of usable) {
    const ck = coordKey(n.lat, n.lon);
    const ak = complexKey(n.address ?? '');

    const byCoord = coordGroups.get(ck);
    if (byCoord !== undefined) {
      assigned.set(n.id, byCoord);
      continue;
    }
    const byAddr = ak ? addrGroups.get(ak) : undefined;
    if (byAddr !== undefined) {
      assigned.set(n.id, byAddr);
      continue;
    }

    assigned.set(n.id, n.id);
    coordGroups.set(ck, n.id);
    if (ak) addrGroups.set(ak, n.id);
    repOrder.push(n.id);
    repAddrKey.set(n.id, ak);
  }

  const membersByRep = new Map<number, number[]>();
  const nodeById = new Map<number, Node>();
  for (const n of usable) {
    nodeById.set(n.id, n);
    const rep = assigned.get(n.id) as number;
    const list = membersByRep.get(rep);
    if (list) list.push(n.id);
    else membersByRep.set(rep, [n.id]);
  }

  return repOrder.map((rep, id) => {
    const repNode = nodeById.get(rep) as Node & { lat: number; lon: number };
    return {
      id,
      rep,
      members: membersByRep.get(rep) as number[],
      complexKey: repAddrKey.get(rep) ?? '',
      lat: repNode.lat,
      lon: repNode.lon,
    };
  });
}

/**
 * 1차 그룹을 Haversine 거리가 `thresholdM` 이내면 같은 클러스터로 묶는다
 * (Union-Find).
 *
 * 예: A↔B 80m, B↔C 80m, A↔C 150m → 임계값 100m이면 A,B,C 모두 같은 클러스터
 *     (transitive closure)
 *
 * 클러스터 root는 멤버 중 가장 작은 그룹 id(결정적)이고, 클러스터 id는 첫 멤버가
 * 등장한 순서(0부터)다. 중심점과 다각형(`hull`)도 함께 계산한다.
 */
export function buildClusters(groups: PrimaryGroup[], thresholdM: number): Cluster[] {
  if (groups.length === 0) return [];

  const thresholdKm = thresholdM / 1000.0;
  const parent = new Map<number, number>();
  for (const g of groups) parent.set(g.id, g.id);

  const find = (x: number): number => {
    let cur = x;
    while ((parent.get(cur) as number) !== cur) {
      const p = parent.get(cur) as number;
      parent.set(cur, parent.get(p) as number); // path compression
      cur = parent.get(cur) as number;
    }
    return cur;
  };

  const union = (a: number, b: number): void => {
    const ra = find(a);
    const rb = find(b);
    if (ra === rb) return;
    // 작은 id를 root로 (결정성 확보)
    if (ra < rb) parent.set(rb, ra);
    else parent.set(ra, rb);
  };

  for (let i = 0; i < groups.length; i++) {
    const a = groups[i];
    for (let j = i + 1; j < groups.length; j++) {
      const b = groups[j];
      const d = haversineKm(a.lat, a.lon, b.lat, b.lon);
      if (d <= thresholdKm) union(a.id, b.id);
    }
  }

  const membersByRoot = new Map<number, number[]>();
  const rootOrder: number[] = [];
  for (const g of groups) {
    const root = find(g.id);
    const list = membersByRoot.get(root);
    if (list) {
      list.push(g.id);
    } else {
      membersByRoot.set(root, [g.id]);
      rootOrder.push(root);
    }
  }

  const groupById = new Map<number, PrimaryGroup>();
  for (const g of groups) groupById.set(g.id, g);

  return rootOrder.map((root, id) => {
    const groupIds = membersByRoot.get(root) as number[];
    const points: LatLng[] = groupIds.map((gid) => {
      const g = groupById.get(gid) as PrimaryGroup;
      return { lat: g.lat, lon: g.lon };
    });
    const centroid: LatLng = {
      lat: points.reduce((s, p) => s + p.lat, 0) / points.length,
      lon: points.reduce((s, p) => s + p.lon, 0) / points.length,
    };
    // 작은 클러스터는 반경을 키워 클릭 히트 영역을 확보한다(hullRadiusM).
    return { id, groupIds, centroid, hull: bufferedHull(points, hullRadiusM(points)) };
  });
}
