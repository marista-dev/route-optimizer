"""
optimizer.py
카카오 모빌리티 API + Cluster-First Route-Second 배송 순서 최적화

전략:
  1. K-Means 클러스터링으로 배송지를 적절한 군집으로 분할
  2. 출발지(사무실)에서 각 클러스터 중심까지 카카오 API 실제 주행시간으로
     가장 가까운 클러스터부터 Nearest Neighbor 순서 결정
  3. 각 클러스터 내부에서 OR-Tools TSP로 최적 경로 계산
     - IN 포인트: 이전 클러스터에서 넘어오는 지점
     - OUT 포인트: 다음 클러스터와 가장 가까운 지점
  4. 같은 건물/주소 노드 연속 배치 후처리

공개 API (app.py 호환):
  - build_time_matrix(nodes, headers, progress_cb, stop_event)
  - optimize_route(nodes, time_matrix)
  - clear_checkpoint()
"""

import json
import math
import os
import re
import threading
import time

import requests
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp

# ── 체크포인트 ────────────────────────────────────────────────────────────────
_CHECKPOINT_DIR = os.path.join(
    os.environ.get('APPDATA', os.path.expanduser('~')),
    'RouteOptimizer'
)
CHECKPOINT_FILE = os.path.join(_CHECKPOINT_DIR, 'time_matrix.json')


def _save_checkpoint(data: dict):
    os.makedirs(_CHECKPOINT_DIR, exist_ok=True)
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f)


def _load_checkpoint() -> dict | None:
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return None


def clear_checkpoint():
    """체크포인트 파일 삭제 (중단 또는 새 작업 시작 시 호출)."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            os.remove(CHECKPOINT_FILE)
        except Exception:
            pass


# ── 카카오 모빌리티 API ───────────────────────────────────────────────────────
def _get_driving_time(o_lon, o_lat, d_lon, d_lat, headers: dict) -> int:
    """두 지점 간 자동차 주행 시간(초) 반환. 실패 시 999_999."""
    url    = 'https://apis-navi.kakaomobility.com/v1/directions'
    params = {'origin':      f'{o_lon},{o_lat}',
              'destination': f'{d_lon},{d_lat}',
              'priority':    'RECOMMEND'}
    for _ in range(5):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=7)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('routes') and data['routes'][0]['result_code'] == 0:
                    return data['routes'][0]['summary']['duration']
            elif resp.status_code == 429:
                time.sleep(3)
                continue
        except Exception:
            time.sleep(1)
    return 999_999


# ── 거리 유틸 ─────────────────────────────────────────────────────────────────
def _haversine_km(lat1, lon1, lat2, lon2) -> float:
    """두 좌표 간 Haversine 직선 거리(km)."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ── 클러스터링 ────────────────────────────────────────────────────────────────
def _kmeans_cluster(coords: list, k: int, max_iter: int = 100) -> list:
    """
    순수 Python K-Means 구현 (scikit-learn 의존성 제거).

    Parameters
    ----------
    coords  : [(lat, lon), ...]  배송지 좌표 리스트
    k       : 클러스터 수
    max_iter: 최대 반복 횟수

    Returns
    -------
    labels : [int, ...]  각 좌표의 클러스터 레이블 (0 ~ k-1)
    """
    import random
    n = len(coords)
    if n <= k:
        return list(range(n))

    # K-Means++ 초기화
    centroids = [coords[random.randint(0, n - 1)]]
    for _ in range(1, k):
        dists = []
        for c in coords:
            min_d = min(_haversine_km(c[0], c[1], ct[0], ct[1])
                        for ct in centroids)
            dists.append(min_d ** 2)
        total = sum(dists)
        if total == 0:
            centroids.append(coords[random.randint(0, n - 1)])
            continue
        probs = [d / total for d in dists]
        r = random.random()
        cum = 0
        for idx, p in enumerate(probs):
            cum += p
            if cum >= r:
                centroids.append(coords[idx])
                break
        else:
            centroids.append(coords[-1])

    labels = [0] * n
    for _ in range(max_iter):
        # 할당
        new_labels = []
        for c in coords:
            best_j, best_d = 0, float('inf')
            for j, ct in enumerate(centroids):
                d = _haversine_km(c[0], c[1], ct[0], ct[1])
                if d < best_d:
                    best_d = d
                    best_j = j
            new_labels.append(best_j)

        if new_labels == labels:
            break
        labels = new_labels

        # 중심 갱신
        for j in range(k):
            members = [coords[i] for i in range(n) if labels[i] == j]
            if members:
                centroids[j] = (
                    sum(m[0] for m in members) / len(members),
                    sum(m[1] for m in members) / len(members),
                )

    return labels


def _determine_k(n_points: int) -> int:
    """배송지 수에 따른 적정 클러스터 수 결정."""
    if n_points <= 5:
        return 1
    if n_points <= 12:
        return 2
    if n_points <= 20:
        return 3
    if n_points <= 30:
        return 4
    return min(max(n_points // 6, 4), 8)


def _cluster_centroid(nodes: list, indices: list) -> tuple:
    """클러스터에 속한 노드들의 중심 좌표 반환."""
    lats = [nodes[i]['lat'] for i in indices]
    lons = [nodes[i]['lon'] for i in indices]
    return (sum(lats) / len(lats), sum(lons) / len(lons))


# ── 시간 행렬 구축 ────────────────────────────────────────────────────────────
def build_time_matrix(nodes: list, headers: dict,
                      progress_cb=None,
                      stop_event: threading.Event = None) -> list:
    """
    클러스터 기반 시간 행렬 구축.

    기존과 동일한 인터페이스 유지:
      nodes[0] = 출발지, nodes[1:] = 배송지
      반환: N×N 주행 시간 행렬

    최적화: 클러스터 내부 + 클러스터 간 연결점만 계산하여
           API 호출 수를 대폭 줄임.
    """
    n = len(nodes)
    if n <= 1:
        return [[0]]

    # 체크포인트 복구
    saved = _load_checkpoint()
    if saved and saved.get('n') == n:
        matrix = saved.get('matrix')
        if matrix and len(matrix) == n and len(matrix[0]) == n:
            # 이미 완성된 행렬인지 확인
            remaining = sum(1 for i in range(n) for j in range(n)
                            if i != j and matrix[i][j] is None)
            if remaining == 0:
                return matrix
    else:
        matrix = [[None] * n for _ in range(n)]
        for i in range(n):
            matrix[i][i] = 0

    # 배송지 좌표 (nodes[1:])
    delivery_nodes = list(range(1, n))
    coords = [(nodes[i]['lat'], nodes[i]['lon']) for i in delivery_nodes]

    # 클러스터링
    k = _determine_k(len(delivery_nodes))
    if k <= 1:
        # 클러스터 1개 = 전체 N×N 계산
        clusters = {0: delivery_nodes}
    else:
        labels = _kmeans_cluster(coords, k)
        clusters = {}
        for idx, label in enumerate(labels):
            clusters.setdefault(label, []).append(delivery_nodes[idx])

    # 필요한 쌍 목록 구성
    # 1) 출발지 → 각 클러스터 중심에 가장 가까운 노드 (최소 각 클러스터 1개)
    # 2) 클러스터 내부 모든 쌍
    # 3) 인접 클러스터 간 연결 후보 (경계 노드)
    pairs_needed = set()

    # 출발지(0) ↔ 모든 배송지
    for i in delivery_nodes:
        pairs_needed.add((0, i))
        pairs_needed.add((i, 0))

    # 클러스터 내부 모든 쌍
    for cid, members in clusters.items():
        for i in members:
            for j in members:
                if i != j:
                    pairs_needed.add((i, j))

    # 클러스터 간 경계 노드 (각 클러스터에서 다른 클러스터와 가장 가까운 3개씩)
    cluster_ids = sorted(clusters.keys())
    for ci in cluster_ids:
        for cj in cluster_ids:
            if ci == cj:
                continue
            # ci의 모든 노드 → cj의 모든 노드 중 거리가 가까운 쌍
            border_pairs = []
            for ni in clusters[ci]:
                for nj in clusters[cj]:
                    d = _haversine_km(nodes[ni]['lat'], nodes[ni]['lon'],
                                      nodes[nj]['lat'], nodes[nj]['lon'])
                    border_pairs.append((d, ni, nj))
            border_pairs.sort()
            # 상위 min(5, 전체) 쌍만 API 호출
            for _, ni, nj in border_pairs[:min(5, len(border_pairs))]:
                pairs_needed.add((ni, nj))
                pairs_needed.add((nj, ni))

    # 이미 계산된 쌍 제외
    pairs_todo = [(i, j) for (i, j) in pairs_needed
                  if matrix[i][j] is None]

    total = len(pairs_todo)
    done  = 0

    for i, j in pairs_todo:
        if stop_event and stop_event.is_set():
            _save_checkpoint({'n': n, 'matrix': matrix})
            return matrix

        if matrix[i][j] is None:
            matrix[i][j] = _get_driving_time(
                nodes[i]['lon'], nodes[i]['lat'],
                nodes[j]['lon'], nodes[j]['lat'],
                headers)
            time.sleep(0.15)
            done += 1

            if done % 10 == 0:
                _save_checkpoint({'n': n, 'matrix': matrix})
                if progress_cb:
                    progress_cb(done, total)

    # 아직 None인 셀은 Haversine 추정치로 채움 (직선거리 ÷ 40km/h)
    for i in range(n):
        for j in range(n):
            if i != j and matrix[i][j] is None:
                dist_km = _haversine_km(nodes[i]['lat'], nodes[i]['lon'],
                                         nodes[j]['lat'], nodes[j]['lon'])
                matrix[i][j] = int(dist_km / 40.0 * 3600)  # 초 단위

    _save_checkpoint({'n': n, 'matrix': matrix})
    return matrix


# ── 클러스터 순서 결정 ────────────────────────────────────────────────────────
def _order_clusters(nodes: list, clusters: dict, matrix: list) -> list:
    """
    출발지(nodes[0])에서 카카오 API 실제 주행시간 기준으로
    가장 가까운 클러스터부터 Nearest Neighbor 방식으로 순서 결정.

    Returns
    -------
    [(cluster_id, entry_node_idx), ...]  순서대로 정렬된 클러스터 목록
    """
    if len(clusters) == 1:
        cid = list(clusters.keys())[0]
        # 출발지에서 가장 가까운 노드를 entry로
        best_node = min(clusters[cid],
                        key=lambda ni: matrix[0][ni]
                        if matrix[0][ni] is not None else float('inf'))
        return [(cid, best_node)]

    remaining  = set(clusters.keys())
    ordered    = []
    current    = 0  # 현재 위치 = nodes[0] (출발지)

    while remaining:
        best_cid   = None
        best_node  = None
        best_time  = float('inf')

        for cid in remaining:
            for ni in clusters[cid]:
                t = matrix[current][ni]
                if t is not None and t < best_time:
                    best_time = t
                    best_cid  = cid
                    best_node = ni

        if best_cid is None:
            # fallback: Haversine 거리 기반
            for cid in remaining:
                clat, clon = _cluster_centroid(nodes, clusters[cid])
                d = _haversine_km(nodes[current]['lat'], nodes[current]['lon'],
                                   clat, clon)
                if d < best_time:
                    best_time = d
                    best_cid  = cid
                    best_node = clusters[cid][0]

        ordered.append((best_cid, best_node))
        remaining.discard(best_cid)

        # 다음 클러스터 탐색 시 "현재 위치"를 이 클러스터의 중심 노드로 이동
        # → 실제로는 클러스터 내 TSP 후 exit 노드가 되지만,
        #   순서 결정 단계에서는 중심에 가장 가까운 노드 사용
        centroid = _cluster_centroid(nodes, clusters[best_cid])
        best_exit = min(clusters[best_cid],
                        key=lambda ni: _haversine_km(
                            nodes[ni]['lat'], nodes[ni]['lon'],
                            centroid[0], centroid[1]))
        current = best_exit

    return ordered


# ── 클러스터 내 TSP ───────────────────────────────────────────────────────────
def _solve_cluster_tsp(member_indices: list,
                       entry_node: int,
                       exit_node: int | None,
                       nodes: list,
                       matrix: list) -> list:
    """
    클러스터 내부 노드의 최적 순서를 OR-Tools로 계산.

    Parameters
    ----------
    member_indices : 이 클러스터에 속한 전역 노드 인덱스 리스트
    entry_node     : 진입 노드 (전역 인덱스) — 반드시 첫 번째
    exit_node      : 퇴장 노드 (전역 인덱스) — 있으면 반드시 마지막. None이면 자유
    nodes          : 전체 노드 리스트
    matrix         : 전체 시간 행렬

    Returns
    -------
    member_indices를 최적 순서로 재배열한 리스트
    """
    members = list(member_indices)
    n = len(members)

    if n <= 1:
        return members
    if n == 2:
        if exit_node is not None and members[0] == exit_node:
            members.reverse()
        elif entry_node is not None and members[1] == entry_node:
            members.reverse()
        return members

    # entry를 맨 앞으로
    if entry_node in members:
        members.remove(entry_node)
        members.insert(0, entry_node)

    # 로컬 인덱스 ↔ 전역 인덱스 매핑
    local_to_global = members
    global_to_local = {g: l for l, g in enumerate(local_to_global)}

    # 로컬 시간 행렬 구축
    local_matrix = []
    for i in range(n):
        row = []
        for j in range(n):
            gi, gj = local_to_global[i], local_to_global[j]
            t = matrix[gi][gj]
            if t is None:
                t = int(_haversine_km(nodes[gi]['lat'], nodes[gi]['lon'],
                                       nodes[gj]['lat'], nodes[gj]['lon'])
                        / 40.0 * 3600)
            row.append(t)
        local_matrix.append(row)

    # Open TSP: entry(0) → ... → exit(dummy)
    start_idx = 0  # entry는 항상 local index 0
    dummy = n

    ext = [[0] * (n + 1) for _ in range(n + 1)]
    for i in range(n):
        for j in range(n):
            ext[i][j] = local_matrix[i][j]
        ext[i][dummy] = 0

    # exit_node가 지정된 경우: exit으로 끝나도록 유도
    if exit_node is not None and exit_node in global_to_local:
        exit_local = global_to_local[exit_node]
        # exit → dummy 비용 = 0, 나머지 → dummy 비용 = 큰 페널티
        for i in range(n):
            if i == exit_local:
                ext[i][dummy] = 0
            else:
                ext[i][dummy] = 999_999

    manager = pywrapcp.RoutingIndexManager(n + 1, 1, [start_idx], [dummy])
    routing = pywrapcp.RoutingModel(manager)

    def _cost(fi, ti):
        return ext[manager.IndexToNode(fi)][manager.IndexToNode(ti)]

    cb = routing.RegisterTransitCallback(_cost)
    routing.SetArcCostEvaluatorOfAllVehicles(cb)

    params = pywrapcp.DefaultRoutingSearchParameters()
    params.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC)
    params.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH)
    # 클러스터 내부는 소규모이므로 시간 제한 짧게
    params.time_limit.seconds = max(10, n * 3)

    sol = routing.SolveWithParameters(params)
    if not sol:
        # fallback: Nearest Neighbor
        return _nearest_neighbor_order(members, local_matrix)

    order = []
    idx = routing.Start(0)
    while not routing.IsEnd(idx):
        node = manager.IndexToNode(idx)
        if node != dummy and node < n:
            order.append(local_to_global[node])
        idx = sol.Value(routing.NextVar(idx))

    return order


def _nearest_neighbor_order(members: list, local_matrix: list) -> list:
    """Nearest Neighbor fallback."""
    n = len(members)
    visited = [False] * n
    order   = [0]
    visited[0] = True

    for _ in range(n - 1):
        cur  = order[-1]
        best = None
        best_t = float('inf')
        for j in range(n):
            if not visited[j] and local_matrix[cur][j] < best_t:
                best_t = local_matrix[cur][j]
                best   = j
        if best is None:
            break
        order.append(best)
        visited[best] = True

    return [members[i] for i in order]


# ── exit 포인트 결정 ──────────────────────────────────────────────────────────
def _find_exit_node(cluster_members: list, next_cluster_members: list,
                    nodes: list, matrix: list) -> int | None:
    """
    현재 클러스터에서 다음 클러스터와 가장 가까운(주행시간 기준) 노드를 찾아
    exit 포인트로 반환.
    """
    if not next_cluster_members:
        return None

    best_node = None
    best_time = float('inf')

    for ni in cluster_members:
        for nj in next_cluster_members:
            t = matrix[ni][nj]
            if t is None:
                t = int(_haversine_km(nodes[ni]['lat'], nodes[ni]['lon'],
                                       nodes[nj]['lat'], nodes[nj]['lon'])
                        / 40.0 * 3600)
            if t < best_time:
                best_time = t
                best_node = ni

    return best_node


# ── 같은 위치 그룹핑 ─────────────────────────────────────────────────────────
def _strip_unit(address: str) -> str:
    """동/호/층 번호를 제거한 기본 주소 반환 (같은 건물 단위 비교용)."""
    s = re.sub(r'\d+동\s*\d+호|\d+층.*|\d+호', '', address)
    s = re.sub(r'\s+', ' ', s).strip().lower()
    return s


def _group_same_location(order: list, nodes: list) -> list:
    """
    최적화 결과에서 동일 좌표(같은 건물/주소) 노드를 연속 배치하는 후처리.

    기준:
    1. 동일 좌표 (소수점 5자리 ≈ 1m 이내)
    2. 동일 기본 주소 (동호수 제거 후 비교)

    효과: 같은 건물에 호수만 다른 주소가 흩어져 있어도 연속 배치됨.
    """
    def _coord_key(ni: int) -> tuple:
        n = nodes[ni]
        return (round(n['lat'], 5), round(n['lon'], 5))

    def _addr_key(ni: int) -> str:
        addr = nodes[ni].get('address', '')
        return _strip_unit(addr) if addr else ''

    result     = []
    coord_seen = {}  # coord_key → result 내 마지막 삽입 위치
    addr_seen  = {}  # addr_key  → result 내 마지막 삽입 위치

    for ni in order:
        ck = _coord_key(ni)
        ak = _addr_key(ni)

        insert_at = None
        if ck in coord_seen:
            insert_at = coord_seen[ck] + 1
        elif ak and ak in addr_seen:
            insert_at = addr_seen[ak] + 1

        if insert_at is not None:
            result.insert(insert_at, ni)
            # 삽입 위치 이후의 모든 인덱스를 +1 보정
            for d in (coord_seen, addr_seen):
                for k in d:
                    if d[k] >= insert_at:
                        d[k] += 1
            coord_seen[ck] = insert_at
            if ak:
                addr_seen[ak] = insert_at
        else:
            pos = len(result)
            result.append(ni)
            coord_seen[ck] = pos
            if ak:
                addr_seen[ak] = pos

    return result  # ← 기존 버그(`return order`) 수정됨


# ── 메인 최적화 ──────────────────────────────────────────────────────────────
def optimize_route(nodes: list, time_matrix: list):
    """
    Cluster-First, Route-Second 배송 순서 최적화.

    Parameters
    ----------
    nodes       : [{'name', 'lat', 'lon', 'id', 'address'(선택)}, ...]
                  nodes[0] = 출발지 (사무실)
    time_matrix : N×N 주행 시간 행렬 (build_time_matrix 결과)

    Returns
    -------
    배송 순서대로 정렬된 node index 리스트 (출발지 제외) or None

    알고리즘:
    1. 배송지를 K-Means 클러스터링
    2. 출발지에서 카카오 API 주행시간 기준 Nearest Neighbor로 클러스터 순서 결정
    3. 각 클러스터 내에서:
       - entry: 이전 클러스터의 exit에서 가장 가까운 이 클러스터 노드
       - exit:  다음 클러스터의 entry 후보 중 가장 가까운 이 클러스터 노드
       - OR-Tools TSP로 entry → ... → exit 경로 계산
    4. 같은 건물/주소 노드 연속 배치 후처리
    """
    n = len(nodes)
    if n <= 1:
        return None
    if n == 2:
        return [1]

    delivery_nodes = list(range(1, n))

    # ── 1) 클러스터링 ────────────────────────────────────────────────────────
    coords = [(nodes[i]['lat'], nodes[i]['lon']) for i in delivery_nodes]
    k = _determine_k(len(delivery_nodes))

    if k <= 1:
        clusters = {0: delivery_nodes}
    else:
        labels = _kmeans_cluster(coords, k)
        clusters = {}
        for idx, label in enumerate(labels):
            clusters.setdefault(label, []).append(delivery_nodes[idx])

    # ── 2) 클러스터 순서 결정 (카카오 API 주행시간 기준) ─────────────────────
    cluster_order = _order_clusters(nodes, clusters, time_matrix)

    # ── 3) 각 클러스터 내 TSP ────────────────────────────────────────────────
    final_order = []

    for step, (cid, entry_node) in enumerate(cluster_order):
        members = clusters[cid]

        # exit 포인트: 다음 클러스터가 있으면 가장 가까운 노드
        if step + 1 < len(cluster_order):
            next_cid = cluster_order[step + 1][0]
            next_members = clusters[next_cid]
            exit_node = _find_exit_node(members, next_members,
                                         nodes, time_matrix)
        else:
            exit_node = None  # 마지막 클러스터는 자유 종료

        # 이전 클러스터의 마지막 노드에서 가장 가까운 entry 재계산
        if final_order:
            last_node = final_order[-1]
            best_entry = min(
                members,
                key=lambda ni: time_matrix[last_node][ni]
                if time_matrix[last_node][ni] is not None
                else float('inf'))
            entry_node = best_entry

        # 클러스터 내 TSP 풀기
        cluster_route = _solve_cluster_tsp(
            members, entry_node, exit_node, nodes, time_matrix)

        final_order.extend(cluster_route)

    # ── 4) 같은 위치 그룹핑 후처리 ──────────────────────────────────────────
    final_order = _group_same_location(final_order, nodes)

    return final_order
