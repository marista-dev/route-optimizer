"""
optimizer.py
카카오 모빌리티 API + Cluster-First Route-Second 배송 순서 최적화

전략:
  4단계 (build_time_matrix):
    1. K-Means 클러스터링으로 배송지를 군집 분할
    2. 같은 건물/주소 그룹핑 → 대표 노드 추출
    3. 클러스터 중심 좌표끼리 카카오 API → 클러스터 순서 확정
       (출발 창고 → 가장 가까운 중심 = 1번, 1번 → 2번, ...)
    4. 출발지↔대표, 대표↔대표 클러스터 내부 쌍 카카오 API 호출
    5. 클러스터 간 경계 대표 노드 쌍 카카오 API 호출 (IN/OUT 결정용)
    6. 결과를 모듈 변수에 저장 → 5단계에서 그대로 사용

  5단계 (optimize_route):
    1. 4단계 결과 그대로 사용 (클러스터, 순서, 그룹핑 전부)
    2. 각 클러스터 내 IN/OUT 결정
    3. OR-Tools TSP로 IN → OUT 흐름 최적화 (대표 노드만 참여)
    4. 대표 노드 뒤에 같은 건물 멤버 펼침

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

# ── 4단계→5단계 전달용 모듈 변수 ─────────────────────────────────────────────
_last_clusters       = None  # {cluster_id: [node_indices, ...]}
_last_cluster_order  = None  # [cluster_id, ...]  순서대로
_last_location_groups = None  # {대표노드: [대표, 멤버1, ...], ...}
_last_rep_nodes      = None  # [대표노드 인덱스, ...]

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
    global _last_clusters, _last_cluster_order
    global _last_location_groups, _last_rep_nodes
    _last_clusters = None
    _last_cluster_order = None
    _last_location_groups = None
    _last_rep_nodes = None
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


# ── 같은 위치 그룹핑 ─────────────────────────────────────────────────────────
def _strip_unit(address: str) -> str:
    """동/호/층 번호를 제거한 기본 주소 반환 (같은 건물 단위 비교용)."""
    s = re.sub(r'\d+동\s*\d+호|\d+층.*|\d+호', '', address)
    s = re.sub(r'\s+', ' ', s).strip().lower()
    return s


def _build_location_groups(node_indices: list, nodes: list) -> dict:
    """
    같은 건물/주소 노드를 그룹으로 묶는다.

    기준:
    1. 동일 좌표 (소수점 5자리 ≈ 1m 이내)
    2. 동일 기본 주소 (동호수 제거 후 비교)

    Returns: {대표노드: [대표, 멤버1, 멤버2, ...], ...}
    """
    def _coord_key(ni):
        n = nodes[ni]
        return (round(n['lat'], 5), round(n['lon'], 5))

    def _addr_key(ni):
        addr = nodes[ni].get('address', '')
        return _strip_unit(addr) if addr else ''

    coord_groups = {}
    addr_groups  = {}
    assigned     = {}

    for ni in node_indices:
        if ni in assigned:
            continue
        ck = _coord_key(ni)
        ak = _addr_key(ni)
        if ck in coord_groups:
            assigned[ni] = coord_groups[ck]
            continue
        if ak and ak in addr_groups:
            assigned[ni] = addr_groups[ak]
            continue
        assigned[ni] = ni
        coord_groups[ck] = ni
        if ak:
            addr_groups[ak] = ni

    groups = {}
    for ni in node_indices:
        rep = assigned[ni]
        groups.setdefault(rep, []).append(ni)
    return groups


# ── 클러스터링 ────────────────────────────────────────────────────────────────
def _kmeans_cluster(coords: list, k: int, max_iter: int = 100) -> list:
    """순수 Python K-Means++ 구현."""
    import random
    n = len(coords)
    if n <= k:
        return list(range(n))

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


# ── 시간 행렬 구축 (4단계) ────────────────────────────────────────────────────
def build_time_matrix(nodes: list, headers: dict,
                      progress_cb=None,
                      stop_event: threading.Event = None) -> list:
    """
    4단계: 클러스터링 → 그룹핑 → 클러스터 순서 → API 호출.

    실행 순서:
    1. K-Means 클러스터링
    2. 같은 건물 그룹핑 → 대표 노드 추출
    3. 클러스터 중심 좌표끼리 카카오 API → 클러스터 순서 확정
    4. 출발지↔대표, 클러스터 내부 대표끼리 쌍 카카오 API 호출
    5. 클러스터 간 경계 대표 노드 쌍 카카오 API 호출
    6. 모듈 변수에 저장 → 5단계 전달
    """
    global _last_clusters, _last_cluster_order
    global _last_location_groups, _last_rep_nodes

    n = len(nodes)
    if n <= 1:
        return [[0]]

    # 체크포인트 복구
    saved = _load_checkpoint()
    if saved and saved.get('n') == n:
        matrix = saved.get('matrix')
        if matrix and len(matrix) == n and len(matrix[0]) == n:
            remaining = sum(1 for i in range(n) for j in range(n)
                            if i != j and matrix[i][j] is None)
            if remaining == 0:
                # 클러스터 정보도 복구
                if (saved.get('clusters') and saved.get('cluster_order')
                        and saved.get('location_groups')
                        and saved.get('rep_nodes')):
                    _last_clusters = {int(k): v
                                      for k, v in saved['clusters'].items()}
                    _last_cluster_order = saved['cluster_order']
                    _last_location_groups = {int(k): v
                                             for k, v in saved['location_groups'].items()}
                    _last_rep_nodes = saved['rep_nodes']
                return matrix
    else:
        matrix = [[None] * n for _ in range(n)]
        for i in range(n):
            matrix[i][i] = 0

    delivery_nodes = list(range(1, n))

    # ── 1) K-Means 클러스터링 ────────────────────────────────────────────────
    coords = [(nodes[i]['lat'], nodes[i]['lon']) for i in delivery_nodes]
    k = _determine_k(len(delivery_nodes))

    if k <= 1:
        clusters = {0: delivery_nodes}
    else:
        labels = _kmeans_cluster(coords, k)
        clusters = {}
        for idx, label in enumerate(labels):
            clusters.setdefault(label, []).append(delivery_nodes[idx])

    _last_clusters = clusters

    if stop_event and stop_event.is_set():
        _save_checkpoint({'n': n, 'matrix': matrix})
        return matrix

    # ── 2) 같은 건물 그룹핑 → 대표 노드 추출 ────────────────────────────────
    location_groups = _build_location_groups(delivery_nodes, nodes)
    rep_nodes = list(location_groups.keys())

    _last_location_groups = location_groups
    _last_rep_nodes = rep_nodes

    # 대표 노드가 어느 클러스터에 속하는지 매핑
    node_to_cluster = {}
    for cid, members in clusters.items():
        for ni in members:
            node_to_cluster[ni] = cid

    # 클러스터별 대표 노드 목록
    cluster_reps = {}
    for rep in rep_nodes:
        cid = node_to_cluster.get(rep)
        if cid is not None:
            cluster_reps.setdefault(cid, []).append(rep)

    # ── 3) 클러스터 중심 좌표끼리 카카오 API → 순서 확정 ─────────────────────
    if len(clusters) <= 1:
        cluster_order = list(clusters.keys())
    else:
        centroids = {}
        for cid, members in clusters.items():
            centroids[cid] = _cluster_centroid(nodes, members)

        remaining_c = set(clusters.keys())
        cluster_order = []
        cur_lat, cur_lon = nodes[0]['lat'], nodes[0]['lon']

        while remaining_c:
            if stop_event and stop_event.is_set():
                cluster_order.extend(remaining_c)
                break

            best_cid  = None
            best_time = float('inf')

            for cid in remaining_c:
                clat, clon = centroids[cid]
                t = _get_driving_time(cur_lon, cur_lat, clon, clat, headers)
                time.sleep(0.15)
                if t < best_time:
                    best_time = t
                    best_cid  = cid

            if best_cid is None:
                # fallback: Haversine
                for cid in remaining_c:
                    clat, clon = centroids[cid]
                    d = _haversine_km(cur_lat, cur_lon, clat, clon)
                    if d < best_time:
                        best_time = d
                        best_cid  = cid

            cluster_order.append(best_cid)
            remaining_c.discard(best_cid)
            cur_lat, cur_lon = centroids[best_cid]

    _last_cluster_order = cluster_order

    if stop_event and stop_event.is_set():
        _save_checkpoint({'n': n, 'matrix': matrix})
        return matrix

    # ── 4) API 호출: 출발지↔대표, 클러스터 내부 대표끼리 ────────────────────
    pairs_needed = set()

    # 출발지(0) ↔ 모든 대표 노드
    for rep in rep_nodes:
        pairs_needed.add((0, rep))
        pairs_needed.add((rep, 0))

    # 클러스터 내부 대표끼리 모든 쌍
    for cid, reps in cluster_reps.items():
        for i in reps:
            for j in reps:
                if i != j:
                    pairs_needed.add((i, j))

    # ── 5) 클러스터 간 경계 대표 노드 쌍 (IN/OUT 결정용) ────────────────────
    cluster_ids = sorted(clusters.keys())
    for ci in cluster_ids:
        reps_ci = cluster_reps.get(ci, [])
        for cj in cluster_ids:
            if ci == cj:
                continue
            reps_cj = cluster_reps.get(cj, [])
            border_pairs = []
            for ni in reps_ci:
                for nj in reps_cj:
                    d = _haversine_km(nodes[ni]['lat'], nodes[ni]['lon'],
                                      nodes[nj]['lat'], nodes[nj]['lon'])
                    border_pairs.append((d, ni, nj))
            border_pairs.sort()
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

    # 아직 None인 셀 = Haversine 추정치 (직선거리 ÷ 40km/h)
    for i in range(n):
        for j in range(n):
            if i != j and matrix[i][j] is None:
                dist_km = _haversine_km(nodes[i]['lat'], nodes[i]['lon'],
                                         nodes[j]['lat'], nodes[j]['lon'])
                matrix[i][j] = int(dist_km / 40.0 * 3600)

    # ── 6) 모듈 변수 + 체크포인트 저장 ──────────────────────────────────────
    _save_checkpoint({
        'n': n,
        'matrix': matrix,
        'clusters': {str(k): v for k, v in clusters.items()},
        'cluster_order': cluster_order,
        'location_groups': {str(k): v for k, v in location_groups.items()},
        'rep_nodes': rep_nodes,
    })

    return matrix


# ── 클러스터 내 TSP ───────────────────────────────────────────────────────────
def _solve_cluster_tsp(rep_indices: list,
                       entry_node: int,
                       exit_node: int | None,
                       nodes: list,
                       matrix: list) -> list:
    """
    클러스터 내부의 대표 노드들의 최적 순서를 OR-Tools로 계산.
    entry_node = 반드시 첫 번째, exit_node = 반드시 마지막 (None이면 자유).
    """
    members = list(rep_indices)
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

    local_to_global = members
    global_to_local = {g: l for l, g in enumerate(local_to_global)}

    # 로컬 시간 행렬
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

    # Open TSP
    start_idx = 0
    dummy = n
    ext = [[0] * (n + 1) for _ in range(n + 1)]
    for i in range(n):
        for j in range(n):
            ext[i][j] = local_matrix[i][j]
        ext[i][dummy] = 0

    if exit_node is not None and exit_node in global_to_local:
        exit_local = global_to_local[exit_node]
        for i in range(n):
            ext[i][dummy] = 999_999 if i != exit_local else 0

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
    params.time_limit.seconds = max(10, n * 3)

    sol = routing.SolveWithParameters(params)
    if not sol:
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
def _find_exit_node(cluster_reps: list, next_cluster_reps: list,
                    nodes: list, matrix: list) -> int | None:
    """현재 클러스터 대표 중 다음 클러스터와 가장 가까운 노드 반환."""
    if not next_cluster_reps:
        return None
    best_node = None
    best_time = float('inf')
    for ni in cluster_reps:
        for nj in next_cluster_reps:
            t = matrix[ni][nj]
            if t is None:
                t = int(_haversine_km(nodes[ni]['lat'], nodes[ni]['lon'],
                                       nodes[nj]['lat'], nodes[nj]['lon'])
                        / 40.0 * 3600)
            if t < best_time:
                best_time = t
                best_node = ni
    return best_node


# ── 메인 최적화 (5단계) ──────────────────────────────────────────────────────
def optimize_route(nodes: list, time_matrix: list):
    """
    5단계: 4단계 결과를 그대로 사용하여 배송 순서 최적화.

    실행 순서:
    1. 4단계에서 저장한 클러스터, 순서, 그룹핑 결과 로드
    2. 각 클러스터 내 IN/OUT 결정
    3. OR-Tools TSP (대표 노드만, IN→OUT 고정)
    4. 대표 순서대로 같은 건물 멤버 펼침
    """
    global _last_clusters, _last_cluster_order
    global _last_location_groups, _last_rep_nodes

    n = len(nodes)
    if n <= 1:
        return None
    if n == 2:
        return [1]

    delivery_nodes = list(range(1, n))

    # ── 4단계 결과 로드 ──────────────────────────────────────────────────────
    clusters       = _last_clusters
    cluster_order  = _last_cluster_order
    location_groups = _last_location_groups
    rep_nodes      = _last_rep_nodes

    # 4단계 결과가 없으면 (직접 호출된 경우) 여기서 계산
    if clusters is None or cluster_order is None:
        coords = [(nodes[i]['lat'], nodes[i]['lon']) for i in delivery_nodes]
        k = _determine_k(len(delivery_nodes))
        if k <= 1:
            clusters = {0: delivery_nodes}
        else:
            labels = _kmeans_cluster(coords, k)
            clusters = {}
            for idx, label in enumerate(labels):
                clusters.setdefault(label, []).append(delivery_nodes[idx])
        cluster_order = list(clusters.keys())

    if location_groups is None:
        location_groups = _build_location_groups(delivery_nodes, nodes)
        rep_nodes = list(location_groups.keys())

    # 대표 노드가 어느 클러스터에 속하는지 매핑
    node_to_cluster = {}
    for cid, members in clusters.items():
        for ni in members:
            node_to_cluster[ni] = cid

    # 클러스터별 대표 노드 목록
    cluster_reps = {}
    for rep in rep_nodes:
        cid = node_to_cluster.get(rep)
        if cid is not None:
            cluster_reps.setdefault(cid, []).append(rep)

    # ── 클러스터 순서대로 TSP 풀기 ───────────────────────────────────────────
    rep_order = []

    for step, cid in enumerate(cluster_order):
        reps = cluster_reps.get(cid, [])
        if not reps:
            continue

        # IN: 이전 클러스터의 마지막 대표에서 가장 가까운 이 클러스터 대표
        if rep_order:
            last_rep = rep_order[-1]
            entry_node = min(
                reps,
                key=lambda ni: time_matrix[last_rep][ni]
                if time_matrix[last_rep][ni] is not None
                else float('inf'))
        else:
            # 첫 클러스터: 출발지에서 가장 가까운 대표
            entry_node = min(
                reps,
                key=lambda ni: time_matrix[0][ni]
                if time_matrix[0][ni] is not None
                else float('inf'))

        # OUT: 다음 클러스터 대표와 가장 가까운 이 클러스터 대표
        if step + 1 < len(cluster_order):
            next_cid = cluster_order[step + 1]
            next_reps = cluster_reps.get(next_cid, [])
            exit_node = _find_exit_node(reps, next_reps,
                                         nodes, time_matrix)
        else:
            exit_node = None  # 마지막 클러스터는 자유 종료

        # 클러스터 내 TSP (대표만, IN→OUT 고정)
        cluster_route = _solve_cluster_tsp(
            reps, entry_node, exit_node, nodes, time_matrix)

        rep_order.extend(cluster_route)

    # ── 대표 순서대로 같은 건물 멤버 펼침 ────────────────────────────────────
    final_order = []
    for rep in rep_order:
        group_members = location_groups.get(rep, [rep])
        for member in group_members:
            if member not in final_order:
                final_order.append(member)

    return final_order
