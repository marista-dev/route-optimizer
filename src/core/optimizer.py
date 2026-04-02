"""
optimizer.py
카카오 모빌리티 API + Sweep-Nearest 기반 배송 순서 최적화

전략:
  4단계 (build_time_matrix):
    1. Distance-based Sweep Nearest (DSN) 클러스터링
       - 출발 창고 기준 극좌표 각도(θ) 계산
       - 가장 먼 노드부터 시작, Nearest Neighbor로 클러스터 채움
       - 용량(max_per_cluster) 초과 시 다음 각도 방향으로 새 클러스터
       → 구조적으로 시계 방향 순서 보장, 왔다갔다 불가능
    2. 클러스터 순서 = 각도 순서 (API 호출 불필요)
    3. 클러스터 + 순서를 모듈 변수에 저장 → 5단계 전달

  5단계 (optimize_route):
    1. 같은 건물/주소 그룹핑 → 대표 노드 추출
    2. 대표 노드끼리 카카오 API 호출 (클러스터 내부 + 경계)
    3. IN / OUT 결정
    4. OR-Tools TSP로 IN → OUT 흐름 최적화 (대표 노드만 참여)
    5. 대표 노드 뒤에 같은 건물 멤버 펼침

공개 API (app.py 호환):
  - build_time_matrix(nodes, headers, progress_cb, stop_event, log_cb)
  - optimize_route(nodes, time_matrix, headers, log_cb)
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
_last_clusters      = None  # {cluster_id: [node_indices, ...]}
_last_cluster_order = None  # [cluster_id, ...]  순서대로

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
    _last_clusters = None
    _last_cluster_order = None
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


def _polar_angle(depot_lat, depot_lon, node_lat, node_lon) -> float:
    """출발 창고 기준 극좌표 각도(0~360도) 반환."""
    dy = node_lat - depot_lat
    dx = node_lon - depot_lon
    angle = math.degrees(math.atan2(dy, dx))
    if angle < 0:
        angle += 360.0
    return angle


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


# ── Distance-based Sweep Nearest (DSN) 클러스터링 ────────────────────────────
def _sweep_nearest_cluster(nodes: list, delivery_nodes: list,
                           max_per_cluster: int = 30) -> dict:
    """
    DSN (Distance-based Sweep Nearest) 클러스터링.

    Sweep + Nearest Neighbor 하이브리드:
    1. 출발 창고 기준 극좌표 각도(θ)를 계산하여 정렬
    2. 가장 먼 노드(depot에서 멀리 있는)부터 클러스터 시작 (DSN 특성)
    3. 클러스터 내에서는 Nearest Neighbor로 가까운 노드부터 채움
    4. max_per_cluster 초과하면 다음 각도 방향으로 새 클러스터

    효과:
    - 시계 방향 순서가 구조적으로 보장
    - 같은 방향인데 먼 노드가 다른 클러스터에 섞이지 않음
    - K-Means의 "경계 점프" 문제 없음

    Parameters
    ----------
    nodes           : 전체 노드 리스트 (nodes[0] = 출발지)
    delivery_nodes  : 배송지 인덱스 리스트
    max_per_cluster : 클러스터 당 최대 노드 수

    Returns
    -------
    {cluster_id: [node_indices, ...], ...}
    """
    if len(delivery_nodes) <= max_per_cluster:
        return {0: list(delivery_nodes)}

    depot_lat = nodes[0]['lat']
    depot_lon = nodes[0]['lon']

    # 1) 각 배송지의 극좌표 각도 + depot으로부터 거리 계산
    node_info = []
    for ni in delivery_nodes:
        angle = _polar_angle(depot_lat, depot_lon,
                             nodes[ni]['lat'], nodes[ni]['lon'])
        dist  = _haversine_km(depot_lat, depot_lon,
                              nodes[ni]['lat'], nodes[ni]['lon'])
        node_info.append((ni, angle, dist))

    # 2) 각도 순 정렬 (시계 방향 기본 순서)
    node_info.sort(key=lambda x: x[1])

    # 3) DSN: 각도 순서대로 순회하면서 Nearest Neighbor로 클러스터 채움
    clusters = {}
    assigned = set()
    cluster_id = 0

    # 시작점: 가장 먼 미할당 노드의 각도 위치부터
    # (먼 곳부터 잡아야 나중에 먼 노드가 가까운 클러스터에 억지 편입되지 않음)
    unassigned_by_angle = list(node_info)  # 각도 순 정렬된 상태

    while len(assigned) < len(delivery_nodes):
        # 미할당 노드 중 가장 먼 노드를 시드로 선택
        seed = None
        seed_dist = -1
        for ni, angle, dist in unassigned_by_angle:
            if ni not in assigned and dist > seed_dist:
                seed_dist = dist
                seed = ni

        if seed is None:
            break

        # 이 시드의 각도 기준으로, 각도가 가까운 미할당 노드들을
        # Nearest Neighbor로 채워서 클러스터 구성
        current_cluster = [seed]
        assigned.add(seed)

        while len(current_cluster) < max_per_cluster:
            last = current_cluster[-1]
            best_ni   = None
            best_dist = float('inf')

            for ni, angle, dist in unassigned_by_angle:
                if ni in assigned:
                    continue
                d = _haversine_km(nodes[last]['lat'], nodes[last]['lon'],
                                  nodes[ni]['lat'], nodes[ni]['lon'])
                if d < best_dist:
                    best_dist = d
                    best_ni   = ni

            if best_ni is None:
                break

            # 너무 멀어지면 (2km 초과) 새 클러스터로 분리
            seed_lat = nodes[seed]['lat']
            seed_lon = nodes[seed]['lon']
            dist_from_seed = _haversine_km(
                seed_lat, seed_lon,
                nodes[best_ni]['lat'], nodes[best_ni]['lon'])

            if dist_from_seed > 3.0 and len(current_cluster) >= 3:
                # 이미 3개 이상 채웠고, 시드에서 3km 넘으면 새 클러스터
                break

            current_cluster.append(best_ni)
            assigned.add(best_ni)

        clusters[cluster_id] = current_cluster
        cluster_id += 1

    return clusters


def _order_clusters_by_angle(nodes: list, clusters: dict) -> list:
    """
    클러스터를 출발 창고 기준 극좌표 각도 순으로 정렬.
    Sweep이므로 클러스터 순서 = 각도 순서 (API 불필요).

    Returns: [cluster_id, ...]
    """
    depot_lat = nodes[0]['lat']
    depot_lon = nodes[0]['lon']

    cluster_angles = []
    for cid, members in clusters.items():
        # 클러스터 중심의 각도
        avg_lat = sum(nodes[ni]['lat'] for ni in members) / len(members)
        avg_lon = sum(nodes[ni]['lon'] for ni in members) / len(members)
        angle = _polar_angle(depot_lat, depot_lon, avg_lat, avg_lon)
        cluster_angles.append((cid, angle))

    # 각도 순 정렬
    cluster_angles.sort(key=lambda x: x[1])

    # 출발 창고에서 가장 가까운 클러스터를 첫 번째로 회전
    # → Nearest Neighbor로 시작점 결정
    best_start = 0
    best_dist  = float('inf')
    for i, (cid, angle) in enumerate(cluster_angles):
        members = clusters[cid]
        avg_lat = sum(nodes[ni]['lat'] for ni in members) / len(members)
        avg_lon = sum(nodes[ni]['lon'] for ni in members) / len(members)
        d = _haversine_km(depot_lat, depot_lon, avg_lat, avg_lon)
        if d < best_dist:
            best_dist = d
            best_start = i

    # 가장 가까운 클러스터부터 시계 방향으로 회전
    ordered = cluster_angles[best_start:] + cluster_angles[:best_start]

    return [cid for cid, angle in ordered]


# ── 4단계: Sweep 클러스터링 + 순서 확정 ───────────────────────────────────────
def build_time_matrix(nodes: list, headers: dict,
                      progress_cb=None,
                      stop_event: threading.Event = None,
                      log_cb=None) -> list:
    """
    4단계: DSN Sweep 클러스터링 → 각도 기반 순서 확정.
    """
    global _last_clusters, _last_cluster_order

    def _log(msg):
        if log_cb:
            log_cb(msg)

    n = len(nodes)
    if n <= 1:
        return [[0]]

    # 빈 행렬 초기화 (대각선 = 0)
    matrix = [[0 if i == j else None for j in range(n)] for i in range(n)]

    delivery_nodes = list(range(1, n))

    # ── 4-1) DSN Sweep Nearest 클러스터링 ────────────────────────────────────
    _log("  4-1)  Sweep Nearest 클러스터링 중...")
    _log(f"       출발 창고 기준 극좌표 각도 + 거리 기반 분할")

    # 클러스터당 최대 노드 수 결정
    total = len(delivery_nodes)
    if total <= 20:
        max_per = total  # 20건 이하면 전체를 하나로
    elif total <= 50:
        max_per = 20
    else:
        max_per = max(15, total // 6)

    clusters = _sweep_nearest_cluster(nodes, delivery_nodes, max_per)

    _log(f"  ✅  {len(clusters)}개 클러스터 생성 완료")
    for cid, members in clusters.items():
        _log(f"       클러스터 {cid + 1}: {len(members)}건")

    _last_clusters = clusters

    if stop_event and stop_event.is_set():
        return matrix

    # ── 4-2) 클러스터 순서 = 각도 순서 (API 불필요) ──────────────────────────
    _log(f"\n  4-2)  클러스터 순서 결정 중 (극좌표 각도 순)...")
    cluster_order = _order_clusters_by_angle(nodes, clusters)

    _log(f"  ✅  클러스터 순서 확정: "
         + " → ".join(f"C{cid + 1}" for cid in cluster_order))

    if progress_cb:
        progress_cb(1, 1)

    _last_cluster_order = cluster_order

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


# ── 5단계: 그룹핑 + API 호출 + TSP + 펼침 ────────────────────────────────────
def optimize_route(nodes: list, time_matrix: list,
                   headers: dict = None, log_cb=None):
    """
    5단계: 그룹핑 → API 호출 → IN/OUT → TSP → 멤버 펼침.
    """
    global _last_clusters, _last_cluster_order

    def _log(msg):
        if log_cb:
            log_cb(msg)

    n = len(nodes)
    if n <= 1:
        return None
    if n == 2:
        return [1]

    delivery_nodes = list(range(1, n))

    # ── 4단계 결과 로드 ──────────────────────────────────────────────────────
    clusters      = _last_clusters
    cluster_order = _last_cluster_order

    # 4단계 결과가 없으면 (직접 호출된 경우) fallback
    if clusters is None or cluster_order is None:
        clusters = _sweep_nearest_cluster(nodes, delivery_nodes)
        cluster_order = _order_clusters_by_angle(nodes, clusters)

    # ── 5-1) 같은 건물 그룹핑 → 대표 노드 추출 ──────────────────────────────
    _log("  5-1)  같은 건물/주소 그룹핑 중...")
    location_groups = _build_location_groups(delivery_nodes, nodes)
    rep_nodes = list(location_groups.keys())
    grouped_cnt = sum(1 for g in location_groups.values() if len(g) > 1)
    _log(f"  ✅  대표 {len(rep_nodes)}개 추출"
         f" (같은 건물 그룹 {grouped_cnt}개)")

    # 대표 노드 → 클러스터 매핑
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

    # ── 5-2) 대표 노드끼리 카카오 API 호출 ───────────────────────────────────
    if headers is not None:
        _log(f"\n  5-2)  대표 노드 간 도로 시간 계산 중 (카카오 API)...")

        pairs_needed = set()

        # 출발지(0) ↔ 모든 대표
        for rep in rep_nodes:
            pairs_needed.add((0, rep))
            pairs_needed.add((rep, 0))

        # 클러스터 내부 대표끼리 모든 쌍
        for cid, reps in cluster_reps.items():
            for i in reps:
                for j in reps:
                    if i != j:
                        pairs_needed.add((i, j))

        # 클러스터 간 경계 대표 쌍 (인접 클러스터만)
        for idx, cid in enumerate(cluster_order):
            if idx + 1 < len(cluster_order):
                next_cid = cluster_order[idx + 1]
                reps_ci = cluster_reps.get(cid, [])
                reps_cj = cluster_reps.get(next_cid, [])
                border = []
                for ni in reps_ci:
                    for nj in reps_cj:
                        d = _haversine_km(nodes[ni]['lat'], nodes[ni]['lon'],
                                          nodes[nj]['lat'], nodes[nj]['lon'])
                        border.append((d, ni, nj))
                border.sort()
                for _, ni, nj in border[:min(5, len(border))]:
                    pairs_needed.add((ni, nj))
                    pairs_needed.add((nj, ni))

        pairs_todo = [(i, j) for (i, j) in pairs_needed
                      if time_matrix[i][j] is None]

        total_api = len(pairs_todo)
        _log(f"       API 호출 대상: {total_api}쌍")

        done_api = 0
        for i, j in pairs_todo:
            if time_matrix[i][j] is None:
                time_matrix[i][j] = _get_driving_time(
                    nodes[i]['lon'], nodes[i]['lat'],
                    nodes[j]['lon'], nodes[j]['lat'],
                    headers)
                time.sleep(0.15)
                done_api += 1
                if done_api % 20 == 0:
                    _log(f"       →  {done_api} / {total_api} 완료")

        _log(f"  ✅  도로 시간 계산 완료 — {done_api}쌍")

    # ── 5-3) IN/OUT 결정 + 클러스터 내 TSP ───────────────────────────────────
    _log(f"\n  5-3)  클러스터별 배송 순서 최적화 중...")
    rep_order = []

    for step, cid in enumerate(cluster_order):
        reps = cluster_reps.get(cid, [])
        if not reps:
            continue

        # IN
        if rep_order:
            last_rep = rep_order[-1]
            entry_node = min(
                reps,
                key=lambda ni: time_matrix[last_rep][ni]
                if time_matrix[last_rep][ni] is not None
                else float('inf'))
        else:
            entry_node = min(
                reps,
                key=lambda ni: time_matrix[0][ni]
                if time_matrix[0][ni] is not None
                else float('inf'))

        # OUT
        if step + 1 < len(cluster_order):
            next_cid = cluster_order[step + 1]
            next_reps = cluster_reps.get(next_cid, [])
            exit_node = _find_exit_node(reps, next_reps,
                                         nodes, time_matrix)
        else:
            exit_node = None

        # TSP
        cluster_route = _solve_cluster_tsp(
            reps, entry_node, exit_node, nodes, time_matrix)

        rep_order.extend(cluster_route)

        in_name  = nodes[entry_node].get('name', '')
        out_name = nodes[exit_node].get('name', '') if exit_node else '자유'
        _log(f"       C{cid + 1}: {len(reps)}개 대표"
             f"  IN={in_name}  OUT={out_name}")

    _log(f"  ✅  클러스터별 TSP 완료")

    # ── 5-4) 대표 순서대로 같은 건물 멤버 펼침 ───────────────────────────────
    _log(f"\n  5-4)  같은 건물 멤버 연속 배치 중...")
    final_order = []
    for rep in rep_order:
        group_members = location_groups.get(rep, [rep])
        for member in group_members:
            if member not in final_order:
                final_order.append(member)

    _log(f"  ✅  최종 {len(final_order)}건 순서 확정")

    return final_order
