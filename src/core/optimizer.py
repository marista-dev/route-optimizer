"""
optimizer.py
카카오 모빌리티 API + OR-Tools TSP 배송 순서 최적화

전략 (클러스터링 없음, 단순하고 정확한 구조):
  4단계 (build_time_matrix):
    1. 같은 건물/주소 그룹핑 → 대표 노드 추출
    2. 대표 노드끼리 전체 N×N 카카오 API 호출

  5단계 (optimize_route):
    1. 전체 대표를 OR-Tools TSP로 한 번에 최적 순서 계산
       (출발 창고에서 시작, 총 주행시간 최소화)
    2. 대표 순서대로 같은 건물 멤버 펼침

공개 API:
  - build_time_matrix(nodes, headers, progress_cb, stop_event, log_cb)
      → (time_matrix, location_groups)
  - optimize_route(nodes, time_matrix, location_groups, log_cb)
      → final_order
"""

import math
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp

# HTTP keep-alive 세션 (병렬 스레드 안전)
_SESSION = requests.Session()

# 병렬 워커 수 — 카카오 모빌리티 API의 미공개 rate limit을 고려해 보수적으로 3개
# (50회 burst에서 429 보고 사례 기준)
_API_WORKERS = 3

# 카카오 모빌리티 API
def _is_rate_limit_400(resp) -> bool:
    """카카오 Mobility API는 rate limit 초과 시 HTTP 400 + code -10으로 응답한다.

    일반 400 (좌표 오류)와 구별해서 rate limit만 retry해야 함.
    """
    if resp.status_code != 400:
        return False
    try:
        body = resp.json()
        # code: -10 또는 msg에 'limit' 포함되면 rate limit
        if body.get('code') == -10:
            return True
        msg = str(body.get('msg', '')).lower()
        return 'limit' in msg
    except Exception:
        return False


def _get_driving_time(o_lon, o_lat, d_lon, d_lat, headers: dict) -> int:
    """두 지점 간 자동차 주행 시간(초) 반환. 5회 retry 후에도 실패 시 999_999.

    Retry 전략 (exponential backoff):
      - 200 OK + result_code 0:        성공 리턴
      - 200 OK + result_code != 0:     좌표/경로 문제 → 즉시 실패 (retry 불필요)
      - 400 + "API limit" (code -10):  rate limit → 3,6,12,24,48초 backoff
      - 429:                           rate limit → 3,6,12,24,48초 backoff
      - 4xx (401/403 등):             인증 오류 → 즉시 실패
      - 5xx:                           서버 오류 → 2,4,8,16,32초 backoff
      - timeout / Connection:          일시적 장애 → 1,2,4,8,16초 backoff
    """
    url    = 'https://apis-navi.kakaomobility.com/v1/directions'
    params = {'origin':      f'{o_lon},{o_lat}',
              'destination': f'{d_lon},{d_lat}',
              'priority':    'RECOMMEND'}
    for attempt in range(5):
        try:
            resp = _SESSION.get(url, headers=headers, params=params, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('routes') and data['routes'][0]['result_code'] == 0:
                    return data['routes'][0]['summary']['duration']
                # 200 + result_code != 0 → 좌표/경로 문제. retry 의미 없음.
                return 999_999
            elif resp.status_code == 429 or _is_rate_limit_400(resp):
                # Rate limit: exponential backoff (3 → 6 → 12 → 24 → 48초)
                time.sleep(3 * (2 ** attempt))
                continue
            elif resp.status_code >= 500:
                # 서버 일시 장애: 짧은 backoff
                time.sleep(2 * (2 ** attempt))
                continue
            elif 400 <= resp.status_code < 500:
                # 그 외 4xx (일반 400, 401, 403 등) → retry 불필요
                return 999_999
        except (requests.Timeout, requests.ConnectionError):
            # 네트워크 이슈: 약한 backoff (1 → 2 → 4 → 8 → 16초)
            time.sleep(1 * (2 ** attempt))
        except Exception:
            time.sleep(1 * (2 ** attempt))
    return 999_999


# 거리 유틸
def _haversine_km(lat1, lon1, lat2, lon2) -> float:
    """두 좌표 간 Haversine 직선 거리(km)."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# 같은 위치 그룹핑
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


# 4단계: 그룹핑 + 전체 N×N API 호출
def build_time_matrix(nodes: list, headers: dict,
                      progress_cb=None,
                      stop_event: threading.Event = None,
                      log_cb=None) -> tuple:
    """
    4단계: 그룹핑 → 대표 노드끼리 전체 N×N 카카오 API 호출.

    Returns: (time_matrix, location_groups)
    """
    def _log(msg):
        if log_cb:
            log_cb(msg)

    n = len(nodes)
    if n <= 1:
        return [[0]], {}

    # 빈 행렬 초기화 (대각선 = 0)
    matrix = [[0 if i == j else None for j in range(n)] for i in range(n)]

    # 4-1) 같은 건물 그룹핑 → 대표 노드 추출
    _log("  4-1)  같은 건물/주소 그룹핑 중...")
    location_groups = _build_location_groups(list(range(1, n)), nodes)
    rep_nodes = list(location_groups.keys())
    grouped_cnt = sum(1 for g in location_groups.values() if len(g) > 1)
    _log(f"  ✅  전체 {n - 1}건 → 대표 {len(rep_nodes)}개 추출"
         f" (같은 건물 그룹 {grouped_cnt}개)")

    if stop_event and stop_event.is_set():
        return matrix, location_groups

    # 4-2) 대표 노드끼리 전체 N×N 카카오 API 호출
    # 출발지(0) ↔ 대표 + 대표 ↔ 대표 전체 쌍
    all_indices = [0] + rep_nodes  # 출발지 + 모든 대표
    pairs_todo = [(i, j) for i in all_indices for j in all_indices
                  if i != j and matrix[i][j] is None]

    total_api = len(pairs_todo)
    _log(f"\n  4-2)  대표 노드 간 도로 시간 계산 중 (카카오 API)...")
    _log(f"       대표 {len(rep_nodes)}개 + 출발지 → {total_api}쌍 호출 예정 (병렬 {_API_WORKERS}개)")

    done_api = 0
    api_fail_cnt = 0       # API 5회 retry 모두 실패 (999_999 sentinel)
    api_exception_cnt = 0  # future.result() 자체 예외
    if total_api > 0:
        with ThreadPoolExecutor(max_workers=_API_WORKERS) as executor:
            future_to_pair = {
                executor.submit(_get_driving_time,
                                nodes[i]['lon'], nodes[i]['lat'],
                                nodes[j]['lon'], nodes[j]['lat'],
                                headers): (i, j)
                for i, j in pairs_todo
            }

            for future in as_completed(future_to_pair):
                if stop_event and stop_event.is_set():
                    executor.shutdown(wait=False, cancel_futures=True)
                    return matrix, location_groups

                i, j = future_to_pair[future]
                try:
                    result = future.result()
                    if result == 999_999:
                        # 5번 retry 모두 실패 → Haversine 추정 (TSP 왜곡 방지)
                        dist_km = _haversine_km(
                            nodes[i]['lat'], nodes[i]['lon'],
                            nodes[j]['lat'], nodes[j]['lon'])
                        matrix[i][j] = int(dist_km / 40.0 * 3600)
                        api_fail_cnt += 1
                    else:
                        matrix[i][j] = result
                except Exception as e:
                    # future 자체 예외 → Haversine 추정
                    dist_km = _haversine_km(
                        nodes[i]['lat'], nodes[i]['lon'],
                        nodes[j]['lat'], nodes[j]['lon'])
                    matrix[i][j] = int(dist_km / 40.0 * 3600)
                    api_fail_cnt += 1
                    api_exception_cnt += 1
                    if api_exception_cnt <= 3:
                        _log(f"       ⚠️  쌍 ({i},{j}) 예외: {type(e).__name__}: {e}")
                done_api += 1

                if done_api % 50 == 0:
                    _log(f"       →  {done_api} / {total_api} 완료")
                    if progress_cb:
                        progress_cb(done_api, total_api)

    # all_indices 내 None 셀만 Haversine 추정치로 채움
    # (optimize_route는 이 범위만 참조 — 비-대표 셀 낭비 방지)
    for i in all_indices:
        for j in all_indices:
            if i != j and matrix[i][j] is None:
                dist_km = _haversine_km(nodes[i]['lat'], nodes[i]['lon'],
                                         nodes[j]['lat'], nodes[j]['lon'])
                matrix[i][j] = int(dist_km / 40.0 * 3600)

    _log(f"  ✅  도로 시간 계산 완료 — {done_api}쌍 (API 성공 {done_api - api_fail_cnt}, Haversine 대체 {api_fail_cnt})")
    if api_fail_cnt > 0:
        pct = api_fail_cnt / total_api * 100
        _log(f"  ⚠️  API 실패 {api_fail_cnt}/{total_api}쌍 ({pct:.1f}%) → Haversine 직선거리 추정으로 대체")
        if api_exception_cnt > 0:
            _log(f"      └─ 예외 발생 {api_exception_cnt}건 (나머지는 5회 retry 모두 실패)")
        if pct > 10:
            _log(f"  ⚠️  실패율이 높습니다. 워커 수를 줄이거나 잠시 후 재시도를 권장합니다.")

    if progress_cb:
        progress_cb(total_api, total_api)

    return matrix, location_groups


# 5단계: OR-Tools TSP + 멤버 펼침
def optimize_route(nodes: list, time_matrix: list,
                   location_groups: dict, log_cb=None):
    """
    5단계: 전체 대표 노드를 OR-Tools TSP로 한 번에 최적 순서 계산 → 멤버 펼침.
    """
    def _log(msg):
        if log_cb:
            log_cb(msg)

    n = len(nodes)
    if n <= 1:
        return None
    if n == 2:
        return [1]

    rep_nodes = list(location_groups.keys())
    num = len(rep_nodes)

    # 5-1) 전체 대표를 OR-Tools TSP로 최적 순서 계산
    _log("  5-1)  OR-Tools TSP 최적 순서 계산 중...")
    _log(f"       대표 {num}개를 한 번에 최적화")

    if num <= 2:
        rep_order = list(rep_nodes)
    else:
        local_nodes = [0] + rep_nodes  # 출발지 + 대표들
        local_n = len(local_nodes)

        # 로컬 시간 행렬
        local_matrix = []
        for i in range(local_n):
            row = []
            for j in range(local_n):
                gi, gj = local_nodes[i], local_nodes[j]
                t = time_matrix[gi][gj]
                if t is None:
                    t = int(_haversine_km(nodes[gi]['lat'], nodes[gi]['lon'],
                                           nodes[gj]['lat'], nodes[gj]['lon'])
                            / 40.0 * 3600)
                row.append(t)
            local_matrix.append(row)

        # Open TSP: 출발지(0) → ... → dummy (미복귀)
        dummy = local_n
        ext = [[0] * (local_n + 1) for _ in range(local_n + 1)]
        for i in range(local_n):
            for j in range(local_n):
                ext[i][j] = local_matrix[i][j]
            ext[i][dummy] = 0

        manager = pywrapcp.RoutingIndexManager(
            local_n + 1, 1, [0], [dummy])
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
        # 대표 100개 수준 → 60초면 충분
        params.time_limit.seconds = min(120, max(30, num * 2))

        _log(f"       TSP 제한 시간: {params.time_limit.seconds}초")

        sol = routing.SolveWithParameters(params)

        if sol:
            rep_order = []
            idx = routing.Start(0)
            while not routing.IsEnd(idx):
                node = manager.IndexToNode(idx)
                if node != 0 and node != dummy and node < local_n:
                    rep_order.append(local_nodes[node])
                idx = sol.Value(routing.NextVar(idx))
            _log(f"  ✅  TSP 최적화 완료 — {len(rep_order)}개 대표 순서 확정")
        else:
            # fallback: Nearest Neighbor
            _log("  ⚠️  TSP 실패 → Nearest Neighbor fallback")
            rep_order = _nearest_neighbor_chain(rep_nodes, nodes, time_matrix)
            _log(f"  ✅  Nearest Neighbor 완료 — {len(rep_order)}개 순서 확정")

    # 5-2) 대표 순서대로 같은 건물 멤버 펼침
    _log(f"\n  5-2)  같은 건물 멤버 연속 배치 중...")
    final_order = [m for rep in rep_order for m in location_groups.get(rep, [rep])]

    _log(f"  ✅  최종 {len(final_order)}건 순서 확정")

    return final_order


def _nearest_neighbor_chain(rep_nodes: list, nodes: list,
                            time_matrix: list) -> list:
    """
    출발지(0)에서 시작하여 가장 가까운 미방문 대표 노드를 순서대로 연결.
    OR-Tools TSP 실패 시 fallback.
    """
    visited = set()
    order   = []
    current = 0  # 출발지

    while len(order) < len(rep_nodes):
        best_ni   = None
        best_time = float('inf')

        for ni in rep_nodes:
            if ni in visited:
                continue
            t = time_matrix[current][ni]
            if t is not None and t < best_time:
                best_time = t
                best_ni   = ni

        if best_ni is None:
            # 남은 노드 아무거나
            for ni in rep_nodes:
                if ni not in visited:
                    best_ni = ni
                    break
            if best_ni is None:
                break

        order.append(best_ni)
        visited.add(best_ni)
        current = best_ni

    return order
