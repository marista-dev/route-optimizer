"""
optimizer.py
카카오 모빌리티 API + OR-Tools TSP 배송 순서 최적화

전략 (2단계 계층적 그룹핑 + 하이브리드 최적화):
  4단계 (build_time_matrix):
    1. 1차 그룹핑: 같은 건물/주소 (좌표 1m 또는 동·호수 제거 동일)
    2. 2차 그룹핑: 1차 대표 간 100m 이내를 Union-Find로 클러스터화
    3. 2차 대표끼리만 카카오 API 호출 (호출 수 대폭 감소)

  5단계 (optimize_route):
    1. 2차 대표를 OR-Tools TSP로 최적 순서 계산 (정밀 도로시간 기반)
    2. 각 클러스터 내부 1차 대표들을 NN(Haversine)으로 정렬
       (직전 클러스터 마지막 노드에서 가장 가까운 멤버부터 연속)
    3. 1차 그룹 멤버(같은 건물) 펼침

공개 API:
  - build_time_matrix(nodes, headers, progress_cb, stop_event, log_cb)
      → (time_matrix, primary_groups, secondary_clusters)
  - optimize_route(nodes, time_matrix, primary_groups, secondary_clusters, log_cb)
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

# 2차 그룹핑 임계값 (미터) — 1차 대표 간 이 거리 이내면 같은 클러스터로 묶음
# → API 호출 수 대폭 감소 (일일 한도 10K 대응)
# 클러스터 내부는 NN(Haversine)으로 순서 결정 → 소실 미미 (도보 2~3분, 택배기사 재주차 시간과 비슷)
_SECONDARY_CLUSTER_M = 200

# Rate limit 중단 임계값 — 일일 한도 초과 감지 시 즉시 중단 + 알림
# 일시적 burst와 구분하기 위해 연속/누적 둘 다 체크
_RATE_LIMIT_CONSEC_LIMIT = 5  # 연속 이 회수 rate-limit → 중단
_RATE_LIMIT_TOTAL_LIMIT  = 10  # 누적 이 회수 rate-limit → 중단


class RateLimitExceededError(Exception):
    """카카오 API 일일 한도 초과 감지 시 발생. app.py가 알림 창을 띄우고 작업 중단.

    Attributes:
        consecutive: 연속 감지 횟수
        total: 누적 감지 횟수
        progress: 중단 시점의 진행률 (done / total)
    """
    def __init__(self, consecutive: int, total: int, progress: tuple = None):
        self.consecutive = consecutive
        self.total = total
        self.progress = progress
        msg = f"API 일일 한도 초과 (연속 {consecutive}건, 누적 {total}건)"
        super().__init__(msg)

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
    """두 지점 간 자동차 주행 시간(초) 반환.

    반환값:
      - 정상: 주행 시간 (조)
      - 일반 실패 (timeout/5xx/좌표오류 등): 999_999 → Haversine 대체
      - **rate-limit 실패 (5회 retry 후 일일 한도 계속 감지): -1** → 카운터 증가

    Retry 전략 (exponential backoff):
      - 200 OK + result_code 0:        성공 리턴
      - 200 OK + result_code != 0:     좌표/경로 문제 → 999_999
      - 400 + "API limit" (code -10):  rate limit → 3,6,12,24,48초 backoff → 계속 실패시 -1
      - 429:                           rate limit → 3,6,12,24,48초 backoff → 계속 실패시 -1
      - 4xx (401/403 등):             인증 오류 → 999_999
      - 5xx:                           서버 오류 → 2,4,8,16,32초 backoff
      - timeout / Connection:          일시적 장애 → 1,2,4,8,16초 backoff
    """
    url    = 'https://apis-navi.kakaomobility.com/v1/directions'
    params = {'origin':      f'{o_lon},{o_lat}',
              'destination': f'{d_lon},{d_lat}',
              'priority':    'RECOMMEND'}
    last_was_rate_limit = False  # 5회 retry 중 마지막 단계가 rate-limit이었는지

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
                last_was_rate_limit = True
                time.sleep(3 * (2 ** attempt))
                continue
            elif resp.status_code >= 500:
                # 서버 일시 장애: 짧은 backoff
                last_was_rate_limit = False
                time.sleep(2 * (2 ** attempt))
                continue
            elif 400 <= resp.status_code < 500:
                # 그 외 4xx (일반 400, 401, 403 등) → retry 불필요
                return 999_999
        except (requests.Timeout, requests.ConnectionError):
            # 네트워크 이슈: 약한 backoff (1 → 2 → 4 → 8 → 16초)
            last_was_rate_limit = False
            time.sleep(1 * (2 ** attempt))
        except Exception:
            last_was_rate_limit = False
            time.sleep(1 * (2 ** attempt))

    # 5회 retry 모두 실패 → 마지막 원인으로 구분
    return -1 if last_was_rate_limit else 999_999


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


# 2차 그룹핑 (Haversine 기반 클러스터화)
def _build_secondary_clusters(primary_reps: list, nodes: list,
                              threshold_m: float = _SECONDARY_CLUSTER_M) -> dict:
    """
    1차 대표 노드들을 Haversine 거리가 threshold_m 이내면 같은
    클러스터로 묶는다 (Union-Find 알고리즘).

    예: A↔B 80m, B↔C 80m, A↔C 150m → 임계값 100m이면 A,B,C 모두 같은 클러스터
          (transitive closure: A↔B 연결, B↔C 연결 → A,B,C 한 덩어리)

    Returns: {클러스터_대표: [멤버1, 멤버2, ...], ...}
             클러스터 대표는 멤버 중 더 작은 인덱스 (결정적)
    """
    if not primary_reps:
        return {}

    threshold_km = threshold_m / 1000.0

    # Union-Find
    parent = {r: r for r in primary_reps}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]  # path compression
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            # 작은 인덱스를 root로 (결정성 확보)
            if ra < rb:
                parent[rb] = ra
            else:
                parent[ra] = rb

    # 모든 쌍 비교 → 임계값 이내면 union
    n_reps = len(primary_reps)
    for i in range(n_reps):
        a = primary_reps[i]
        for j in range(i + 1, n_reps):
            b = primary_reps[j]
            d = _haversine_km(nodes[a]['lat'], nodes[a]['lon'],
                              nodes[b]['lat'], nodes[b]['lon'])
            if d <= threshold_km:
                union(a, b)

    # 클러스터 구성
    clusters = {}
    for r in primary_reps:
        root = find(r)
        clusters.setdefault(root, []).append(r)

    return clusters


def _nearest_within_cluster(members: list, prev_node_idx: int,
                            nodes: list) -> list:
    """
    클러스터 멤버들을 prev_node에서 가장 가까운 순서로 정렬 (NN, Haversine).

    1) prev_node → 멤버 중 가장 가까운 노드 선택 → first
    2) first → 다음 가장 가까운 → second
    3) ... 모든 멤버 소진 때까지

    Returns: 정렬된 멤버 인덱스 리스트
    """
    if not members:
        return []
    if len(members) == 1:
        return list(members)

    remaining = list(members)
    ordered = []
    current = prev_node_idx

    while remaining:
        best_idx = 0
        best_d = float('inf')
        for i, m in enumerate(remaining):
            d = _haversine_km(
                nodes[current]['lat'], nodes[current]['lon'],
                nodes[m]['lat'], nodes[m]['lon'])
            if d < best_d:
                best_d = d
                best_idx = i
        next_node = remaining.pop(best_idx)
        ordered.append(next_node)
        current = next_node

    return ordered


# 4단계: 그룹핑 + 전체 N×N API 호출
def build_time_matrix(nodes: list, headers: dict,
                      progress_cb=None,
                      stop_event: threading.Event = None,
                      log_cb=None) -> tuple:
    """
    4단계: 1차 그룹핑 → 2차 클러스터화 → 2차 대표끼리 카카오 API 호출.

    Returns: (time_matrix, primary_groups, secondary_clusters)
      - time_matrix: 전체 N×N 중 "출발지 + 2차 대표" 셔만 채워짐
      - primary_groups: {1차_대표: [멤버원본1, ...]}
      - secondary_clusters: {클러스터_대표: [1차_대표1, 1차_대표2, ...]}
    """
    def _log(msg):
        if log_cb:
            log_cb(msg)

    n = len(nodes)
    if n <= 1:
        return [[0]], {}, {}

    # 빈 행렬 초기화 (대각선 = 0)
    matrix = [[0 if i == j else None for j in range(n)] for i in range(n)]

    # 4-1) 1차 그룹핑: 같은 건물/주소
    _log("  4-1)  1차 그룹핑 (같은 건물/주소) 중...")
    primary_groups = _build_location_groups(list(range(1, n)), nodes)
    primary_reps = list(primary_groups.keys())
    p_grouped_cnt = sum(1 for g in primary_groups.values() if len(g) > 1)
    _log(f"  ✅  전체 {n - 1}건 → 1차 대표 {len(primary_reps)}개"
         f" (같은 건물 그룹 {p_grouped_cnt}개)")

    if stop_event and stop_event.is_set():
        return matrix, primary_groups, {}

    # 4-2) 2차 그룹핑: 1차 대표 간 100m 이내 클러스터화
    _log(f"\n  4-2)  2차 그룹핑 (Haversine {_SECONDARY_CLUSTER_M}m) 중...")
    secondary_clusters = _build_secondary_clusters(
        primary_reps, nodes, _SECONDARY_CLUSTER_M)
    cluster_reps = list(secondary_clusters.keys())
    s_grouped_cnt = sum(1 for g in secondary_clusters.values() if len(g) > 1)
    avg_size = sum(len(g) for g in secondary_clusters.values()) / max(1, len(cluster_reps))
    _log(f"  ✅  1차 대표 {len(primary_reps)}개 → 2차 클러스터 {len(cluster_reps)}개"
         f" (병합 {s_grouped_cnt}개, 평균 멤버 {avg_size:.1f}개)")

    if stop_event and stop_event.is_set():
        return matrix, primary_groups, secondary_clusters

    # 4-3) 2차 대표끼리만 카카오 API 호출 (호출 수 대폭 감소)
    all_indices = [0] + cluster_reps  # 출발지 + 2차 대표
    pairs_todo = [(i, j) for i in all_indices for j in all_indices
                  if i != j and matrix[i][j] is None]

    total_api = len(pairs_todo)
    _log(f"\n  4-3)  2차 대표 간 도로 시간 계산 중 (카카오 API)...")
    _log(f"       2차 대표 {len(cluster_reps)}개 + 출발지 → {total_api}쌍 호출 예정 (병렬 {_API_WORKERS}개)")
    if total_api <= 10000:
        _log(f"       → 일일 한도(10K) 대비 {total_api/100:.1f}% 사용 예상")
    else:
        _log(f"       ⚠️  일일 한도(10K) 초과 예상 ({total_api - 10000}건 초과) — 임계값 높이거나 데이터 분할 권장")

    done_api = 0
    api_fail_cnt = 0          # API 실패 총 (Haversine 대체로 이어지는 건)
    api_exception_cnt = 0     # future.result() 자체 예외
    rate_limit_total = 0      # 누적 rate-limit 감지 수
    rate_limit_consec = 0     # 연속 rate-limit 감지 수

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
                    return matrix, primary_groups, secondary_clusters

                i, j = future_to_pair[future]
                try:
                    result = future.result()
                    if result == -1:
                        # Rate-limit 실패 — 일일 한도 초과 의심 신호
                        rate_limit_total += 1
                        rate_limit_consec += 1
                        # 임계값 도달 시 즉시 중단 → RateLimitExceededError
                        if (rate_limit_consec >= _RATE_LIMIT_CONSEC_LIMIT or
                            rate_limit_total >= _RATE_LIMIT_TOTAL_LIMIT):
                            _log(f"")
                            _log(f"  🚨  일일 한도 초과 감지 — 작업 중단")
                            _log(f"      연속 {rate_limit_consec}건, 누적 {rate_limit_total}건 rate-limit 발생")
                            _log(f"      → 새 API 키로 교체하거나 자정 이후 재시도해주세요")
                            executor.shutdown(wait=False, cancel_futures=True)
                            raise RateLimitExceededError(
                                rate_limit_consec, rate_limit_total,
                                progress=(done_api, total_api))
                        # 임계값 아직 아님 → Haversine 대체하고 진행
                        dist_km = _haversine_km(
                            nodes[i]['lat'], nodes[i]['lon'],
                            nodes[j]['lat'], nodes[j]['lon'])
                        matrix[i][j] = int(dist_km / 40.0 * 3600)
                        api_fail_cnt += 1
                    elif result == 999_999:
                        # 일반 실패 (timeout/5xx/좌표오류) → Haversine 대체
                        rate_limit_consec = 0  # 연속 카운터 리셋
                        dist_km = _haversine_km(
                            nodes[i]['lat'], nodes[i]['lon'],
                            nodes[j]['lat'], nodes[j]['lon'])
                        matrix[i][j] = int(dist_km / 40.0 * 3600)
                        api_fail_cnt += 1
                    else:
                        # 정상 응답
                        rate_limit_consec = 0  # 연속 카운터 리셋
                        matrix[i][j] = result
                except RateLimitExceededError:
                    raise  # 상위로 전파
                except Exception as e:
                    # future 자체 예외 → Haversine 추정
                    rate_limit_consec = 0
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

    return matrix, primary_groups, secondary_clusters


# 5단계: TSP(클러스터) + NN(클러스터 내부) + 멤버 펼침
def optimize_route(nodes: list, time_matrix: list,
                   primary_groups: dict, secondary_clusters: dict,
                   log_cb=None):
    """
    5단계: 2차 대표 TSP → 클러스터 내부 NN → 1차 멤버 펼침.
    """
    def _log(msg):
        if log_cb:
            log_cb(msg)

    n = len(nodes)
    if n <= 1:
        return None
    if n == 2:
        return [1]

    cluster_reps = list(secondary_clusters.keys())
    num = len(cluster_reps)

    # 5-1) 2차 대표를 OR-Tools TSP로 최적 순서 계산 (정밀 도로시간)
    _log("  5-1)  OR-Tools TSP 최적 순서 계산 중 (2차 대표 기준)...")
    _log(f"       2차 대표 {num}개를 한 번에 최적화")

    if num <= 2:
        cluster_order = list(cluster_reps)
    else:
        local_nodes = [0] + cluster_reps  # 출발지 + 2차 대표
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
        # 2차 대표 수가 적으므로 시간 제한 더 타이트하게
        params.time_limit.seconds = min(120, max(30, num * 2))

        _log(f"       TSP 제한 시간: {params.time_limit.seconds}초")

        sol = routing.SolveWithParameters(params)

        if sol:
            cluster_order = []
            idx = routing.Start(0)
            while not routing.IsEnd(idx):
                node = manager.IndexToNode(idx)
                if node != 0 and node != dummy and node < local_n:
                    cluster_order.append(local_nodes[node])
                idx = sol.Value(routing.NextVar(idx))
            _log(f"  ✅  TSP 완료 — 2차 클러스터 {len(cluster_order)}개 순서 확정")
        else:
            # fallback: Nearest Neighbor (시간 행렬 기준)
            _log("  ⚠️  TSP 실패 → Nearest Neighbor fallback")
            cluster_order = _nearest_neighbor_chain(cluster_reps, nodes, time_matrix)
            _log(f"  ✅  Nearest Neighbor 완료 — {len(cluster_order)}개 순서")

    # 5-2) 클러스터 내부 NN — 직전 클러스터 마지막 노드에서 가까운 멤버부터 연속
    _log(f"\n  5-2)  클러스터 내부 NN(Haversine) 정렬 중...")
    primary_order = []
    prev_node = 0  # 출발지에서 시작
    multi_clusters = 0
    for cluster_rep in cluster_order:
        members = secondary_clusters.get(cluster_rep, [cluster_rep])
        if len(members) > 1:
            multi_clusters += 1
        sorted_members = _nearest_within_cluster(members, prev_node, nodes)
        primary_order.extend(sorted_members)
        if sorted_members:
            prev_node = sorted_members[-1]  # 다음 클러스터 진입점 결정용
    _log(f"  ✅  1차 대표 {len(primary_order)}개 순서 확정"
         f" (다중 멤버 클러스터 {multi_clusters}개에 NN 적용)")

    # 5-3) 1차 그룹 멤버(같은 건물) 펼침
    _log(f"\n  5-3)  같은 건물 멤버 연속 배치 중...")
    final_order = [m for rep in primary_order
                     for m in primary_groups.get(rep, [rep])]

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
