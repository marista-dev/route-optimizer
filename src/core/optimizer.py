"""
optimizer.py
카카오 모빌리티 API + OR-Tools (SAVINGS + GLS) 배송 순서 최적화
  - build_time_matrix : N×N 자동차 주행 시간 행렬 구축 (체크포인트 + 중단 지원)
  - optimize_route    : Open TSP 풀이 → 배송 node index 순서 반환
  - clear_checkpoint  : 체크포인트 파일 삭제
"""

import json
import os
import re
import threading
import time
import requests
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp

_CHECKPOINT_DIR = os.path.join(
    os.environ.get('APPDATA', os.path.expanduser('~')),
    'RouteOptimizer'
)
CHECKPOINT_FILE = os.path.join(_CHECKPOINT_DIR, 'time_matrix.json')


def _save(matrix):
    os.makedirs(_CHECKPOINT_DIR, exist_ok=True)
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump(matrix, f)


def clear_checkpoint():
    """체크포인트 파일 삭제 (중단 또는 새 작업 시작 시 호출)."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            os.remove(CHECKPOINT_FILE)
        except Exception:
            pass


def _get_driving_time(o_lon, o_lat, d_lon, d_lat, headers: dict) -> int:
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


def build_time_matrix(nodes: list, headers: dict,
                      progress_cb=None,
                      stop_event: threading.Event = None) -> list:
    """
    N×N 주행 시간 행렬 구축.

    Parameters
    ----------
    nodes       : [{'name', 'lat', 'lon', 'id'}, ...]  (0번 = 출발지)
    headers     : 카카오 API Authorization 헤더
    progress_cb : callable(done, total) | None
    stop_event  : 설정되면 현재 반복 후 즉시 반환
    """
    n      = len(nodes)
    matrix = [[None] * n for _ in range(n)]

    # 체크포인트 복구
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, encoding='utf-8') as f:
                saved = json.load(f)
            if len(saved) == n and len(saved[0]) == n:
                matrix = saved
        except Exception:
            pass

    total = n * (n - 1)
    calls = 0

    for i in range(n):
        for j in range(n):
            # 중단 신호 확인 — 현재 행(i) 완료 후 반환
            if stop_event and stop_event.is_set():
                _save(matrix)
                return matrix

            if i == j:
                matrix[i][j] = 0
                continue
            if matrix[i][j] is None:
                matrix[i][j] = _get_driving_time(
                    nodes[i]['lon'], nodes[i]['lat'],
                    nodes[j]['lon'], nodes[j]['lat'],
                    headers)
                time.sleep(0.15)
                calls += 1
                if calls % 10 == 0:
                    _save(matrix)
                    if progress_cb:
                        done = sum(1 for r in matrix for v in r
                                   if v is not None) - n
                        progress_cb(done, total)

    _save(matrix)
    return matrix


def optimize_route(nodes: list, time_matrix: list):
    """
    SAVINGS 초기해 + GLS 180 초 개선.  Open TSP (출발지 미복귀).
    반환: 배송 순서대로 정렬된 node index 리스트 or None
    """
    n     = len(nodes)
    dummy = n
    ext   = [[0] * (n + 1) for _ in range(n + 1)]

    for i in range(n):
        for j in range(n):
            ext[i][j] = time_matrix[i][j]
        ext[i][dummy] = 0

    manager = pywrapcp.RoutingIndexManager(n + 1, 1, [0], [dummy])
    routing = pywrapcp.RoutingModel(manager)

    def _cost(fi, ti):
        return ext[manager.IndexToNode(fi)][manager.IndexToNode(ti)]

    cb = routing.RegisterTransitCallback(_cost)
    routing.SetArcCostEvaluatorOfAllVehicles(cb)

    params = pywrapcp.DefaultRoutingSearchParameters()
    params.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.SAVINGS)
    params.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH)
    params.time_limit.seconds = 180

    sol = routing.SolveWithParameters(params)
    if not sol:
        return None

    order, idx = [], routing.Start(0)
    while not routing.IsEnd(idx):
        node = manager.IndexToNode(idx)
        if node != 0 and node != dummy:
            order.append(node)
        idx = sol.Value(routing.NextVar(idx))

    return _group_same_location(order, nodes)


def _strip_unit(address: str) -> str:
    """동/호/층 번호를 제거한 기본 주소 반환 (같은 건물 단위 비교용)"""
    s = re.sub(r'\d+동\s*\d+호|\d+층.*|\d+호', '', address)
    s = re.sub(r'\s+', ' ', s).strip().lower()
    return s


def _group_same_location(order: list, nodes: list) -> list:
    """
    OR-Tools 결과에서 동일 좌표(같은 건물/주소) 노드를 연속 배치하는 후처리.

    알고리즘:
    1. 순서대로 순회하면서 각 노드의 좌표 키와 주소 키를 확인
    2. 동일 좌표(소수점 5자리 ≈ 1m 이내) 또는 동일 기본주소(동호수 제외)이면
       해당 그룹의 마지막 항목 바로 뒤에 삽입
    3. 새 위치면 결과 끝에 추가

    효과: 같은 건물에 호수만 다른 주소가 흩어져 있어도 연속 배치됨
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

    return result
