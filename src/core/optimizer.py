"""
optimizer.py
카카오 모빌리티 API + OR-Tools (SAVINGS + GLS) 배송 순서 최적화
"""

import json
import os
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
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)


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


def build_time_matrix(nodes: list, headers: dict, progress_cb=None) -> list:
    n      = len(nodes)
    matrix = [[None] * n for _ in range(n)]

    # 체크포인트 복구 — with 구문으로 파일 핸들 명시적 닫기
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
    return order
