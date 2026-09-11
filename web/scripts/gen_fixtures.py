#!/usr/bin/env python3
"""gen_fixtures.py — Python core 로직의 출력을 JSON fixture로 떠서 TS 이식본과 대조한다.

계획 13절(검증)에서 요구하는 "Python 결과를 fixture로 뽑아 TS 결과와 일치 확인"을
자동화한 스크립트다. 실제 배송 데이터에는 개인정보가 들어 있으므로, 여기서 쓰는
주소·좌표는 전부 이 파일에서 직접 지어낸 합성 데이터다(광주 스타일 도로명 주소).

실행:
    /usr/bin/env python3 web/scripts/gen_fixtures.py

`core.optimizer`는 requests·ortools를 import하는데 fixture 생성에는 쓰이지 않으므로
MagicMock으로 갈아끼운 뒤 import한다(시스템 python3에 설치가 필요 없다).
"""

import json
import os
import sys
from unittest.mock import MagicMock

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, '..', '..'))
SRC = os.path.join(REPO, 'src')
OUT_DIR = os.path.join(HERE, '..', 'src', 'core', '__fixtures__')

sys.path.insert(0, SRC)

# optimizer가 import하는 외부 의존 제거 (fixture 생성에는 쓰이지 않는다)
for name in ('requests', 'ortools', 'ortools.constraint_solver'):
    sys.modules.setdefault(name, MagicMock())

from core.address import complex_key, parse_unit, strip_unit, unit_sort_key  # noqa: E402
from core.optimizer import (  # noqa: E402
    _build_location_groups,
    _build_secondary_clusters,
)

# ── 합성 주소 30건 ──────────────────────────────────────────────────────────
# address.py docstring 예제 + 동/호/층/괄호 변형 + 함정(법정동 숫자, '호반...')
ADDRESSES = [
    # docstring parse_unit 예제 4건
    '광주 북구 삼정로 7, 207동 403호(두암동, 주공2단지@)',
    '광주 북구 안산로 38-3, A동 102호 (오치2동, 영진하우스)',
    '광주 북구 하백로 46번길 9, 809호(매곡동, 부림@)',
    '광주 북구 첨단연신로 250, 112동 1205(신용동,첨단휴먼시아)',
    # docstring complex_key 예제 2건
    '광주 북구 매곡로 92, 102동 106호(매곡동,아남@)',
    '광주 북구 매곡로 92, 101동 105호(매곡동, 아남@)',
    # '제' 접두 표기
    '광주 북구 매곡로 92, 제201동 제1504호 (매곡동, 아남@)',
    '광주광역시 북구 매곡로 92 제102동 1504호',
    # 영문 동
    '광주 북구 안산로 40, B동 201호 (오치2동, 대광빌라)',
    '광주 북구 안산로 40, b동 2층 (오치2동, 대광빌라)',
    'C동 1501호 광주 북구 설죽로 100',
    # 층 표기
    '광주 북구 첨단연신로 260, 3층 (신용동, 상가)',
    '광주 북구 첨단연신로 260 2 층',
    '광주 북구 첨단연신로 260, 101동 3층 1202호',
    # 법정동 숫자 — 건물 동으로 오인하면 안 된다
    '광주 북구 오치2동 123-4',
    '광주 북구 운암1동 45',
    '광주 북구 문흥1동 대로 55, 1402호',
    # '호'로 시작하는 건물명 — 호수로 오인하면 안 된다
    '광주 북구 설죽로 507 호반리젠시빌 101동 1203호',
    '광주 북구 설죽로 507 호반리젠시빌',
    # 동 뒤 숫자가 호수가 아닌 경우
    '광주 북구 매곡로 92, 101동 15번지',
    '광주 북구 매곡로 92, 101동 20-1',
    '광주 북구 매곡로 92, 101동 1205',
    # 괄호 안 법정동 표기 흔들림
    '광주 북구 삼정로 7, 202동 406호(두암동,주공2단지@)',
    '광주 북구 삼정로 7, 206동 311호 (두암동, 주공2단지@)',
    # 호수만 있는 경우 / 아무것도 없는 경우
    '광주 북구 하백로 46번길 9, 제809호',
    '광주 북구 하백로 46번길 9',
    '광주 광산구 임방울대로 825, 1402호 (수완동, 수완대라수@)',
    '광주 광산구 임방울대로 825, 1403호 (수완동, 수완대라수@)',
    # 공백·쉼표 흔들림
    '광주  북구   매곡로 92 ,  103동  204호 ',
    '광주 북구 매곡로 92',
]

# ── 합성 노드 15건 (좌표 간격 100~600m) ──────────────────────────────────────
# Python 원본은 인덱스 0을 출발지로 쓰고 1..n을 배송지로 본다.
ORIGIN = {'lat': 35.15000, 'lon': 126.80000, 'address': '광주 북구 우치로 77'}

DELIVERY_NODES = [
    # 같은 건물 — 좌표도 단지키도 같다
    {'lat': 35.20000, 'lon': 126.85000,
     'address': '광주 북구 매곡로 92, 101동 105호(매곡동, 아남@)'},
    {'lat': 35.20000, 'lon': 126.85000,
     'address': '광주 북구 매곡로 92, 102동 106호(매곡동,아남@)'},
    {'lat': 35.20000, 'lon': 126.85000,
     'address': '광주 북구 매곡로 92, 제102동 제1504호 (매곡동,아남@)'},
    # 100m 북쪽 — 다른 단지
    {'lat': 35.20090, 'lon': 126.85000,
     'address': '광주 북구 매곡로 94, 201동 1502호 (매곡동, 부영@)'},
    {'lat': 35.20090, 'lon': 126.85000,
     'address': '광주 북구 매곡로 94, 202동 406호 (매곡동, 부영@)'},
    # 200m 북쪽 — 좌표는 다르지만 아래 노드와 단지키가 같다(주소 기준 병합 확인)
    {'lat': 35.20180, 'lon': 126.85000,
     'address': '광주 북구 삼정로 7, 207동 403호(두암동, 주공2단지@)'},
    {'lat': 35.20180, 'lon': 126.85110,
     'address': '광주 북구 삼정로 7, 202동 406호(두암동,주공2단지@)'},
    # 500m 북쪽 — 200m 임계값에서는 갈라지고 400m에서는 붙는다
    {'lat': 35.20450, 'lon': 126.85000,
     'address': '광주 북구 안산로 38-3, A동 102호 (오치2동, 영진하우스)'},
    {'lat': 35.20450, 'lon': 126.85110,
     'address': '광주 북구 안산로 40, B동 201호 (오치2동, 대광빌라)'},
    {'lat': 35.20520, 'lon': 126.85000,
     'address': '광주 북구 하백로 46번길 9, 809호(매곡동, 부림@)'},
    # 1.8km 떨어진 별개 덩어리
    {'lat': 35.21500, 'lon': 126.86000,
     'address': '광주 북구 첨단연신로 250, 112동 1205(신용동,첨단휴먼시아)'},
    {'lat': 35.21540, 'lon': 126.86000,
     'address': '광주 북구 첨단연신로 250, 106동 1504호(신용동,첨단휴먼시아)'},
    {'lat': 35.21500, 'lon': 126.86160,
     'address': '광주 북구 첨단연신로 260, 3층 (신용동, 상가)'},
    # 또 다른 덩어리 — 좌표는 60m 떨어졌지만 단지키가 같다
    {'lat': 35.22200, 'lon': 126.87000,
     'address': '광주 광산구 임방울대로 825, 1402호 (수완동, 수완대라수@)'},
    {'lat': 35.22260, 'lon': 126.87000,
     'address': '광주 광산구 임방울대로 825, 1403호 (수완동, 수완대라수@)'},
]

THRESHOLDS = [200, 400]


def build_address_fixture():
    out = []
    for i, addr in enumerate(ADDRESSES):
        dong_num, dong_txt, ho = parse_unit(addr)
        out.append({
            'address': addr,
            'parseUnit': {'dongNum': dong_num, 'dongTxt': dong_txt, 'ho': ho},
            'complexKey': complex_key(addr),
            'stripUnit': strip_unit(addr),
            'unitSortKey': list(unit_sort_key(addr, i)),
        })
    return out


def build_grouping_fixture():
    nodes = [ORIGIN] + DELIVERY_NODES  # Python 인덱스 0 = 출발지
    node_indices = list(range(1, len(nodes)))

    primary = _build_location_groups(node_indices, nodes)
    reps = list(primary.keys())

    # Python 인덱스(1부터) → TS 노드 id(0부터)
    def tid(i):
        return i - 1

    groups = [
        {
            'id': gid,
            'rep': tid(rep),
            'members': [tid(m) for m in primary[rep]],
            'complexKey': complex_key(nodes[rep].get('address', '')),
            'lat': nodes[rep]['lat'],
            'lon': nodes[rep]['lon'],
        }
        for gid, rep in enumerate(reps)
    ]
    rep_to_group_id = {rep: gid for gid, rep in enumerate(reps)}

    clusters = {}
    for th in THRESHOLDS:
        sec = _build_secondary_clusters(reps, nodes, th)
        clusters[str(th)] = [
            {'id': cid, 'groupIds': [rep_to_group_id[r] for r in members]}
            for cid, members in enumerate(sec.values())
        ]

    return {
        'origin': ORIGIN,
        'nodes': [
            {'id': i, 'lat': n['lat'], 'lon': n['lon'], 'address': n['address']}
            for i, n in enumerate(DELIVERY_NODES)
        ],
        'groups': groups,
        'clusters': clusters,
    }


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    written = []
    for name, payload in (
        ('address.json', build_address_fixture()),
        ('grouping.json', build_grouping_fixture()),
    ):
        path = os.path.join(OUT_DIR, name)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
            f.write('\n')
        written.append(os.path.relpath(path, REPO))
    for p in written:
        print(f'wrote {p}')


if __name__ == '__main__':
    main()
