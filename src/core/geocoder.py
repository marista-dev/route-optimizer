"""
geocoder.py
카카오 로컬 API 래퍼
  - geocode        : 주소 → 위도/경도
  - reverse_geocode: 위도/경도 → 도로명 주소
  - verify_address : 원본 주소 vs 역지오코딩 결과 정규화 비교
"""

import re
import time
import requests

# HTTP keep-alive 세션 (병렬 스레드 안전)
_SESSION = requests.Session()


# 지오코딩

def _build_queries(address: str) -> list:
    queries = [address]
    if ',' in address:
        queries.append(address.split(',')[0].strip())
    no_bracket = re.sub(r'\(.*?\)', '', address).strip()
    if no_bracket not in queries:
        queries.append(no_bracket)
    stripped = re.sub(r'\d+동\s*\d+호|\d+층.*', '', address).strip().rstrip(',').strip()
    if stripped not in queries:
        queries.append(stripped)
    return queries


def geocode(address: str, headers: dict) -> dict | None:
    """주소 → {'lat', 'lon', 'kakao_road_addr'} or None"""
    url = 'https://dapi.kakao.com/v2/local/search/address.json'
    for query in _build_queries(address):
        try:
            resp = _SESSION.get(url, headers=headers,
                                params={'query': query}, timeout=7)
            if resp.status_code == 200:
                docs = resp.json().get('documents', [])
                if docs:
                    doc = docs[0]
                    road  = doc.get('road_address')
                    jibun = doc.get('address')
                    road_addr = (road['address_name'] if road
                                 else jibun['address_name'] if jibun else '')
                    return {'lat': float(doc['y']),
                            'lon': float(doc['x']),
                            'kakao_road_addr': road_addr}
            elif resp.status_code == 429:
                time.sleep(3)
        except Exception:
            time.sleep(1)
    return None


def reverse_geocode(lat: float, lon: float, headers: dict) -> str:
    """위도/경도 → 도로명(또는 지번) 주소 문자열"""
    url = 'https://dapi.kakao.com/v2/local/geo/coord2address.json'
    try:
        resp = _SESSION.get(url, headers=headers,
                            params={'x': lon, 'y': lat,
                                    'input_coord': 'WGS84'}, timeout=7)
        if resp.status_code == 200:
            docs = resp.json().get('documents', [])
            if docs:
                road = docs[0].get('road_address')
                if road:
                    return road.get('address_name', '')
                addr = docs[0].get('address')
                if addr:
                    return addr.get('address_name', '')
    except Exception:
        pass
    return ''


# 주소 검증

def _normalize(s: str) -> str:
    s = re.sub(r'광주광역시', '광주', s)
    s = re.sub(r'\(.*?\)', '', s)
    s = re.sub(r'\s+', '', s)
    return s.lower()


def _extract_road_part(s: str) -> str:
    """쉼표·괄호·동호수 제거 후 구/군 이후 도로명만 반환"""
    s = s.split(',')[0].strip()
    s = re.sub(r'\(.*?\)', '', s).strip()
    parts = s.split()
    for i, p in enumerate(parts):
        if p.endswith('구') or p.endswith('군'):
            return ''.join(parts[i + 1:])
    return ''.join(parts)


def verify_address(original: str, reverse_addr: str) -> str:
    """
    Returns
    -------
    '일치' | '요확인(<역주소>)' | '확인불가'
    """
    if not reverse_addr:
        return '확인불가'
    orig = _normalize(_extract_road_part(original))
    rev  = _normalize(_extract_road_part(reverse_addr))
    if not orig or not rev:
        return '확인불가'
    if orig == rev or orig in rev or rev in orig:
        return '일치'
    return f'요확인({reverse_addr})'
