"""
address.py
한국 주소 문자열에서 건물 단위(동/호/층)를 읽어내는 공용 유틸.

optimizer(그룹핑·정렬)와 geocoder(지오코딩 질의 생성)가 같은 규칙을 쓰도록
파싱을 한곳에 모았다.

핵심 난점은 '동'이 두 가지 뜻으로 쓰인다는 점이다.
  - 법정동/행정동 : 운암동, 운암1동, 오치2동, 문흥1동  → 주소의 일부
  - 건물 동       : 101동, 201동, A동, C동            → 건물 식별자
숫자 앞에 한글이 붙으면 법정동으로 보고 건드리지 않는다.

공개 API:
  - parse_unit(address)  → (동번호, 동문자, 호수)   못 찾으면 각각 None
  - complex_key(address) → 같은 단지/건물을 하나로 묶기 위한 정규화 키
  - strip_unit(address)  → 동/호/층을 지운 주소 (지오코딩 질의용, 원문 형태 유지)
"""

import re

# 괄호 주기 — '(신용동,용두주공@)', '(두암동 871-28)' 등
_PAREN_RE = re.compile(r'\([^)]*\)')

# 건물 동(숫자) — 앞에 한글/숫자가 붙으면 법정동이거나 다른 번호이므로 제외.
# '제201동' 표기도 함께 받는다.
_DONG_NUM_RE = re.compile(r'(?<![가-힣0-9])제?\s*(\d+)\s*동(?![가-힣])')

# 건물 동(영문) — 'A동', 'C동'
_DONG_ALPHA_RE = re.compile(r'(?<![가-힣A-Za-z0-9])([A-Za-z])\s*동(?![가-힣])')

# 호수 — '1402호', '403호'. '호반리젠시빌'처럼 숫자 없는 '호'는 매칭되지 않는다.
_HO_RE = re.compile(r'(\d+)\s*호(?![가-힣])')

# 층 — '3층', '2 층'
_FLOOR_RE = re.compile(r'제?\s*\d+\s*층')

# 구분자 (정규화 키에서 통째로 제거)
_SEP_RE = re.compile(r'[\s,]+')


def parse_unit(address: str) -> tuple:
    """주소에서 (동번호, 동문자, 호수)를 뽑는다.

    동번호와 동문자는 배타적이다 — 숫자 동이 있으면 동문자는 None.

    >>> parse_unit('광주 북구 삼정로 7, 207동 403호(두암동, 주공2단지@)')
    (207, None, 403)
    >>> parse_unit('광주 북구 안산로 38-3, A동 102호 (오치2동, 영진하우스)')
    (None, 'A', 102)
    >>> parse_unit('광주 북구 하백로 46번길 9, 809호(매곡동, 부림@)')
    (None, None, 809)
    >>> parse_unit('광주 북구 첨단연신로 250, 112동 1205(신용동,첨단휴먼시아)')
    (112, None, 1205)
    """
    if not address:
        return None, None, None

    # 괄호 안에는 법정동·아파트명이 들어 있어 오탐만 만든다 → 먼저 제거
    s = _PAREN_RE.sub(' ', address)

    dong_num = dong_txt = None
    m = _DONG_NUM_RE.search(s)
    if m:
        dong_num = int(m.group(1))
    else:
        m = _DONG_ALPHA_RE.search(s)
        if m:
            dong_txt = m.group(1).upper()

    # 호수는 동 뒤쪽에서만 찾는다 (앞쪽 도로명 번호를 호수로 오인하지 않도록)
    rest = s[m.end():] if m else s

    mh = _HO_RE.search(rest)
    if mh:
        return dong_num, dong_txt, int(mh.group(1))

    # '호' 접미사 없이 숫자만 적힌 표기 — '112동 1205'
    # 번지형('20-1')은 호수가 아니므로 제외
    if m:
        mn = re.match(r'\s*(\d+)(?![\d\-])', rest)
        if mn:
            return dong_num, dong_txt, int(mn.group(1))

    return dong_num, dong_txt, None


def complex_key(address: str) -> str:
    """같은 단지/건물을 하나로 묶기 위한 정규화 키.

    동·호·층과 괄호 주기를 지우고 공백·쉼표까지 없앤다.
    표기 흔들림('(매곡동,아남@)' vs '(매곡동, 아남@)')으로 같은 단지가
    갈라지는 것을 막기 위해서다.

    >>> complex_key('광주 북구 매곡로 92, 102동 106호(매곡동,아남@)')
    '광주북구매곡로92'
    >>> complex_key('광주 북구 매곡로 92, 101동 105호(매곡동, 아남@)')
    '광주북구매곡로92'
    """
    if not address:
        return ''
    s = _PAREN_RE.sub(' ', address)
    s = _DONG_NUM_RE.sub(' ', s)
    s = _DONG_ALPHA_RE.sub(' ', s)
    s = _HO_RE.sub(' ', s)
    s = _FLOOR_RE.sub(' ', s)
    s = _SEP_RE.sub('', s)
    return s.lower()


def strip_unit(address: str) -> str:
    """동/호/층만 지운 주소. 괄호와 띄어쓰기는 그대로 둔다.

    지오코딩 질의용 — 카카오 로컬 API는 상세 호수가 붙으면 실패하지만
    괄호 안 법정동·건물명은 오히려 정확도를 높여준다.
    """
    if not address:
        return ''
    s = _DONG_NUM_RE.sub(' ', address)
    s = _DONG_ALPHA_RE.sub(' ', s)
    s = _HO_RE.sub(' ', s)
    s = _FLOOR_RE.sub(' ', s)
    s = re.sub(r'\s+', ' ', s)
    return s.strip().rstrip(',').strip()


def unit_sort_key(address: str, tiebreak) -> tuple:
    """같은 단지 안에서 '동 오름차순 → 호 오름차순'으로 세우기 위한 정렬 키.

    숫자 동 → 영문 동 → 동 없음 순으로 묶고, 동/호를 못 읽은 항목은 뒤로 민다.
    마지막 tiebreak(보통 노드 인덱스)로 항상 같은 결과가 나오게 한다.
    """
    dong_num, dong_txt, ho = parse_unit(address)
    if dong_num is not None:
        rank, num, txt = 0, dong_num, ''
    elif dong_txt:
        rank, num, txt = 1, 0, dong_txt
    else:
        rank, num, txt = 2, 0, ''
    return (rank, num, txt,
            0 if ho is not None else 1, ho or 0,
            tiebreak)
