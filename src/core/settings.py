"""
settings.py
사용자 설정 저장/불러오기 (API 키, 출발지 주소).

매 실행마다 API 키를 붙여넣고 창고 주소를 다시 검색하는 불편을 없애기 위해
사용자 폴더에 JSON 한 개로 보관한다.

저장 위치:
  Windows : %LOCALAPPDATA%\\DeliveryRouteOptimizer\\settings.json
  macOS   : ~/Library/Application Support/DeliveryRouteOptimizer/settings.json
  기타    : ~/.config/DeliveryRouteOptimizer/settings.json

API 키는 개인 PC용 도구라는 전제로 평문 저장한다.
공용 PC 등에서 원치 않으면 UI의 '키 기억' 체크를 끄면 저장하지 않는다.

설정 파일을 못 읽거나 못 쓰더라도 프로그램 동작에는 영향이 없어야 하므로
모든 실패는 조용히 무시하고 기본값으로 진행한다.
"""

import json
import os
import sys

_APP_DIR_NAME = 'DeliveryRouteOptimizer'
_FILE_NAME = 'settings.json'

DEFAULTS = {
    'api_key': '',
    'remember_api_key': True,
    'origin_address': '',
    'origin_lat': None,
    'origin_lon': None,
    'last_dir': '',
}


def _config_dir() -> str:
    if sys.platform == 'win32':
        base = os.environ.get('LOCALAPPDATA') or os.path.expanduser('~')
    elif sys.platform == 'darwin':
        base = os.path.expanduser('~/Library/Application Support')
    else:
        base = os.environ.get('XDG_CONFIG_HOME') or os.path.expanduser('~/.config')
    return os.path.join(base, _APP_DIR_NAME)


def config_path() -> str:
    return os.path.join(_config_dir(), _FILE_NAME)


def load() -> dict:
    """저장된 설정을 읽는다. 없거나 깨졌으면 기본값."""
    data = dict(DEFAULTS)
    try:
        with open(config_path(), encoding='utf-8') as fp:
            saved = json.load(fp)
        if isinstance(saved, dict):
            for k in DEFAULTS:
                if k in saved:
                    data[k] = saved[k]
    except Exception:
        pass  # 최초 실행 / 손상 / 권한 없음 — 기본값으로 진행
    if not data.get('remember_api_key'):
        data['api_key'] = ''
    return data


def save(data: dict) -> bool:
    """설정을 저장한다. 실패해도 예외를 올리지 않고 False만 반환."""
    payload = {k: data.get(k, DEFAULTS[k]) for k in DEFAULTS}
    if not payload.get('remember_api_key'):
        payload['api_key'] = ''
    try:
        os.makedirs(_config_dir(), exist_ok=True)
        tmp = config_path() + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as fp:
            json.dump(payload, fp, ensure_ascii=False, indent=2)
        os.replace(tmp, config_path())   # 중간에 끊겨도 기존 파일이 안 깨지게
        return True
    except Exception:
        return False
