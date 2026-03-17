# route-optimizer

> 카카오 API 기반 배송 경로 자동 최적화 — Windows GUI 프로그램

## 사용자 다운로드

**[Releases](../../releases/latest)** 페이지에서 `배송경로자동정리.exe` 를 받아 더블클릭으로 실행.  
Python 설치 불필요.

---

## 동작 흐름

```
엑셀 파일 (.xlsx)
    ↓
카카오 API — 각 배송지 위도/경도 추출
    ↓
카카오 API — 역지오코딩으로 주소 정확도 검사
    (불일치 시 팝업에서 사용자가 직접 수정 + 재검증)
    ↓
카카오 모빌리티 API — N×N 자동차 주행 시간 행렬 구축
    ↓
OR-Tools (SAVINGS + GLS) — 최적 배송 순서 결정
    ↓
원본 엑셀에 배송순서 열 추가 → _배송순서완성.xlsx 저장
```

---

## 신규 릴리즈 빌드 방법 (개발자)

```bash
# 1. 버전 태그 생성
git tag v1.0.0

# 2. 태그 push → GitHub Actions 자동 빌드 → Release 생성
git push origin v1.0.0
```

Actions 탭에서 빌드 진행 상황 확인 가능.  
완료 후 Releases 페이지에 `.exe` 자동 업로드됨.

---

## 로컬 개발 실행

```bash
pip install -r requirements.txt
python src/app.py
```

## 프로젝트 구조

```
route-optimizer/
├─ src/
│   ├─ app.py              # GUI 메인
│   └─ core/
│       ├─ geocoder.py     # 주소 검색 / 위치 확인 / 정확도 검사
│       └─ optimizer.py    # 이동 시간 행렬 + OR-Tools 최적화
├─ .github/
│   └─ workflows/
│       └─ build.yml       # Windows .exe 자동 빌드
└─ requirements.txt
```
