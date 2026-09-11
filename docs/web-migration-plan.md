# 배송 경로 최적화 — Web 전환 계획

> 화면 디자인은 사용자가 Claude Design으로 따로 만든다. 이 문서는 "무엇을, 어떤 순서로" 만들지에 집중한다.

## 1. Context

현재 앱은 Python + tkinter 데스크톱(Windows) 프로그램이다. 카카오 로컬 API로 지오코딩·검증하고, 200m 클러스터 대표 간 카카오 모빌리티 도로시간을 전부 받아 OR-Tools TSP로 전체 순서를 자동 결정한다.

문제는 자동 TSP가 사용자의 현장 감각과 어긋나는 경우가 있고(예: 22→25 사이의 93을 나중에 방문), API 호출 수가 많아 일일 한도·rate-limit 처리가 코드의 상당 부분을 차지하며, 배포가 Windows 실행 파일에 묶여 있다는 점이다.

전환 목표: **GitHub Pages 정적 사이트**로 옮기고, 클러스터 간 순서와 각 클러스터의 진입·이탈 지점은 **사용자가 지도에서 직접 고르게** 하며, 도로시간 API는 **클러스터 내부 순서 계산에만** 쓴다. 주소 파싱·그룹핑·클러스터링·동호 정렬 로직은 그대로 이식한다.

## 2. 확정된 결정

| 항목 | 결정 |
|---|---|
| 프레임워크 | React + Vite + TypeScript |
| 위치 | 이 저장소 `web/` 폴더, GitHub Actions로 `web/`만 빌드해 Pages 배포 |
| 지도 | 카카오맵 Web SDK. JavaScript 키는 사이트에 고정(도메인 제한), 배포자의 카카오 앱 하나를 공유 |
| REST 키 | 사용자가 접속마다 입력, `sessionStorage`에만 보관, 저장소·localStorage에 절대 저장 안 함 |
| 길찾기 API | 클러스터 내부 쌍에만 호출. 클러스터 간 TSP·OR-Tools 제거 |
| 클러스터 표시 | 카카오 `Polygon` + mouseover/mousemove/mouseout/click 이벤트 (샘플 addPolygonMouseEvent2 패턴) |
| 출력 | CSV(utf-8-sig) + xlsx(SheetJS) 다운로드 |

CORS는 확인 완료: 모빌리티 `/v1/directions`와 로컬 `/v2/local/*` 모두 GitHub Pages 오리진의 브라우저 요청을 허용한다.

## 3. 사용자 흐름 (6단계 스테퍼)

1. **시작** — REST 키 입력, 출발지 선택(카카오 우편번호 embed → 지오코딩), 파일 업로드
2. **지오코딩** — 주소→좌표, 역지오코딩 검증, 불일치 항목 수정
3. **클러스터링** — 임계값 슬라이더, 지도에 클러스터 다각형 표시
4. **클러스터 순서** — 다각형을 방문 순서대로 클릭
5. **진입·이탈 지점** — 클러스터를 하나씩 돌며 배송지 마커 중 진입점·이탈점 클릭 → 내부 순서 계산
6. **결과** — 지도 경로선 + 표, CSV/xlsx 다운로드

각 단계 완료 시 상태를 `localStorage`에 저장한다. 재접속 시 "이어서 하기"를 제공한다(REST 키만 다시 입력).

## 4. 프로젝트 구조 (`web/`)

```
web/
  index.html            카카오 SDK <script> (JS 키 고정, autoload=false)
  vite.config.ts        base: '/route-optimizer/'
  src/
    core/               ← Python core 이식. React·DOM 의존 없음, 순수 함수
      address.ts        parse_unit / complex_key / strip_unit / unit_sort_key
      geo.ts            haversineKm
      grouping.ts       buildLocationGroups (1차), buildSecondaryClusters (Union-Find)
      hull.ts           클러스터 다각형 좌표 생성
      intraRoute.ts     클러스터 내부 순서 (진입·이탈 고정)
      verify.ts         verify_address, _normalize, _extract_road_part
    api/
      kakaoLocal.ts     geocode(쿼리 폴백), reverseGeocode
      kakaoMobility.ts  drivingTime + backoff + rate-limit 판정
      pool.ts           동시 3개 프로미스 풀 + 진행률 콜백 + 중단
    io/
      readFile.ts       xlsx/csv 파싱, '택배받을 주소' 열·시트 탐색
      writeFile.ts      CSV / xlsx 생성 + 다운로드
    store/
      session.ts        zustand + persist(localStorage). REST 키는 별도 sessionStorage
    map/
      useKakaoMap.ts    SDK 로드·맵 인스턴스
      ClusterLayer.tsx  Polygon + CustomOverlay 라벨 + 이벤트
      MarkerLayer.tsx   배송지(1차 그룹) 마커, 진입·이탈 상태 표시
      RouteLayer.tsx    결과 Polyline
    screens/            단계별 화면 6개 (디자인은 Claude Design 결과물 반영)
    App.tsx             스테퍼
  src/**/*.test.ts      vitest
.github/workflows/pages.yml
```

주요 라이브러리: `react`, `zustand`(persist), `xlsx`(SheetJS CE), `papaparse`(CSV), `vitest`. 지도는 SDK를 전역 `kakao`로 쓰고 타입 선언만 직접 둔다. 볼록껍질은 monotone chain 30줄이면 되므로 라이브러리 없이 구현한다.

## 5. Python → TypeScript 이식 맵

| Python | 대상 | 변경 |
|---|---|---|
| `core/address.py` 전체 | `core/address.ts` | 정규식 그대로. lookbehind `(?<![가-힣0-9])`는 최신 브라우저 모두 지원 |
| `optimizer._haversine_km` | `core/geo.ts` | 그대로 |
| `optimizer._build_location_groups` | `core/grouping.ts` | 그대로 (좌표 5자리 또는 complex_key 동일) |
| `optimizer._build_secondary_clusters` | `core/grouping.ts` | 그대로. 임계값을 인자로 받아 슬라이더와 연결 |
| `optimizer._nearest_within_cluster` | `core/intraRoute.ts` | **변경**: 진입·이탈 노드 고정, 거리 대신 도로시간 행렬 사용. 같은 단지 블록 단위 유지, 블록 내부 동→호 정렬 유지 |
| `optimizer.build_time_matrix` | `api/kakaoMobility.ts` + `core/intraRoute.ts` | **축소**: 클러스터 내부 블록 쌍만 호출. 실패 시 Haversine 40km/h 대체 유지 |
| `optimizer.optimize_route` 5-1 (OR-Tools TSP) | 삭제 | 사용자 클릭 순서로 대체 |
| `optimizer.optimize_route` 5-3 (동호 펼침) | `core/intraRoute.ts` | 그대로 |
| `optimizer.RateLimitExceededError` 연속 5 / 누적 10 | `api/kakaoMobility.ts` | 유지하되 호출 수가 적어 발생 빈도 낮음. 발생 시 키 재입력 모달 |
| `geocoder.geocode`, `_build_queries` | `api/kakaoLocal.ts` | 그대로 (`fetch`, 429 시 3초 대기) |
| `geocoder.reverse_geocode` | `api/kakaoLocal.ts` | 그대로 |
| `geocoder.verify_address` | `core/verify.ts` | 그대로 |
| `app.py` 1단계 시트·열 탐색 | `io/readFile.ts` | SheetJS로 모든 시트를 훑어 '택배받을 주소' 포함 열 탐색, 없으면 첫 시트 |
| `app.py` 2·3단계 병렬 3개 스레드 | `api/pool.ts` | 프로미스 풀 3개 |
| `AddressSearchDialog` / `AddressFixDialog` (localhost 서버 + Chromium) | 우편번호 embed 모달 | **대폭 단순화**: `daum.Postcode`를 모달 div에 직접 embed → 선택 주소를 지오코딩 |
| `_save_xlsx` | `io/writeFile.ts` | 원본 워크북 읽어 대상 시트에 '배송순서' 열 삽입(있으면 덮어씀), 순서로 행 정렬. **셀 서식은 유지 못함** |
| `settings.py` | `store/session.ts` | 출발지·마지막 임계값은 localStorage, REST 키는 sessionStorage |
| `DoneDialog` | 결과 화면 | 다운로드 버튼 두 개 + 미수정 경고 수 |

## 6. 데이터 모델

- **Row**: 원본 행 그대로(모든 열 보존) + `rowIndex`
- **Node**: `{ id, name, address, lat, lon, kakaoAddr, reverseAddr, verdict, fixed }` — 좌표 있는 행마다 1개
- **PrimaryGroup**: `{ rep: nodeId, members: nodeId[], complexKey }` — 같은 건물. 지도 마커 1개 = 1차 그룹 1개 (멤버 수 배지)
- **Cluster**: `{ id, groups: PrimaryGroup[], centroid, hull: LatLng[], order?: number, entry?: groupId, exit?: groupId, innerOrder?: groupId[], timeMatrix? }`
- **Session**: `{ step, origin, fileName, sheetName, rows, nodes, thresholdM, clusters, clusterOrder: clusterId[], finalOrder: nodeId[], warnCount }`

진입·이탈점은 1차 그룹(건물) 단위로 고른다. 같은 건물 안 순서는 동→호 규칙이 정한다.

## 7. 화면별 필수 요소

### S1. 시작
- REST 키 입력(마스킹 토글), 유효성은 첫 지오코딩 성공으로 확인
- 출발지: "주소 찾기" 버튼 → 우편번호 embed 모달 → 확인된 주소 표시. 지난 출발지 기억
- 파일 드롭존(xlsx/csv). 파싱 후 감지된 시트명·행 수·주소 열명 표시
- "이어서 하기" 배너(저장된 세션 있을 때)
- 안내 문구: 데이터는 브라우저 안에서만 처리되며 주소·좌표만 카카오로 전송

### S2. 지오코딩 · 검증
- 진행률 바(2단계, 3단계 별도), 중단 버튼
- 결과 표: 이름 / 원본 주소 / 카카오 확인주소 / 역지오코딩 주소 / 판정(일치·요확인·확인불가·위치없음·수정됨)
- 판정 필터, 요확인·위치없음 행에 "수정" 버튼 → 우편번호 embed 모달(데스크톱의 팝업 대체, 블로킹 아님)
- "나머지 원본 그대로" 일괄 처리, 다음 단계 버튼(위치없음 건수 경고)

### S3. 클러스터링
- 지도 전체 화면 + 우측 패널
- 임계값 슬라이더(200~1000m, 기본 400m 제안) + 클러스터 수·최대 멤버 수 즉시 표시
- 지도: 클러스터 다각형(옅은 채움), mouseover 시 강조 + 커스텀오버레이로 "클러스터 n · 배송지 k건", click 시 인포윈도우에 멤버 이름 목록
- 배송지 마커(1차 그룹) + 출발지 마커
- 다음 단계 버튼

### S4. 클러스터 순서
- 같은 지도. 다각형 클릭 → 순번 부여, 다각형 중심에 큰 번호 라벨(CustomOverlay), 클릭한 순서대로 중심을 잇는 점선
- 패널: 순서 목록(드래그로 재정렬 가능), "마지막 취소", "전체 초기화"
- 미지정 클러스터 수 표시, 전부 지정해야 다음 단계 활성화

### S5. 진입 · 이탈 지점
- 현재 클러스터만 강조, 나머지는 흐리게. 상단에 "클러스터 3 / 12"
- 마커 클릭 1회 → 진입(초록), 2회째 → 이탈(빨강). 단일 마커 클러스터는 자동
- 자동 제안: 진입 = 직전 클러스터 이탈점에서 가장 가까운 마커, 이탈 = 다음 클러스터 중심에 가장 가까운 마커. "제안 적용" 버튼
- 확정 시 해당 클러스터의 도로시간 호출 → 내부 순서 계산 → 클러스터 안 경로선 표시. 이전/다음 클러스터 이동
- 호출 진행률과 실패(Haversine 대체) 건수 표시

### S6. 결과
- 전체 경로선 + 순번 마커
- 표: 배송순서 / 이름 / 주소 / 클러스터 번호
- CSV 다운로드, xlsx 다운로드(서식 미보존 안내), 미수정 경고 건수
- "처음부터" (세션 삭제)

## 8. 지도 표현 세부

- **다각형 생성** (`core/hull.ts`): 클러스터의 모든 배송지 좌표를 각각 반경 약 40m 원의 8개 점으로 부풀린 뒤 전체의 볼록껍질을 구한다. 점이 1개든 2개든 항상 면적 있는 다각형이 나오고 마커가 경계에 걸리지 않는다.
- **이벤트**: 샘플 그대로 `kakao.maps.event.addListener(polygon, 'mouseover'|'mousemove'|'mouseout'|'click')`. S3는 정보 표시, S4는 click이 순번 부여, S5는 다각형 클릭 비활성·마커 클릭만 활성.
- **레이어 갱신**: 임계값 변경 시 다각형 전부 `setMap(null)` 후 재생성. 순번·진입·이탈은 `setOptions`로 색만 바꾼다.
- **SDK 로드**: `index.html`에 `autoload=false`로 넣고 `kakao.maps.load` 콜백 뒤에만 맵 생성. React StrictMode 이중 마운트에 대비해 맵 인스턴스는 ref로 한 번만 만든다.

## 9. 클러스터 내부 순서 알고리즘 (`core/intraRoute.ts`)

1. 클러스터 멤버(1차 그룹)를 `complex_key`로 블록화. 블록 대표 좌표는 첫 멤버.
2. 블록 대표 쌍 전부에 도로시간 호출(k개면 k(k-1)회). 진입·이탈 블록 포함.
3. 블록 수 ≤ 8이면 진입·이탈 고정 완전탐색(중간 ≤ 6개, 720가지), 초과면 진입에서 NN 후 이탈 블록을 끝에 붙임.
4. 블록 내부는 `unit_sort_key`로 동→호 정렬, 1차 그룹 멤버도 같은 규칙으로 펼침.
5. API 실패 셀은 Haversine 40km/h로 채우고 실패 수를 화면에 알린다.

예상 호출 수: 124건·400m 기준 클러스터 20~30개, 내부 블록 평균 4개면 총 300~500회. 현재 방식(수천 회) 대비 크게 줄어든다.

## 10. 구현 순서

| 단계 | 내용 | 완료 기준 |
|---|---|---|
| M0 | `web/` 스캐폴드, Pages 워크플로(Node 24 — vitest 5 요구), 카카오 앱에 `https://<계정>.github.io` 도메인 등록, 빈 지도 표시 | 배포 URL에서 지도가 뜬다 |
| M1 | `core/` 이식 + vitest. Python 함수를 돌려 만든 fixture(JSON)로 동일 결과 검증 | address·grouping·verify 테스트 통과 |
| M2 | S1·S2: 파일 파싱, REST 키, 출발지, 지오코딩·검증·수정 모달, 세션 저장 | 샘플 CSV 124건이 표에 판정과 함께 뜬다 |
| M3 | S3·S4: 클러스터 다각형, 슬라이더, 순서 클릭 | 순서 목록이 저장되고 새로고침 후 복원된다 |
| M4 | S5: 진입·이탈 선택, 모빌리티 호출, 내부 순서 | 클러스터별 경로선과 순서가 나온다 |
| M5 | S6: 결과 지도·표, CSV/xlsx 다운로드, 이어서 하기, 오류·rate-limit 모달 | 데스크톱 결과와 같은 열 구성의 CSV가 내려온다 |

Claude Design 결과물은 M2 이후 화면별로 붙인다. M0~M1은 디자인 없이 진행 가능.

## 10-1. 멀티에이전트(Opus) 분할 실행

원칙: 에이전트마다 **서로 겹치지 않는 디렉터리**를 맡긴다. 공용 계약(`src/types.ts`)은 0번 에이전트가 먼저 확정하고, 나머지는 그 타입에만 의존한다. 작성과 검토는 다른 에이전트가 맡고, 커밋은 사용자 지시가 있을 때만 한다.

| 웨이브 | 에이전트 | 담당 경로 | 산출물 / 완료 기준 |
|---|---|---|---|
| W0 (단독) | executor · opus | `web/` 스캐폴드, `index.html`, `vite.config.ts`, `src/types.ts`, `src/store/session.ts` 뼈대, vitest 설정, `.github/workflows/pages.yml`, `docs/web-migration-plan.md` | `npm run build`·`npm test` 통과, 빈 지도 표시. 이후 웨이브의 계약(types) 확정 |
| W1-A | executor · opus | `src/core/**` + 테스트 | Python `address.py`·`optimizer.py` 그룹핑 함수로 fixture JSON 생성 → TS 결과 일치. hull·intraRoute 포함 |
| W1-B | executor · opus | `src/api/**`, `src/io/**` + fetch-mock 테스트 | geocode 폴백, backoff·rate-limit 분기, xlsx/csv 읽기·쓰기(샘플 CSV로 왕복 검증) |
| W1-C | executor · opus | `src/map/**` | SDK 로드 훅, Polygon 이벤트 4종·CustomOverlay·InfoWindow 레이어, 마커·경로 레이어. props로만 데이터 받음 |
| W2 (단독) | executor · opus | `src/screens/**`, `src/App.tsx`, store 완성 | S1~S6 스테퍼 연결, localStorage 복원, 우편번호 embed 모달. 샘플 CSV로 S1→S6 완주 |
| W3-A | code-reviewer · opus (읽기 전용) | 전체 `web/` | 심각도별 리뷰 → W2 에이전트가 수정 |
| W3-B | verifier · opus | 전체 | 테스트·빌드·Pages 배포 로그 확인, 검증 체크리스트(13절) 결과 보고 |

W1의 세 에이전트는 병렬이며 같은 워킹트리를 쓰되 경로가 겹치지 않아 충돌이 없다. W2는 W1 결과를 통합하므로 단독으로 돈다. Claude Design 결과가 준비되면 W2 안에서 화면별로 반영한다.

## 11. 배포

- Vite `base`를 저장소 이름으로. Pages는 Actions 방식(`actions/deploy-pages`), 워크플로는 `web/**` 변경 시만 실행.
- 카카오 개발자 콘솔: 지도를 켤 앱은 **길찾기를 쓰는 그 앱**이어야 무료 쿼터가 적용된다(2026-07-21 정책: 계정당 첫 활성화 앱만 무료). 플랫폼 > Web에 Pages 도메인 등록, JavaScript 키를 `index.html`에 고정.
- 로컬 개발용으로 `http://localhost:5173`도 도메인에 등록.

## 12. 리스크와 남은 결정

- **xlsx 서식 손실**: SheetJS CE는 셀 색·테두리를 못 쓴다. 값·열 순서·시트는 유지. 안내 문구로 처리.
- **PII 보관**: 세션 저장에 원본 행(주민번호·전화)이 들어간다. 완료 시 "처음부터"로 지우고, 결과 다운로드 후 자동 삭제할지는 구현 중 결정.
- **REST 키 재입력**: 탭을 닫으면 사라지므로 "이어서 하기"는 키 입력부터 시작한다.
- **클러스터 수**: 200m면 65개로 순서 클릭이 과하다. 기본 400m로 두고 슬라이더로 조절. 병합·분할 편집은 MVP 이후.
- **모바일**: 지도 클릭 UI라 데스크톱·태블릿 우선. 폰은 보장하지 않는다.
- **Python 앱 유지**: 전환 완료 전까지 `src/`는 그대로 둔다. 삭제 여부는 별도 결정.

## 13. 검증

- **단위**: `core/*.test.ts`. Python `address.py` docstring 예제 + 샘플 CSV 124건에 대해 Python `complex_key`·`unit_sort_key`·클러스터링 결과를 fixture로 뽑아 TS 결과와 일치 확인.
- **API 계층**: `fetch`를 mock해 429·400(code -10)·5xx·timeout 분기와 backoff 순서 확인.
- **수동 E2E**: 샘플 CSV로 S1→S6 완주. 확인 항목: 지오코딩 124/124, 클러스터 수 표시, 순서 클릭·취소·복원, 진입·이탈 자동 제안, 호출 수 로그, CSV 열 구성이 데스크톱 산출물(`*_배송순서완성.csv`)과 동일, xlsx에 '배송순서' 열이 있으면 그 자리에 덮어쓰고 없으면 1열에 추가되며(데스크톱과 동일), 행이 순서대로 정렬됨.
- **배포**: Pages URL에서 지도·지오코딩·길찾기 세 호출이 브라우저 콘솔 CORS 오류 없이 동작.
