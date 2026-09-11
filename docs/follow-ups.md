# 후속 작업 목록

릴리스 전 점검(2026-09-12)에서 나온 항목 중 **출시 후로 미룬 것**들이다.
"지금 하지 않기로 한 판단"과 그 이유를 남겨, 나중에 같은 고민을 다시 하지 않게 한다.

전제: 사용자 1명(배송 코디네이터), 주 몇 회, 데스크톱 브라우저, 유지보수자 1명.
**과설계도 결함으로 취급한다.** 아래 항목은 모두 "없어도 도구는 동작한다".

## 값 순서

| # | 항목 | 이유 | 규모 |
|---|---|---|---|
| L1 | S2 지오코딩 부분 결과 보존 | 중단 후 재시작이 받아 둔 좌표를 버려 API를 두 번 태운다. 릴리스에는 50건 초과 시 확인 대화상자만 넣었다. 근본 해결은 `runPool`의 부분 결과를 살려 이미 받은 좌표를 건너뛰는 것 — S5의 도로시간 경로(`collectedRef` + `known`)에 이미 같은 패턴이 구현돼 있으니 그대로 옮기면 된다 | 중 |
| L2 | `EntryExitScreen` 3분할 | 644줄. 길이 자체가 아니라 `confirm()`의 상태기계(rate-limit·haversine 모드·부분 결과 캐시)가 JSX 사이에 끼어 읽는 순서가 강제되지 않는 것이 문제. `screens/useClusterRouting.ts`로 떼면 **테스트가 하나도 없는 유료 쿼터 경로**에 테스트를 붙일 수 있다 | 중 |
| L3 | `Cluster`의 optional 5필드 제거 | `Cluster.entry/exit/innerOrder/timeMatrix/haversineFallbacks`, `store/session.ts`의 `ClusterPick`, `helpers.ts`의 `ClusterPicks` — 같은 모양이 세 번 선언돼 있다. `Cluster`를 순수 기하(id/groupIds/centroid/hull)로 줄이고 `ClusterPick`을 `types.ts`의 유일 정의로 삼으면 `withPicks`가 통째로 사라진다. `types.ts`(계약)를 건드리므로 미뤘다 | 소 |
| L4 | `useGroupLabels` 훅 | 같은 마커를 화면마다 다른 이름으로 부른다(S3는 단지명, S5는 단지명+건수, S6는 단지명). 코드량 이득은 거의 0이고 **일관성**이 이유다 | 소 |
| L5 | `ResultScreen` 3분할 | 538줄. 그 자체로 결함은 아니다. 재업로드 검증·다운로드(개인정보 경로)에 테스트를 붙이고 싶어질 때만 하면 된다 | 중 |
| L6 | `.ro-s6` → `.ro-panel--s6` | S3~S5는 `ro-panel--sN`인데 S6만 `ro-s6`라 CSS 규칙이 갈렸다. `ro-s1`/`ro-s2`는 화면 루트 클래스라 `ro-sN`이 두 뜻으로 쓰인다 | 소 |
| L7 | xlsx 라이브러리 지연 로딩 | 번들 807kB(gzip 260kB)의 대부분이 SheetJS다. 첫 화면에는 필요 없지만(CSV 업로드는 papaparse), 지연 로딩하려면 `buildCsv`/`buildXlsx`가 async가 되어 `ResultScreen`까지 파급된다. 데스크톱 1인 사용자에게 gzip 260kB는 체감되지 않아 미뤘다 | 중 |
| L8 | `partialize`로 `rows` 분리 | 모든 액션이 `savedAt`을 갱신해 매번 원본 행 전체를 localStorage에 다시 쓴다. 124행 ≈ 200KB라 체감되지 않지만, 행이 1000을 넘으면 첫 병목이 여기다 | 소 |

### 타입 검사 게이트에 대한 주의

`npx tsc --noEmit -p .`는 **아무것도 검사하지 않고 조용히 통과한다.** 이 저장소의 루트 `tsconfig.json`이
`references`만 가진 solution-style 설정이라 `--noEmit`과 결합하면 참조 프로젝트를 타지 않는다.
`src/types.ts`에 일부러 타입 오류를 넣고 확인했다 — `-p .`는 exit 0, `tsc -b`는 오류 2건을 잡았다.

실제 게이트는 `npm run typecheck`(= `tsc -b --force`) 또는 `npm run build`(`tsc -b && vite build`)다.
CI(`pages.yml`)는 `npm run build`를 돌리므로 이미 덮여 있다. 손으로 확인할 때만 주의하면 된다.

## 하지 않기로 한 것 (다시 꺼내지 말 것)

| 항목 | 이유 |
|---|---|
| `<MapScreen>` 공통 껍데기 | 화면 넷이 공유하는 건 4줄뿐이고 레이어 조합은 전부 다르다. 16줄 아끼려고 파일과 간접 계층을 더하는 건 순손해 |
| `<MoveButtons>` 컴포넌트 | 두 호출부의 이벤트 처리(`stopPropagation`)와 aria 문구가 달라 props만 늘고 호출부는 안 짧아진다 |
| `GeocodeScreen`을 `useAbortable`로 통일 | effect 정리 함수 생명주기와 훅의 `start/abort` 모양이 맞지 않는다. 억지로 맞추면 코드가 는다 |
| `DataTable` 분리 | props가 13개지만 두 표가 스티키 헤더·빈 상태·행 클래스를 실제로 공유한다. 쪼개면 드래그 로직이 화면으로 샌다. 세 번째 표가 생기면 그때 |
| ErrorBoundary | 세션이 localStorage에 있어 렌더 실패는 새로고침으로 복구된다 |
| 로깅·텔레메트리·분석 | 사용자 1명 |
| localStorage 용량 초과 처리 | 124행 ≈ 한도의 4% |
| `clusterPicks` 변경 시 경로선 전체 재생성 | 클러스터 30개면 1ms 미만. 고치려면 스토어 슬라이스 분리나 캐시가 필요해 이득보다 복잡하다. 클러스터 100개를 넘기면 여기가 첫 병목 |

## 운영자가 해야 하는 설정

| 항목 | 내용 |
|---|---|
| 카카오 JavaScript 키 | 빌드된 HTML에 그대로 들어간다. **도메인 제한이 유일한 방어**다. 카카오 콘솔 > 플랫폼 > Web에 Pages 도메인과 `http://localhost:5173`만 등록할 것 |
| 카카오 REST 키 | 사이트에 넣지 않는다. 사용자가 접속할 때마다 입력하고 `sessionStorage`에만 남으며 탭을 닫으면 사라진다 |
| 무료 쿼터 | 2026-07-21 정책상 개발자 계정의 **첫 번째로 활성화한 앱**에만 무료 쿼터가 붙는다. 지도를 켤 앱은 길찾기를 쓰는 그 앱이어야 한다 |
| GitHub Actions | 저장소 Settings > Variables에 `VITE_KAKAO_JS_KEY` 등록 |
