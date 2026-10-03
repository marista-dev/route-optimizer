# `src/map` — 지도 레이어

카카오맵 Web SDK를 감싼 React 레이어 묶음이다. **스토어를 직접 읽지 않는다.**
모든 데이터는 props로 받으므로, 화면(`src/screens/*`)이 zustand에서 값을 꺼내 내려주면 된다.

## 구성

| 파일 | 역할 |
|---|---|
| `useKakaoMap.ts` | `kakao.maps.load` 뒤에 지도를 **한 번만** 생성. `{ map, error, fitBounds, relayout, panBy, whenIdle }` (함수들은 정체성이 바뀌지 않는다) |
| `MapContext.ts` | `map`·`fitBounds`·`relayout`·`panBy(dx, dy)`·`whenIdle(timeoutMs)`를 레이어에 내려주는 컨텍스트 (`useMapContext()`). `whenIdle`은 지도가 멈추고(`idle`) 타일을 받은 뒤(`tilesloaded`, 안 오면 잠깐 뒤) 또는 시간 초과에 끝나는 Promise |
| `MapCanvas.tsx` | 전체 크기 지도 컨테이너. 훅을 소유하고 children을 컨텍스트로 감싼다 |
| `ClusterLayer.tsx` | 클러스터 다각형 + hover 라벨 + 순번 라벨 + 인포윈도우 |
| `MarkerLayer.tsx` | 1차 그룹 마커(멤버 수 배지·진입/이탈 색·순번 라벨) + 출발지 마커 |
| `MarkerPositions.tsx` | 그리지 않는 레이어. 1차 그룹 마커의 화면 좌표(컨테이너 기준 px)를 지도 이동·확대·창 크기 변경 때마다 콜백으로 올린다(S6 캡처의 영역 판정용) |
| `MapInteraction.tsx` | 그리지 않는 레이어. 사용자가 지도를 끌거나 확대하기 시작하면(`dragstart`/`zoom_start`) `onInteractStart`, 멈추면(`idle`) `onInteractEnd`(S6 캡처 독·라벨 흐리기용). 끌기 도중(`dragstart`~`dragend`)의 `idle`은 `dragend`까지 미룬다 |
| `overlayPress.ts` | 클릭 가능한 오버레이의 누르기 처리 `attachOverlayPress`(아래 "오버레이 클릭과 지도 끌기") + 순수 판정 `exceededSlop`·`isRepeatClick`·`createRepeatGuard`(`overlayPress.test.ts`) |
| `MapBridge.tsx` | 그리지 않는 레이어. 컨텍스트 값(`panBy`·`whenIdle`·`relayout` 등)을 `onChange`로 올려, `MapCanvas` 밖의 화면 훅이 이벤트 처리기에서 지도 명령을 쓸 수 있게 한다(S6 캡처의 `다음 구역`·자동 저장) |
| `RouteLayer.tsx` | 좌표열들을 Polyline으로 |
| `OrderLinkLayer.tsx` | 클러스터 중심을 방문 순서대로 잇는 점선 |
| `RefPointLayer.tsx` | 클릭되지 않는 참고점 라벨(S5의 이전 위치 · 다음 클러스터 중심) |
| `palette.ts` | 다각형·마커·경로선이 공유하는 의미 있는 색(`MAP_COLOR`). `map.css`는 같은 값을 `src/index.css`의 CSS 변수로 쓴다 |

`MapCanvas`가 `map.css`를 import하므로 화면 쪽에서 CSS를 따로 넣을 필요는 없다.

## 오버레이 zIndex

겹칠 때 무엇이 위로 올라오는지는 아래 순서로 고정돼 있다. 새 레이어를 넣을 때 이 표를 지킬 것.

| zIndex | 오버레이 | 비고 |
|---|---|---|
| 3 | (비어 있음) | |
| 4 | `RefPointLayer` 참고점, `ClusterLayer` hover 라벨 | 클릭 안 됨 |
| 5 | `MarkerLayer` 1차 그룹 마커 | |
| 6 | `MarkerLayer` 출발지 | |
| 7 | `ClusterLayer` 중심 클릭 타깃(`.ro-cluster-hit`) | `onClusterClick`/`info` 모드에서만 생성 |
| 8 | `MarkerLayer` 강조된 마커 | S6 표 행 hover, 클릭 가능한 마커에 마우스를 올린 동안 |
| 9 | `MarkerLayer` 강조 라벨 | |
| 10 | `ClusterLayer` 방문 순번 숫자 | 무엇에도 가리지 않는다 |

순번 숫자가 마커보다 위인 이유: 1건짜리 클러스터는 중심이 그 그룹의 좌표와 같아 마커·클릭 타깃과
정확히 겹친다. 그 경우에는 숫자를 마커 위쪽으로 비켜 찍기까지 한다(`yAnchor`).

### 지도 위 DOM(카카오 오버레이 아님)

S6 캡처 모드의 고정 A4 프레임(`components/CaptureFrame`)과 하단 독(`components/CaptureToolbar`)은
카카오 오버레이가 아니라 지도 컨테이너와 같은 자리에 얹는 일반 DOM이다. 그래서 위 표의 zIndex와는
쌓임 맥락이 다르다. `.ro-map__canvas`에 `isolation: isolate`를 걸어 SDK 내부 z-index가 바깥으로
새지 않게 했으므로, 바깥 요소는 CSS `z-index`만으로 순서가 정해진다.

| CSS z-index | 요소 | 비고 |
|---|---|---|
| 10 | `.ro-panel`(SidePanel), `.ro-pill` | 캡처 모드에서는 패널을 렌더하지 않는다 |
| 12 | `.ro-capture` 프레임·어두운 처리·라벨·잘림 링·✓ 배지 | 모두 포인터를 통과시킨다(라벨 칩도 — 지도 끌기를 막지 않게 tooltip을 뺐다) |
| 14 | `.ro-capture-dock` | 지도 조작 중에는 흐려지고 포인터를 통과시킨다 |
| 20 | `.ro-header` | 캡처 모드(`html.ro-capture-mode`)에서는 위로 접혀 6px만 남고, hover·포커스 때 내려온다 |
| 50 | `.ro-modal`(캡처 미리보기 포함) | |
| 60 | `.ro-toast` | |

## 오버레이 클릭과 지도 끌기

카카오 `CustomOverlay`를 `clickable: true`로 만들면 그 위에서 시작한 누르기·끌기가 지도로 가지 않는다.
그대로 DOM `click`만 달면 지도를 옮기려고 마커를 누르고 끈 손이 떼는 순간 클릭이 된다(S5 진입·이탈이
바뀌고 S6 표가 튄다). 그래서 클릭 가능한 오버레이(`MarkerLayer`의 클릭 가능한 마커, `ClusterLayer`의
중심 히트 타깃)는 `click`을 직접 달지 말고 `attachOverlayPress(el, getMap, { onClick, ... })`를 쓴다.

- 누른 뒤 5px(slop)을 넘게 움직이면 클릭을 취소하고, 포인터를 잡은 채 이동량만큼 `setCenter`로 지도를
  **직접** 옮긴다(애니메이션 없음). 제자리에서 뗀 한 번만 `onClick`이다.
- 더블클릭의 두 번째는 무시한다(DOM `click`의 `detail > 1`이면서 직전 클릭도 같은 요소일 때, 그리고 요소별 300ms 시간 가드). pointer 이벤트의
  `detail`은 명세상 늘 0이라 클릭 판정은 `click` 이벤트에서 한다.
- 이 끌기는 SDK 끌기가 아니라 지도 `dragstart`/`dragend`가 저절로 오지 않는다. 레이어가
  `mapDragNotifier(getMap)`로 `kakao.maps.event.trigger(map, 'dragstart' | 'dragend')`를 직접 내서,
  `MapInteraction`(S6 캡처 독 흐리기·자동 저장 중지)이 SDK 끌기와 똑같이 받는다. `setCenter` 뒤 `idle`은
  SDK가 낸다.
- 클릭할 수 없는 오버레이는 `clickable: false`(기본)로 두면 끌기가 그대로 지도로 간다. 붙이지 말 것.
- 카카오 다각형 `click`은 DOM 이벤트가 아니라 `detail`이 없으므로, `ClusterLayer`는 같은 클러스터를
  300ms 안에 다시 누르면 무시한다(`createRepeatGuard`, 히트 타깃과 공유). S4에서 더블클릭이 순번을
  매겼다 바로 해제하지 않게 한다.

### 선택·커서

- `.ro-map` 아래 전체는 `user-select: none`이다(지도 글자에 파란 선택 블록이 생기지 않게). `MapCanvas`가
  `selectstart`·`dragstart`를 막고, 누를 때 이미 걸린 선택을 지운다(입력 칸은 예외).
- 클릭되지 않는 오버레이는 `cursor: inherit`로 지도 커서(손바닥)를 따르고, 클릭되는 것만 `pointer`다.
- 클릭 가능한 마커는 hover 때 살짝 커지고 바깥 링(`outline`)이 생기며, `MarkerLayer`가 zIndex를 강조값(8)으로
  올린다. zIndex는 `applyStates`가 강조 여부와 hover 여부로 함께 정한다(아래 표의 값은 그대로다).

## 공통 규칙

- 카카오 객체는 `map`이 생긴 뒤에만 만든다. 레이어는 `map === null`이면 아무것도 하지 않는다.
- `clusters` / `groups` / `paths` **배열 정체성이 바뀌면 전부 재생성**한다. 매 렌더 새 배열을 만들지 말고
  `useMemo`로 감싸 넘길 것(안 그러면 매 렌더 폴리곤이 다시 그려진다).
- 모드·순번·진입/이탈 같은 상태 변화는 재생성 없이 `setOptions` / 클래스 교체로만 반영된다.
- `orderOf`, `orderLabel`, `onClusterClick` 등 콜백은 매 렌더 새로 만들어도 되지만
  `useCallback`으로 감싸면 불필요한 스타일 재적용을 줄일 수 있다.

화면별 조합은 각 화면 코드를 보라.

## 오류 처리

JS 키가 없거나 SDK 스크립트가 실패하면 `useKakaoMap`의 `error`에 안내 문구가 담기고
`MapCanvas`가 그대로 보여준다. 이 경우 `map`은 계속 null이라 레이어들은 조용히 아무것도 하지 않는다.
