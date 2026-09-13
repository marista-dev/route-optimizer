# `src/map` — 지도 레이어

카카오맵 Web SDK를 감싼 React 레이어 묶음이다. **스토어를 직접 읽지 않는다.**
모든 데이터는 props로 받으므로, 화면(`src/screens/*`)이 zustand에서 값을 꺼내 내려주면 된다.

## 구성

| 파일 | 역할 |
|---|---|
| `useKakaoMap.ts` | `kakao.maps.load` 뒤에 지도를 **한 번만** 생성. `{ map, error, fitBounds }` |
| `MapContext.ts` | `map`·`fitBounds`를 레이어에 내려주는 컨텍스트 (`useMapContext()`) |
| `MapCanvas.tsx` | 전체 크기 지도 컨테이너. 훅을 소유하고 children을 컨텍스트로 감싼다 |
| `ClusterLayer.tsx` | 클러스터 다각형 + hover 라벨 + 순번 라벨 + 인포윈도우 |
| `MarkerLayer.tsx` | 1차 그룹 마커(멤버 수 배지·진입/이탈 색·순번 라벨) + 출발지 마커 |
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
| 8 | `MarkerLayer` 강조된 마커 | S6 표 행 hover |
| 9 | `MarkerLayer` 강조 라벨 | |
| 10 | `ClusterLayer` 방문 순번 숫자 | 무엇에도 가리지 않는다 |

순번 숫자가 마커보다 위인 이유: 1건짜리 클러스터는 중심이 그 그룹의 좌표와 같아 마커·클릭 타깃과
정확히 겹친다. 그 경우에는 숫자를 마커 위쪽으로 비켜 찍기까지 한다(`yAnchor`).

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
