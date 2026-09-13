/**
 * 지도 팔레트 — 다각형·마커·경로선이 공유하는 의미 있는 색.
 *
 * 전에는 `ClusterLayer`·`RouteLayer`·`OrderLinkLayer`가 각자 같은 값을 리터럴로
 * 들고 있었다. 이름(`NEXT_STROKE` 등)과 주석으로만 "S5 이탈선과 같은 빨강"이라고
 * 약속했을 뿐이라, 하나를 바꾸면 나머지는 옛 색 그대로 남았다 — 타입도 린트도
 * 잡지 못하는 어긋남이었다. 이 상수를 import하면 그 계약이 강제된다.
 *
 * `map.css`는 같은 값을 CSS 변수(`--primary`/`--success`/`--danger`,
 * `src/index.css`의 `:root`)로 쓴다.
 */
export const MAP_COLOR = {
  /** 기본(브랜드 파랑) — 다각형·순번 라벨·경로선 */
  base: '#1E4ED8',
  /** 확정된 클러스터 · 진입 지점 */
  done: '#16A34A',
  /** S5에서 다음에 갈 클러스터 · 이탈 지점 — "여기로 빠져나간다"를 빨강으로 못 박는다 */
  next: '#DC2626',
  /** S5에서 아직 손대지 않은(흐린) 클러스터의 외곽선 */
  idleStroke: '#9AA1AC',
  /** S5에서 아직 손대지 않은(흐린) 클러스터의 채움 */
  idleFill: '#5B6370',
} as const;
