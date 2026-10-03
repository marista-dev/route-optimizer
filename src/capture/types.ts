/**
 * 결과 화면(S6) 지도 캡처 기능의 공용 계약 타입.
 *
 * `geometry.ts` / `useTabCapture.ts` / `CaptureFrame` / `CaptureToolbar` /
 * `MarkerPositions` / `ResultScreen`이 이 파일에만 의존해 서로를 맞춘다.
 */

/** 사각형. 기준 좌표계는 쓰는 자리에서 밝힌다(지도 컨테이너 기준 CSS px 등). */
export interface Rect {
  x: number;
  y: number;
  width: number;
  height: number;
}

/** 너비·높이 한 쌍(px). */
export interface Size {
  width: number;
  height: number;
}

/** 1차 그룹 마커 하나의 화면 위치. 지도 컨테이너 왼쪽 위 기준 CSS px. */
export interface MarkerPoint {
  groupId: number;
  x: number;
  y: number;
}

/**
 * 영역 대비 마커 위치 판정.
 * - `inside`: 마커 원 전체가 영역 안
 * - `edge`: 영역 경계에 걸쳐 잘린다
 * - `outside`: 영역 밖
 */
export type PointPlacement = 'inside' | 'edge' | 'outside';

/** 탭 캡처 스트림 상태. */
export type TabCaptureStatus = 'idle' | 'starting' | 'live';

/** `useTabCapture` 반환값. */
export interface TabCapture {
  /** 이 브라우저가 탭 단위 캡처(`getDisplayMedia` + 탭 공유)를 쓸 수 있는지 */
  supported: boolean;
  status: TabCaptureStatus;
  /**
   * 스트림이 없으면 허락을 받아 시작하고, 있으면 그대로 쓴다.
   * `viewportRect`(window 기준 CSS px) 영역을 잘라 PNG Blob으로 돌려준다.
   * 사용자가 허락을 거절하면 `DOMException`(name `NotAllowedError`)을 던진다.
   * `options.attribution`을 주면 그 문구를 오른쪽 아래에 덧그린다(출처 표기).
   */
  grab: (viewportRect: Rect, options?: { attribution?: string }) => Promise<Blob>;
  /** 스트림을 끝낸다(트랙 stop). 여러 번 불러도 안전하다 */
  stop: () => void;
}

/** A4 프레임 방향. */
export type Orientation = 'portrait' | 'landscape';

/**
 * `planNextWindow` 결과.
 * `dx`/`dy`는 지도를 `map.panBy(dx, dy)`로 옮길 양(px)이다. 옮긴 뒤에는 고른 창이
 * 프레임과 정확히 겹친다. `groupIds`는 그 창에 온전히 들어오는 "남은" 그룹이다.
 */
export interface WindowPlan {
  dx: number;
  dy: number;
  groupIds: number[];
}

/** 자동 저장 진행 상황. */
export interface AutoProgress {
  done: number;
  total: number;
}
