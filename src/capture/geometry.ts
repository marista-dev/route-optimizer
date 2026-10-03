/**
 * 지도 캡처용 좌표 계산. DOM을 건드리지 않는 순수 함수만 둔다.
 *
 * 좌표계는 두 가지다.
 * - 화면 좌표: CSS px. 지도 컨테이너 기준인지 window 기준인지는 함수마다 밝힌다.
 * - 비디오 좌표: `getDisplayMedia` 프레임의 실제 픽셀. 기기 배율(DPR)과
 *   크롬의 "공유 중" 바 때문에 화면 좌표와 비율이 다르다.
 */
import type { MarkerPoint, PointPlacement, Rect, Size } from './types';

/**
 * 순번 마커 반지름(px). 판정 여유로 쓴다.
 *
 * `map.css`의 `.ro-marker.is-order-3`(세 자리 순번)이 지름 36px로 가장 크다.
 * 전역 `box-sizing: border-box`라 2.5px 흰 테두리는 이 36px 안에 들어 있다.
 * 반지름 18px에, 테두리 바깥 1px 초록 링(`box-shadow: 0 0 0 1px`)을 더해 19px로 잡는다.
 * 흐린 그림자와 hover 확대(`is-highlight`)는 캡처 모드에서 신경 쓰지 않는다 —
 * 그림자는 잘려도 순번을 읽는 데 지장이 없고, 확대는 캡처 모드에서 꺼진다.
 * 한 자리 순번(26px)에도 같은 값을 쓴다. 조금 넉넉하게 "걸침"으로 보는 편이 안전하다.
 */
export const MARKER_RADIUS_PX = 19;

/**
 * 카카오 로고·저작권 표기가 차지하는 영역(px).
 * 지도 컨테이너 왼쪽 아래 모서리에서 잰 너비·높이다.
 */
export const KAKAO_LOGO_ZONE: Size = { width: 80, height: 30 };

/** 두 점을 대각선 끝으로 하는 사각형. 어느 방향으로 끌어도 너비·높이는 양수다. */
export function normalizeRect(a: { x: number; y: number }, b: { x: number; y: number }): Rect {
  return {
    x: Math.min(a.x, b.x),
    y: Math.min(a.y, b.y),
    width: Math.abs(a.x - b.x),
    height: Math.abs(a.y - b.y),
  };
}

/**
 * 사각형을 `bounds`(0,0 기준) 안으로 밀어 넣고 최소 크기를 지킨다.
 *
 * 크기가 bounds보다 크면 bounds에 맞춰 줄이고, 최소 크기보다 작으면 늘린다
 * (단, bounds보다 커지지는 않는다). 그다음 위치를 안쪽으로 민다 —
 * 끌다가 가장자리에 닿으면 크기는 그대로 두고 멈추게 하려는 것이다.
 */
export function clampRect(rect: Rect, bounds: Size, minSize: number): Rect {
  const width = Math.min(Math.max(rect.width, minSize), bounds.width);
  const height = Math.min(Math.max(rect.height, minSize), bounds.height);
  const x = Math.min(Math.max(rect.x, 0), Math.max(bounds.width - width, 0));
  const y = Math.min(Math.max(rect.y, 0), Math.max(bounds.height - height, 0));
  return { x, y, width, height };
}

/**
 * 화면(window 기준 CSS px) 사각형을 비디오 프레임 픽셀 좌표로 바꾼다.
 *
 * 가로·세로 비율을 따로 쓴다. 공유 중 바가 뜨면 탭 높이만 줄어 비율이 어긋날 수 있다.
 * 결과는 정수로 반올림하고 프레임 안으로 잘라 내며, 너비·높이는 최소 1px이다
 * (`drawImage`·`canvas`가 0 크기를 받지 않는다).
 */
export function cropToVideo(viewportRect: Rect, viewport: Size, video: Size): Rect {
  const sx = viewport.width > 0 ? video.width / viewport.width : 1;
  const sy = viewport.height > 0 ? video.height / viewport.height : 1;

  const left = clamp(Math.round(viewportRect.x * sx), 0, Math.max(video.width - 1, 0));
  const top = clamp(Math.round(viewportRect.y * sy), 0, Math.max(video.height - 1, 0));
  const right = clamp(Math.round((viewportRect.x + viewportRect.width) * sx), 0, video.width);
  const bottom = clamp(Math.round((viewportRect.y + viewportRect.height) * sy), 0, video.height);

  return {
    x: left,
    y: top,
    width: Math.max(right - left, 1),
    height: Math.max(bottom - top, 1),
  };
}

function clamp(v: number, min: number, max: number): number {
  return Math.min(Math.max(v, min), max);
}

/**
 * 반지름 `radius`인 원이 사각형 안에 있는지 판정한다.
 * 원 전체가 안이면 `inside`, 전부 밖이면 `outside`, 경계에 걸치면 `edge`.
 * 점과 사각형은 같은 좌표계(보통 지도 컨테이너 기준)여야 한다.
 */
export function classifyPoint(
  p: { x: number; y: number },
  rect: Rect,
  radius: number,
): PointPlacement {
  const right = rect.x + rect.width;
  const bottom = rect.y + rect.height;

  if (
    p.x - radius >= rect.x &&
    p.x + radius <= right &&
    p.y - radius >= rect.y &&
    p.y + radius <= bottom
  ) {
    return 'inside';
  }

  // 사각형에서 원 중심까지 가장 가까운 점과의 거리로 겹침을 본다(모서리 근처도 정확하다).
  const dx = p.x - clamp(p.x, rect.x, right);
  const dy = p.y - clamp(p.y, rect.y, bottom);
  return dx * dx + dy * dy >= radius * radius ? 'outside' : 'edge';
}

/** 마커 여러 개를 한 번에 판정한다. 키는 `groupId`. */
export function classifyPoints(
  points: readonly MarkerPoint[],
  rect: Rect,
  radius: number,
): Map<number, PointPlacement> {
  const map = new Map<number, PointPlacement>();
  for (const p of points) map.set(p.groupId, classifyPoint(p, rect, radius));
  return map;
}

/**
 * 순번 목록을 연속 구간으로 묶어 보여 준다.
 * `[12, 13, 14, 20]` → `"12~14, 20"`. 정렬·중복 제거는 여기서 한다. 빈 목록은 `""`.
 */
export function formatOrderRanges(nums: readonly number[]): string {
  const sorted = [...new Set(nums)].sort((a, b) => a - b);
  const parts: string[] = [];
  let i = 0;
  while (i < sorted.length) {
    const start = sorted[i];
    let end = start;
    while (i + 1 < sorted.length && sorted[i + 1] === end + 1) end = sorted[++i];
    parts.push(start === end ? String(start) : `${start}~${end}`);
    i++;
  }
  return parts.join(', ');
}

/**
 * 잘라 낼 영역이 카카오 로고 자리를 온전히 덮는지.
 *
 * `rect`와 `container`는 지도 컨테이너 기준 CSS px다. 로고는 컨테이너 왼쪽 아래
 * {@link KAKAO_LOGO_ZONE} 크기 안에 있다. 다 덮지 못하면 이미지에 로고가 없거나
 * 잘린 것이므로, 화면 쪽에서 출처 문구를 따로 그려 넣어야 한다.
 */
export function includesKakaoLogo(rect: Rect, container: Size): boolean {
  const zoneTop = container.height - KAKAO_LOGO_ZONE.height;
  return (
    rect.x <= 0 &&
    rect.x + rect.width >= KAKAO_LOGO_ZONE.width &&
    rect.y <= zoneTop &&
    rect.y + rect.height >= container.height
  );
}
