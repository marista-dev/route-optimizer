/**
 * 지도 캡처용 좌표 계산. DOM을 건드리지 않는 순수 함수만 둔다.
 *
 * 좌표계는 두 가지다.
 * - 화면 좌표: CSS px. 지도 컨테이너 기준인지 window 기준인지는 함수마다 밝힌다.
 * - 비디오 좌표: `getDisplayMedia` 프레임의 실제 픽셀. 기기 배율(DPR)과
 *   크롬의 "공유 중" 바 때문에 화면 좌표와 비율이 다르다.
 */
import type { MarkerPoint, Orientation, PointPlacement, Rect, Size, WindowPlan } from './types';

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

/** A4 용지의 긴 변 : 짧은 변 비율(1:√2). */
export const A4_RATIO = Math.SQRT2;

/** 유한한 수가 아니면 0으로 본다(NaN·Infinity 방어). */
function finite(v: number): number {
  return Number.isFinite(v) ? v : 0;
}

/**
 * 지도 컨테이너 안에 들어가는 가장 큰 A4 프레임(지도 컨테이너 기준 CSS px).
 *
 * 쓸 수 있는 영역은 컨테이너에서 사방 `margin`을 빼고, 아래쪽은 `reserveBottom`
 * (하단 독 자리)을 더 뺀 범위다. 그 안에서 가장 큰 A4 사각형을 구해 가로·세로 모두
 * 가운데에 놓는다.
 * - 세로(`portrait`): 높이 = 너비 × √2
 * - 가로(`landscape`): 너비 = 높이 × √2
 *
 * 결과는 정수다. 크기는 내림해서 영역을 넘지 않게 한다(그래서 비율은 1px 안쪽으로
 * 어긋날 수 있다). 컨테이너가 0 이하이거나 너무 작으면 너비·높이를 1px로 둔다 — NaN은
 * 나오지 않는다.
 */
export function fitA4Frame(
  container: Size,
  orientation: Orientation,
  reserveBottom: number,
  margin: number,
): Rect {
  const m = Math.max(finite(margin), 0);
  const availW = finite(container.width) - 2 * m;
  const availH = finite(container.height) - 2 * m - Math.max(finite(reserveBottom), 0);
  const w0 = Math.max(availW, 0);
  const h0 = Math.max(availH, 0);

  let width: number;
  let height: number;
  if (orientation === 'portrait') {
    width = Math.min(w0, h0 / A4_RATIO);
    height = width * A4_RATIO;
  } else {
    height = Math.min(h0, w0 / A4_RATIO);
    width = height * A4_RATIO;
  }
  width = Math.max(Math.floor(width + 1e-9), 1);
  height = Math.max(Math.floor(height + 1e-9), 1);

  return {
    x: Math.round(m + (availW - width) / 2),
    y: Math.round(m + (availH - height) / 2),
    width,
    height,
  };
}

/** 캡처 프레임 크기 배율 하한. 최대 프레임({@link fitA4Frame}) 대비 비율이다. */
export const FRAME_SCALE_MIN = 0.4;
/** 캡처 프레임 크기 배율 상한(최대 프레임 그대로). */
export const FRAME_SCALE_MAX = 1;

/**
 * 배율을 허용 범위로 자른다. 범위는 [{@link FRAME_SCALE_MIN}, {@link FRAME_SCALE_MAX}]이고,
 * 짧은 변이 `minShort`보다 작아지지 않게 하한을 더 올린다. 최대 프레임 자체가 `minShort`보다
 * 작으면 줄일 수 없으므로 늘 1이다. 유한한 수가 아니면 1로 본다.
 */
export function clampFrameScale(maxRect: Rect, scale: number, minShort = 160): number {
  const short = Math.min(maxRect.width, maxRect.height);
  if (!(short > 0) || short <= minShort) return FRAME_SCALE_MAX;
  const lo = Math.min(Math.max(FRAME_SCALE_MIN, minShort / short), FRAME_SCALE_MAX);
  const s = Number.isFinite(scale) ? scale : FRAME_SCALE_MAX;
  return clamp(s, lo, FRAME_SCALE_MAX);
}

/**
 * 최대 프레임 `maxRect`를 **중심을 고정한 채** `scale`배로 줄인 프레임(같은 좌표계, 정수 px).
 *
 * - 비율은 `maxRect`와 같다(A4). 크기는 내림하므로 1px 안쪽으로 어긋날 수 있다.
 * - `scale`은 [{@link FRAME_SCALE_MIN}, 1]로 자르고, 짧은 변이 `minShort`(기본 160px)
 *   아래로 내려가지 않게 더 자른다.
 * - `maxRect`의 짧은 변이 이미 `minShort` 이하이면 `maxRect`를 그대로 돌려준다.
 * - 원점은 반올림하고, 결과가 늘 `maxRect` 안에 들어가게 한다.
 */
export function scaleFrame(maxRect: Rect, scale: number, minShort = 160): Rect {
  const s = clampFrameScale(maxRect, scale, minShort);
  if (s >= FRAME_SCALE_MAX) return { ...maxRect };
  const width = Math.max(Math.floor(maxRect.width * s + 1e-9), 1);
  const height = Math.max(Math.floor(maxRect.height * s + 1e-9), 1);
  const x = clamp(
    Math.round(maxRect.x + (maxRect.width - width) / 2),
    maxRect.x,
    maxRect.x + maxRect.width - width,
  );
  const y = clamp(
    Math.round(maxRect.y + (maxRect.height - height) / 2),
    maxRect.y,
    maxRect.y + maxRect.height - height,
  );
  return { x, y, width, height };
}

/**
 * 프레임 모서리 핸들을 `pointer`까지 끌었을 때의 배율(중심 고정, 비율 유지).
 *
 * `pointer`는 `maxRect`와 같은 좌표계다. 중심에서 포인터까지 가로·세로 거리를 각각 반 너비·
 * 반 높이로 나눈 값 중 큰 쪽을 배율로 삼는다 — 끈 모서리가 포인터에 닿고, 프레임이 포인터를
 * 넘지 않는 가장 작은 크기다. 결과는 {@link scaleFrame}과 같은 규칙으로 잘라 돌려준다.
 */
export function scaleFromCornerDrag(
  maxRect: Rect,
  pointer: { x: number; y: number },
  minShort = 160,
): number {
  const cx = maxRect.x + maxRect.width / 2;
  const cy = maxRect.y + maxRect.height / 2;
  const hw = maxRect.width / 2;
  const hh = maxRect.height / 2;
  const sx = hw > 0 ? Math.abs(pointer.x - cx) / hw : 0;
  const sy = hh > 0 ? Math.abs(pointer.y - cy) / hh : 0;
  return clampFrameScale(maxRect, Math.max(sx, sy), minShort);
}

/** 부동소수 비교 여유(px). `planPages`가 좌표를 거듭 옮기며 생기는 오차를 흡수한다. */
const EPS = 1e-6;

/** 오름차순 배열에서 `v` 이하인 원소 개수. */
function countAtMost(sorted: readonly number[], v: number): number {
  let lo = 0;
  let hi = sorted.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (sorted[mid] <= v) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

/** 오름차순 배열에서 `v` 미만인 원소 개수. */
function countLess(sorted: readonly number[], v: number): number {
  let lo = 0;
  let hi = sorted.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (sorted[mid] < v) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

/** 닫힌 구간 하나. */
interface Span {
  lo: number;
  hi: number;
}

/**
 * 한 축에서 창 시작 위치 후보를 만든다. `range`(anchor가 들어가는 범위) 안의
 * 각 구간 끝점, 범위 양 끝, 그리고 현재 위치(`current`, 범위로 잘라 냄)다.
 *
 * "어떤 마커 집합이 모두 들어가는 위치"는 구간들의 교집합이고, 그 안에서 현재 위치에
 * 가장 가까운 점은 교집합 끝점이거나 현재 위치 자체다. 그래서 이 후보만 보면
 * (개수 최대 → 이동 최소) 기준의 최적해를 놓치지 않는다.
 */
function axisCandidates(spans: readonly Span[], range: Span, current: number): number[] {
  const out = [range.lo, range.hi, clamp(current, range.lo, range.hi)];
  for (const s of spans) {
    if (s.lo >= range.lo && s.lo <= range.hi) out.push(s.lo);
    if (s.hi >= range.lo && s.hi <= range.hi) out.push(s.hi);
  }
  out.sort((a, b) => a - b);
  // 중복 제거
  const uniq: number[] = [];
  for (const v of out) if (uniq.length === 0 || v - uniq[uniq.length - 1] > EPS) uniq.push(v);
  return uniq;
}

/**
 * 다음에 찍을 창 위치를 고른다.
 *
 * `points`는 지도 컨테이너 기준 마커 중심 좌표(px)다. 화면 밖(음수 포함)이어도 된다.
 * `frame`과 같은 크기의 창을 움직여 보며,
 * 1. anchor 마커 원이 창 안에 온전히 들어가야 하고(필수),
 * 2. `remaining` 마커가 온전히(원 전체) 들어가는 개수가 가장 많고,
 * 3. 같으면 현재 프레임 위치에서 이동량 |dx|+|dy|가 가장 작은 위치를 고른다.
 *
 * 마커 원이 창 안에 있다는 것은 창 왼쪽 L이 [p.x + r − w, p.x − r] 안에 있다는 뜻이다
 * (세로도 같다). 그래서 문제는 "가로 구간·세로 구간을 동시에 가장 많이 덮는 점 찾기"가 된다.
 *
 * 복잡도: 가로 후보 O(n)마다, 그 L을 덮는 마커만 골라 세로 후보 O(n)를 이분 탐색으로
 * 세므로 O(n² log n)이다. 마커 500개에서 수십 ms 수준이다.
 *
 * 결과 `dx`/`dy`는 `chosenLeft − frame.x`, `chosenTop − frame.y`다. 카카오
 * `map.panBy(dx, dy)`는 지도 중심을 +dx, +dy px 옮기므로 내용은 −dx, −dy 만큼 움직여
 * 고른 창이 프레임 자리에 온다. 값은 반올림하지 않는다(정확한 포함 판정을 지키려고).
 * `groupIds`는 고른 창에 온전히 들어가는 남은 그룹이며 anchor를 늘 포함한다.
 *
 * anchor가 `points`에 없으면 `null`. 마커 지름이 프레임보다 커서 anchor가 온전히
 * 들어갈 수 없으면 anchor를 창 가운데에 두고, `groupIds`는 anchor 하나만 담는다
 * (반복 호출이 끝나도록).
 */
export function planNextWindow(
  points: readonly MarkerPoint[],
  remaining: ReadonlySet<number>,
  anchorId: number,
  frame: Rect,
  radius: number,
): WindowPlan | null {
  const anchor = points.find((p) => p.groupId === anchorId);
  if (!anchor) return null;

  const w = frame.width;
  const h = frame.height;
  const r = Math.max(radius, 0);

  if (2 * r > w || 2 * r > h) {
    return {
      dx: anchor.x - w / 2 - frame.x,
      dy: anchor.y - h / 2 - frame.y,
      groupIds: [anchorId],
    };
  }

  const xRange: Span = { lo: anchor.x + r - w, hi: anchor.x - r };
  const yRange: Span = { lo: anchor.y + r - h, hi: anchor.y - r };

  // anchor 범위와 겹칠 수 있는 남은 마커만 남긴다(anchor 자신은 따로 센다).
  const seen = new Set<number>([anchorId]);
  const cands: { id: number; x: Span; y: Span }[] = [];
  for (const p of points) {
    if (seen.has(p.groupId) || !remaining.has(p.groupId)) continue;
    seen.add(p.groupId);
    const x: Span = { lo: p.x + r - w, hi: p.x - r };
    const y: Span = { lo: p.y + r - h, hi: p.y - r };
    if (x.hi < xRange.lo - EPS || x.lo > xRange.hi + EPS) continue;
    if (y.hi < yRange.lo - EPS || y.lo > yRange.hi + EPS) continue;
    cands.push({ id: p.groupId, x, y });
  }

  const lefts = axisCandidates(
    cands.map((c) => c.x),
    xRange,
    frame.x,
  );

  let bestCount = -1;
  let bestCost = Infinity;
  let bestL = xRange.lo;
  let bestT = yRange.lo;

  for (const L of lefts) {
    const active = cands.filter((c) => c.x.lo <= L + EPS && L <= c.x.hi + EPS);
    // 이 L에서 얻을 수 있는 최대치도 지금 최선보다 작으면 건너뛴다.
    if (active.length < bestCount) continue;
    const costX = Math.abs(L - frame.x);
    if (active.length === bestCount && costX >= bestCost) continue;

    const ys = active.map((c) => c.y);
    const los = ys.map((s) => s.lo).sort((a, b) => a - b);
    const his = ys.map((s) => s.hi).sort((a, b) => a - b);
    for (const T of axisCandidates(ys, yRange, frame.y)) {
      const count = countAtMost(los, T + EPS) - countLess(his, T - EPS);
      const cost = costX + Math.abs(T - frame.y);
      if (count > bestCount || (count === bestCount && cost < bestCost - EPS)) {
        bestCount = count;
        bestCost = cost;
        bestL = L;
        bestT = T;
      }
    }
  }

  const groupIds = [anchorId];
  for (const c of cands) {
    if (
      c.x.lo <= bestL + EPS &&
      bestL <= c.x.hi + EPS &&
      c.y.lo <= bestT + EPS &&
      bestT <= c.y.hi + EPS
    ) {
      groupIds.push(c.id);
    }
  }

  return { dx: bestL - frame.x, dy: bestT - frame.y, groupIds };
}

/**
 * 남은 그룹 중 순번이 가장 빠른 것. 순번을 모르는(`undefined`) 그룹은 맨 뒤로 보내고,
 * 순번이 같으면 groupId가 작은 쪽을 고른다. 비어 있으면 `undefined`.
 */
export function nextAnchor(
  remaining: ReadonlySet<number>,
  orderOf: (groupId: number) => number | undefined,
): number | undefined {
  let best: number | undefined;
  let bestOrder = Infinity;
  for (const id of remaining) {
    const order = orderOf(id) ?? Infinity;
    if (best === undefined || order < bestOrder || (order === bestOrder && id < best)) {
      best = id;
      bestOrder = order;
    }
  }
  return best;
}

/**
 * 남은 그룹이 모두 찍힐 때까지 {@link planNextWindow}를 반복한 계획(확대 수준 고정).
 *
 * 매번 anchor는 {@link nextAnchor}로 고른다. 한 장을 계획하면 그 장의 `groupIds`를
 * 남은 목록에서 빼고, 모든 마커 좌표를 (−dx, −dy)만큼 옮겨 `panBy` 이후의 화면을
 * 흉내 낸다. 그래서 **각 장의 `dx`/`dy`는 바로 앞 장 위치 기준**이다 — 결과를 순서대로
 * `map.panBy(dx, dy)` 하면 된다. 첫 장은 현재 화면(`points`) 기준이다.
 *
 * `points`에 좌표가 없는 그룹은 건너뛴다. `maxPages`장에서 멈춘다(무한 반복 방지).
 */
export function planPages(
  points: readonly MarkerPoint[],
  remaining: ReadonlySet<number>,
  orderOf: (groupId: number) => number | undefined,
  frame: Rect,
  radius: number,
  maxPages = 200,
): WindowPlan[] {
  const left = new Set(remaining);
  let current: MarkerPoint[] = points.map((p) => ({ ...p }));
  const pages: WindowPlan[] = [];

  while (left.size > 0 && pages.length < maxPages) {
    const anchorId = nextAnchor(left, orderOf);
    if (anchorId === undefined) break;
    const plan = planNextWindow(current, left, anchorId, frame, radius);
    if (!plan) {
      left.delete(anchorId);
      continue;
    }
    pages.push(plan);
    for (const id of plan.groupIds) left.delete(id);
    current = current.map((p) => ({ groupId: p.groupId, x: p.x - plan.dx, y: p.y - plan.dy }));
  }
  return pages;
}

/**
 * {@link planNextWindow} 결과의 `dx`/`dy`를 `map.panBy`에 넘길 정수로 바꾼다.
 *
 * 고른 창은 대개 어떤 마커가 프레임 경계에 딱 붙는 자리라, 그냥 반올림하면 0.5px 차이로
 * 그 마커가 "걸침"이 될 수 있다. 그래서 가로·세로 각각 내림·올림 네 가지를 모두 해 보고,
 * 옮긴 뒤 `plan.groupIds`가 프레임에 온전히 가장 많이 들어가는 쪽을 고른다
 * (같으면 원래 값에 가까운 쪽). `points`는 `planNextWindow`에 넘긴 것과 같은 좌표다.
 */
export function roundPan(
  points: readonly MarkerPoint[],
  plan: WindowPlan,
  frame: Rect,
  radius: number,
): { dx: number; dy: number } {
  const wanted = new Set(plan.groupIds);
  const targets = points.filter((p) => wanted.has(p.groupId));
  const xs = [...new Set([Math.floor(plan.dx), Math.ceil(plan.dx)])];
  const ys = [...new Set([Math.floor(plan.dy), Math.ceil(plan.dy)])];

  let best = { dx: Math.round(plan.dx), dy: Math.round(plan.dy) };
  let bestCount = -1;
  let bestCost = Infinity;
  for (const dx of xs) {
    for (const dy of ys) {
      let count = 0;
      for (const p of targets) {
        if (classifyPoint({ x: p.x - dx, y: p.y - dy }, frame, radius) === 'inside') count++;
      }
      const cost = Math.abs(dx - plan.dx) + Math.abs(dy - plan.dy);
      if (count > bestCount || (count === bestCount && cost < bestCost)) {
        best = { dx, dy };
        bestCount = count;
        bestCost = cost;
      }
    }
  }
  return best;
}
