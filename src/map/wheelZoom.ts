/**
 * 휠 확대·축소 안정화용 순수 함수. DOM·카카오 SDK를 건드리지 않아 단위 테스트한다
 * (`wheelZoom.test.ts`). 실제 연결은 `useKakaoMap`이 한다.
 *
 * 카카오맵 레벨은 정수(1~14)이고 **숫자가 클수록 축소**(넓게 보기)다. 휠을 아래로 굴리면
 * (`deltaY > 0`) 보통 지도에서 축소이므로, 여기서 돌려주는 단계 `+1`은 "레벨 +1 = 축소",
 * `-1`은 "레벨 −1 = 확대"다. 그대로 `map.getLevel() + step`에 더하면 된다.
 */

/** `WheelEvent.deltaMode` 값 */
const DOM_DELTA_LINE = 1;
const DOM_DELTA_PAGE = 2;

/** 라인 단위 휠(파이어폭스 등) 한 줄을 px로 칠 때의 값 */
export const WHEEL_LINE_PX = 16;

/**
 * 휠 이동량을 px 단위로 맞춘다.
 * - `deltaMode` 0(px): 그대로
 * - 1(라인): × {@link WHEEL_LINE_PX}
 * - 2(페이지): × `pageHeight`(보통 지도 컨테이너 높이)
 *
 * 유한한 수가 아니면 0이다.
 */
export function normalizeWheelDelta(
  e: { deltaY: number; deltaMode: number },
  pageHeight: number,
): number {
  const dy = Number.isFinite(e.deltaY) ? e.deltaY : 0;
  if (e.deltaMode === DOM_DELTA_LINE) return dy * WHEEL_LINE_PX;
  if (e.deltaMode === DOM_DELTA_PAGE) return dy * (Number.isFinite(pageHeight) ? pageHeight : 0);
  return dy;
}

/** 휠 누적기 설정 */
export interface WheelAccumulatorOptions {
  /** 이만큼(px) 쌓이면 한 단계. 마우스 휠 한 칸(크롬 100px, 파이어폭스 3줄=48px) 모두 한 칸에 한 단계가 되도록 48 */
  threshold?: number;
  /** 한 단계를 낸 뒤 이 시간(ms) 동안 들어온 입력은 버린다. 기본 250 */
  cooldownMs?: number;
  /**
   * 마지막 입력 뒤 이 시간(ms)이 지나면 쌓아 둔 값을 비운다(오래전 찌꺼기와 합쳐지지 않게).
   * 기본 400. 0 이하이면 끈다.
   */
  idleResetMs?: number;
}

/**
 * 휠 입력(px, {@link normalizeWheelDelta} 결과)을 모아 확대 단계로 바꾸는 판정기를 만든다.
 *
 * 돌려주는 함수 `(deltaPx, now) => -1 | 0 | 1`:
 * - 입력을 누적하다가 절댓값이 `threshold` 이상이면 그 방향의 한 단계(`+1` 축소 / `−1` 확대)를
 *   내고 누적값을 비운다. 한 번에 아무리 크게 와도 한 단계뿐이다.
 * - 방향이 바뀌면 누적값을 버리고 새 방향으로 다시 센다.
 * - 단계를 낸 뒤 `cooldownMs` 동안은 입력을 버린다. 그 사이 한 칸(`threshold`)보다 작은 입력이 계속
 *   들어오면 버리는 구간을 입력마다 `cooldownMs`씩 늘린다 — 트랙패드로 한 번 쓸면(관성 포함) 한
 *   단계뿐이다. 다음 단계는 손을 떼고 `cooldownMs` 이상 쉰 뒤에 난다.
 * - `now`는 ms 시각(`performance.now()` 등). 시간이 거꾸로 가면 쿨다운·유휴 판정을 하지 않는다.
 */
export function createWheelAccumulator({
  threshold = 48,
  cooldownMs = 250,
  idleResetMs = 400,
}: WheelAccumulatorOptions = {}): (deltaPx: number, now: number) => -1 | 0 | 1 {
  let acc = 0;
  let lastStep = -Infinity;
  /** 이 시각 전까지 입력을 버린다. 단계를 낸 순간 `lastStep + cooldownMs`이고, 작은 입력이 이어지면 늘어난다 */
  let blockUntil = -Infinity;
  let lastInput = -Infinity;
  const limit = Math.max(threshold, Number.EPSILON);

  return (deltaPx, now) => {
    const prevInput = lastInput;
    lastInput = now;
    if (!Number.isFinite(deltaPx) || deltaPx === 0) return 0;

    if (now >= lastStep && now < blockUntil) {
      acc = 0;
      // 한 칸보다 작은 입력이 끊이지 않고 이어지면(트랙패드 쓸기·관성 스크롤) 같은 동작의 꼬리로 보고
      // 막는 시간을 늘린다. 한 칸 이상인 입력(마우스 휠 노치)은 늘리지 않아, 휠을 빠르게 돌리면
      // 쿨다운마다 한 단계씩 나아간다.
      if (Math.abs(deltaPx) < limit) blockUntil = Math.max(blockUntil, now + cooldownMs);
      return 0;
    }

    const idle = now - prevInput;
    if (idleResetMs > 0 && idle >= idleResetMs) acc = 0;

    if (acc !== 0 && Math.sign(acc) !== Math.sign(deltaPx)) acc = 0;
    acc += deltaPx;

    if (Math.abs(acc) >= limit) {
      const step = acc > 0 ? 1 : -1;
      acc = 0;
      lastStep = now;
      blockUntil = now + cooldownMs;
      return step;
    }
    return 0;
  };
}

/** 레벨을 [min, max]로 자른다. 정수가 아니면 반올림한다. */
export function clampLevel(level: number, min: number, max: number): number {
  return Math.min(Math.max(Math.round(level), min), max);
}
