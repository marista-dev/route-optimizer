import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import {
  MARKER_RADIUS_PX,
  classifyPoints,
  fitA4Frame,
  formatOrderRanges,
  includesKakaoLogo,
  nextAnchor,
  planNextWindow,
  planPages,
  roundPan,
} from '../capture/geometry';
import type { AutoProgress, MarkerPoint, Orientation, Rect, Size } from '../capture/types';
import { WRONG_SURFACE_ERROR, useTabCapture } from '../capture/useTabCapture';
import type {
  CaptureFrameProps,
  CapturePreviewModalProps,
  CaptureToolbarProps,
} from '../components';
import { downloadBlob, mapImageFileName } from '../io';
import type { MapContextValue, MapInteractionProps } from '../map';
import { showInfo } from '../store/toast';
import { showApiError } from './helpers';

/** 잘라 낸 영역에 카카오 로고가 없을 때 이미지에 덧그리는 출처 문구 */
const ATTRIBUTION = '지도 © Kakao';
/** 프레임 사방 여백(px). 위쪽 여백은 프레임 라벨 칩(약 22px)이 들어갈 자리이기도 하다 */
const FRAME_MARGIN_PX = 24;
/** 독 높이·간격을 재기 전 기본값. `index.css`의 `--capture-dock-h`·`--capture-dock-gap`과 같다 */
const DEFAULT_DOCK_H = 56;
const DEFAULT_DOCK_GAP = 12;
/** 자동 저장 예상 장수 계산 상한 */
const MAX_PLAN_PAGES = 200;
/** 자동 저장에서 지도가 멈추기를 기다리는 최대 시간(ms) */
const IDLE_TIMEOUT_MS = 3000;
/** 옮긴 뒤 마커 좌표가 새 위치로 갱신되기를 기다리는 최대 시간(ms) */
const SETTLE_TIMEOUT_MS = 1500;
/** 예상 장수보다 이만큼 더 돌면 멈춘다(무한 반복 방지) */
const EXTRA_AUTO_STEPS = 5;

const EMPTY_SET: ReadonlySet<number> = new Set();

export interface UseMapCaptureInput {
  /** 그룹 id → 배송 순번. 순번이 있는 그룹만 "찍을 대상"으로 센다 */
  labelByGroup: ReadonlyMap<number, number>;
  /** 스토어의 최종 순서. 바뀌면(정체성 기준) 찍은 순번 기록을 비운다 */
  finalOrder: readonly number[];
  /** 업로드 원본 파일 이름. 이미지 파일 이름의 바탕이 된다 */
  fileName: string;
}

export interface MapCapture {
  /** 캡처 모드인지 */
  active: boolean;
  /** 캡처 모드로 들어간다 */
  enter: () => void;
  /** 캡처 모드를 끝낸다(자동 저장 중지·스트림 종료) */
  exit: () => void;
  /** `.ro-mapscreen__canvas`에 붙이는 ref. 크기를 재고 캡처 좌표의 기준으로 쓴다 */
  attachCanvas: (el: HTMLDivElement | null) => void;
  /** `<MarkerPositions onChange>`에 연결 */
  onPoints: (points: MarkerPoint[]) => void;
  /** `<MapBridge onChange>`에 연결. 지도 명령(`panBy`·`whenIdle`·`relayout`)을 받아 둔다 */
  onMapContext: (ctx: MapContextValue | null) => void;
  /** `<MapInteraction>` props. 캡처 모드에서만 마운트한다 */
  interaction: MapInteractionProps;
  /** 캡처 모드일 때만 값이 있다 */
  frame: CaptureFrameProps | null;
  /** 캡처 모드 하단 독 props */
  toolbar: CaptureToolbarProps;
  /** 미리보기를 띄울 때만 값이 있다 */
  preview: CapturePreviewModalProps | null;
}

/** 미리보기 대기 중인 한 장. 찍힌 그룹은 저장 시점이 아니라 grab 시점 기준이다 */
interface PendingShot {
  blob: Blob;
  fileName: string;
  /** grab 시점에 프레임 안에 온전히 들어 있던 그룹 */
  groups: number[];
  /** grab 시점의 최종 순서. 저장 전에 순서가 바뀌었으면 기록에 넣지 않는다 */
  order: readonly number[];
}

/** 화면 배치를 비교하기 위한 값. 공유 중 바가 뜨면 바뀐다 */
interface LayoutSnapshot {
  left: number;
  top: number;
  width: number;
  height: number;
  innerWidth: number;
  innerHeight: number;
}

/** 진행 중인 자동 저장 한 번. `stop`은 여러 번 불러도 안전하다 */
interface AutoRun {
  stop: () => void;
}

/** `n`번의 애니메이션 프레임을 기다린다. 숨김 처리가 실제로 그려진 뒤 찍기 위해 쓴다 */
function nextFrames(n: number): Promise<void> {
  return new Promise((resolve) => {
    const step = (left: number) => {
      if (left <= 0) resolve();
      else requestAnimationFrame(() => step(left - 1));
    };
    step(n);
  });
}

function readLayout(el: HTMLElement): LayoutSnapshot {
  const box = el.getBoundingClientRect();
  return {
    left: box.left,
    top: box.top,
    width: box.width,
    height: box.height,
    innerWidth: window.innerWidth,
    innerHeight: window.innerHeight,
  };
}

function sameLayout(a: LayoutSnapshot, b: LayoutSnapshot): boolean {
  return (
    a.left === b.left &&
    a.top === b.top &&
    a.width === b.width &&
    a.height === b.height &&
    a.innerWidth === b.innerWidth &&
    a.innerHeight === b.innerHeight
  );
}

function isTypingTarget(target: EventTarget | null): boolean {
  return (
    target instanceof HTMLElement &&
    target.closest('input, textarea, select, [contenteditable=""], [contenteditable="true"]') !==
      null
  );
}

/** 순번이 있고 아직 찍지 않았으며 화면 좌표가 있는 그룹 */
function remainingWithPoints(
  points: readonly MarkerPoint[],
  labels: ReadonlyMap<number, number>,
  captured: ReadonlySet<number>,
): Set<number> {
  const out = new Set<number>();
  for (const p of points) {
    if (labels.has(p.groupId) && !captured.has(p.groupId)) out.add(p.groupId);
  }
  return out;
}

/** 순번이 있는 그룹 중 `rect` 안에 온전히 들어간 것 */
function insideLabeled(
  points: readonly MarkerPoint[],
  labels: ReadonlyMap<number, number>,
  rect: Rect,
): number[] {
  const labeled = points.filter((p) => labels.has(p.groupId));
  const placement = classifyPoints(labeled, rect, MARKER_RADIUS_PX);
  return labeled.filter((p) => placement.get(p.groupId) === 'inside').map((p) => p.groupId);
}

/** CSS 변수(px)를 숫자로 읽는다. 없거나 숫자가 아니면 `fallback` */
function readCssPx(name: string, fallback: number): number {
  const v = parseFloat(getComputedStyle(document.documentElement).getPropertyValue(name));
  return Number.isFinite(v) ? v : fallback;
}

/** grab이 던진 오류를 사용자 안내로 바꾼다. 사용자가 취소한 경우(AbortError)는 조용히 넘긴다 */
function reportGrabError(err: unknown): void {
  const errName = err instanceof DOMException ? err.name : '';
  if (errName === 'NotAllowedError') {
    showInfo('화면 공유를 허용해야 이미지로 저장할 수 있습니다.');
  } else if (errName === WRONG_SURFACE_ERROR) {
    showInfo('공유 창에서 이 탭을 골라야 지도를 저장할 수 있습니다.');
  } else if (errName !== 'AbortError') {
    // 브라우저가 던진 DOMException 메시지는 영어라 한국어 문구로 바꿔 보여 준다.
    showApiError(err instanceof DOMException ? null : err, '지도 이미지를 만들지 못했습니다.');
  }
}

/**
 * S6 지도 캡처 모드의 상태와 동작을 한데 묶는다.
 *
 * 화면(`ResultScreen`)은 돌려받은 props를 `CaptureFrame`·`CaptureToolbar`·
 * `CapturePreviewModal`·`MarkerPositions`·`MapInteraction`·`MapBridge`에 꽂기만 한다.
 *
 * - 프레임: 지도 컨테이너 안에서 하단 독 자리를 뺀 가장 큰 A4 사각형(`fitA4Frame`). 고정이며
 *   사용자는 아래의 지도를 끌어 맞춘다.
 * - `다음 구역`: 남은 순번 중 가장 빠른 것을 담으면서 남은 마커가 가장 많이 들어가는 자리로
 *   `panBy`한다(`planNextWindow`).
 * - `남은 구역 모두 저장`: 같은 계산을 한 장마다 다시 하며 옮기고 → 기다리고 → 찍고 → 바로
 *   내려받는다. 중지·Esc·지도 조작·캡처 모드 종료·언마운트에 멈춘다.
 * - 찍힌 순번: 단일 저장은 미리보기에서 `저장`을 누른 장만, 자동 저장은 내려받은 장을 센다.
 *   어느 그룹이 찍혔는지는 grab 시점의 판정을 그대로 쓴다.
 * - 최종 순서가 바뀌면 찍은 순번 기록은 의미가 없으므로 비운다. 캡처 모드를 닫았다
 *   다시 열어도 같은 순서라면 기록은 남는다.
 */
export function useMapCapture({
  labelByGroup,
  finalOrder,
  fileName,
}: UseMapCaptureInput): MapCapture {
  const capture = useTabCapture();

  const [active, setActive] = useState(false);
  const [orientation, setOrientation] = useState<Orientation>('portrait');
  /** 캡처 직전 장식 숨김 */
  const [hidden, setHidden] = useState(false);
  /** 사용자가 지도를 끌거나 확대하는 중 */
  const [faded, setFaded] = useState(false);
  const [saving, setSaving] = useState(false);
  const [autoProgress, setAutoProgress] = useState<AutoProgress | null>(null);
  const [points, setPoints] = useState<MarkerPoint[]>([]);
  const [pending, setPending] = useState<PendingShot | null>(null);
  /** 찍은 그룹. 어느 최종 순서 기준인지 함께 들고 있다가 순서가 바뀌면 빈 것으로 본다 */
  const [captured, setCaptured] = useState<{
    order: readonly number[];
    groups: ReadonlySet<number>;
  }>(() => ({ order: finalOrder, groups: EMPTY_SET }));
  const capturedGroups = captured.order === finalOrder ? captured.groups : EMPTY_SET;

  /** 최신 마커 좌표. 자동 저장이 렌더를 기다리지 않고 읽는다 */
  const pointsRef = useRef<MarkerPoint[]>([]);
  /** `MapBridge`가 건네준 지도 명령 */
  const mapRef = useRef<MapContextValue | null>(null);
  /** 지금까지 저장한 장 수. 파일 이름 번호로 쓰며, 이름이 겹치지 않도록 화면이 살아 있는 동안 계속 센다 */
  const shotCountRef = useRef(0);
  /** 캡처 모드 세대. 닫거나 언마운트되는 순간 올려, 진행 중이던 결과가 늦게 도착해도 버린다 */
  const sessionRef = useRef(0);
  /** 단일 저장·자동 저장이 도는 중인지(동시에 하나만) */
  const savingRef = useRef(false);
  const autoRef = useRef<AutoRun | null>(null);

  const onPoints = useCallback((next: MarkerPoint[]) => {
    pointsRef.current = next;
    setPoints(next);
  }, []);

  const onMapContext = useCallback((ctx: MapContextValue | null) => {
    mapRef.current = ctx;
  }, []);

  // ── 지도 컨테이너 · 독 크기 ───────────────────────────────────────────────
  const [canvasEl, setCanvasEl] = useState<HTMLDivElement | null>(null);
  const [bounds, setBounds] = useState<Size>({ width: 0, height: 0 });
  const [dock, setDock] = useState({ height: DEFAULT_DOCK_H, gap: DEFAULT_DOCK_GAP });

  useEffect(() => {
    if (!canvasEl) return;
    const measure = () => {
      const width = canvasEl.clientWidth;
      const height = canvasEl.clientHeight;
      setBounds((prev) =>
        prev.width === width && prev.height === height ? prev : { width, height },
      );
    };
    measure();
    if (typeof ResizeObserver === 'undefined') {
      window.addEventListener('resize', measure);
      return () => window.removeEventListener('resize', measure);
    }
    const observer = new ResizeObserver(measure);
    observer.observe(canvasEl);
    return () => observer.disconnect();
  }, [canvasEl]);

  // 독은 좁은 화면에서 두 줄로 내려갈 수 있어 실제 높이를 잰다. 독은 캡처 모드 동안 계속
  // 마운트돼 있고(숨김은 visibility) 이 effect보다 먼저 그려진다.
  useEffect(() => {
    if (!active || !canvasEl) return;
    const el = canvasEl.querySelector<HTMLElement>('.ro-capture-dock');
    const gap = readCssPx('--capture-dock-gap', DEFAULT_DOCK_GAP);
    const measure = () => {
      const height = el ? Math.ceil(el.offsetHeight) : readCssPx('--capture-dock-h', DEFAULT_DOCK_H);
      setDock((prev) => (prev.height === height && prev.gap === gap ? prev : { height, gap }));
    };
    measure();
    if (!el || typeof ResizeObserver === 'undefined') return;
    const observer = new ResizeObserver(measure);
    observer.observe(el);
    return () => observer.disconnect();
  }, [active, canvasEl]);

  // 캡처 모드에서는 앱 헤더를 접는다(CSS). 지도 컨테이너 크기가 바뀌므로 다음 프레임에 relayout.
  useEffect(() => {
    if (!active) return;
    const root = document.documentElement;
    root.classList.add('ro-capture-mode');
    const raf = requestAnimationFrame(() => mapRef.current?.relayout());
    return () => {
      cancelAnimationFrame(raf);
      root.classList.remove('ro-capture-mode');
      requestAnimationFrame(() => mapRef.current?.relayout());
    };
  }, [active]);

  // ── 프레임과 마커 판정 ────────────────────────────────────────────────────
  const frameRect = useMemo(
    () => fitA4Frame(bounds, orientation, dock.height + dock.gap, FRAME_MARGIN_PX),
    [bounds, orientation, dock],
  );

  /** 순번이 붙은 그룹의 마커만. 순번이 없으면 찍을 대상이 아니다 */
  const labeledPoints = useMemo(
    () => points.filter((p) => labelByGroup.has(p.groupId)),
    [points, labelByGroup],
  );
  const placement = useMemo(
    () => classifyPoints(labeledPoints, frameRect, MARKER_RADIUS_PX),
    [labeledPoints, frameRect],
  );
  const edgeMarkers = useMemo(
    () => labeledPoints.filter((p) => placement.get(p.groupId) === 'edge'),
    [labeledPoints, placement],
  );
  // 화면 안(조금 여유)에 있는 찍은 마커에만 ✓ 배지를 띄운다.
  const capturedMarkers = useMemo(
    () =>
      labeledPoints.filter(
        (p) =>
          capturedGroups.has(p.groupId) &&
          p.x >= -MARKER_RADIUS_PX &&
          p.y >= -MARKER_RADIUS_PX &&
          p.x <= bounds.width + MARKER_RADIUS_PX &&
          p.y <= bounds.height + MARKER_RADIUS_PX,
      ),
    [labeledPoints, capturedGroups, bounds],
  );

  const label = useMemo(() => {
    const paper = `A4 ${orientation === 'portrait' ? '세로' : '가로'}`;
    const inside = labeledPoints.filter((p) => placement.get(p.groupId) === 'inside');
    if (inside.length === 0) {
      return `${paper} · 이 장에 온전히 든 순번 없음`;
    }
    const ranges = formatOrderRanges(inside.flatMap((p) => labelByGroup.get(p.groupId) ?? []));
    return `${paper} · 이 장 ${ranges} (${inside.length}곳)`;
  }, [orientation, labeledPoints, placement, labelByGroup]);

  // ── 찍은 순번 / 남은 순번 ─────────────────────────────────────────────────
  const totalCount = labelByGroup.size;
  const remainingCount = useMemo(() => {
    let n = 0;
    for (const id of labelByGroup.keys()) if (!capturedGroups.has(id)) n++;
    return n;
  }, [labelByGroup, capturedGroups]);

  // 비동기 흐름 도중(허락 창·공유 중 바로 레이아웃이 바뀐 뒤)에도 최신 값을 읽도록 ref로 들고 있는다.
  const latestRef = useRef({
    frameRect,
    bounds,
    labelByGroup,
    capturedGroups,
    fileName,
    finalOrder,
    live: capture.status === 'live',
  });
  useEffect(() => {
    latestRef.current = {
      frameRect,
      bounds,
      labelByGroup,
      capturedGroups,
      fileName,
      finalOrder,
      live: capture.status === 'live',
    };
  });

  /** 찍은 그룹을 기록에 더한다. grab 시점의 순서가 지금과 다르면 넣지 않는다 */
  const addCaptured = useCallback((ids: readonly number[], order: readonly number[]) => {
    setCaptured((prev) => {
      const current = latestRef.current.finalOrder;
      if (order !== current) return prev;
      const base = prev.order === current ? prev.groups : EMPTY_SET;
      return { order: current, groups: new Set([...base, ...ids]) };
    });
  }, []);

  // ── 진입 · 종료 ───────────────────────────────────────────────────────────
  const enter = useCallback(() => setActive(true), []);

  const { stop } = capture;
  const exit = useCallback(() => {
    sessionRef.current++;
    autoRef.current?.stop();
    stop();
    setActive(false);
    setHidden(false);
    setFaded(false);
    setPending(null);
    setAutoProgress(null);
    pointsRef.current = [];
    setPoints([]);
  }, [stop]);

  // 화면을 떠나면 진행 중이던 캡처·자동 저장 결과를 버린다.
  useEffect(
    () => () => {
      sessionRef.current++;
      autoRef.current?.stop();
    },
    [],
  );

  // ── 한 장 찍기 ───────────────────────────────────────────────────────────
  const { grab } = capture;

  /**
   * 지금 프레임을 한 장 찍는다. 장식은 미리 숨겨 두고 부른다.
   * 좌표와 "안에 든 그룹"은 찍는 순간 다시 읽는다.
   */
  const shootFrame = useCallback(
    async (el: HTMLElement, session: number) => {
      const wasLive = latestRef.current.live;
      const shoot = async () => {
        const { frameRect: r, bounds: b, labelByGroup: labels } = latestRef.current;
        const inside = insideLabeled(pointsRef.current, labels, r);
        const layout = readLayout(el);
        const blob = await grab(
          { x: layout.left + r.x, y: layout.top + r.y, width: r.width, height: r.height },
          { attribution: includesKakaoLogo(r, b) ? undefined : ATTRIBUTION },
        );
        return { blob, layout, inside };
      };
      let shot = await shoot();
      // 첫 캡처는 허락 직후 크롬의 "공유 중" 바가 뜨며 화면 높이가 바뀔 수 있다.
      // 그러면 앞서 읽은 좌표가 어긋나므로, 배치가 자리 잡은 뒤 한 번 더 찍는다.
      if (!wasLive && session === sessionRef.current) {
        await nextFrames(3);
        if (!sameLayout(shot.layout, readLayout(el))) shot = await shoot();
      }
      return shot;
    },
    [grab],
  );

  // ── 단일 저장(미리보기) ───────────────────────────────────────────────────
  const runCapture = useCallback(async () => {
    const el = canvasEl;
    if (!el || savingRef.current || autoRef.current) return;
    const session = sessionRef.current;
    savingRef.current = true;
    setSaving(true);
    setHidden(true);

    try {
      // 독·어두운 처리를 숨긴 화면이 실제로 그려질 때까지 기다린다.
      await nextFrames(2);
      const shot = await shootFrame(el, session);
      if (session !== sessionRef.current) return;
      const { fileName: name, finalOrder: order } = latestRef.current;
      setPending({
        blob: shot.blob,
        fileName: mapImageFileName(name, shotCountRef.current + 1),
        groups: shot.inside,
        order,
      });
    } catch (err) {
      if (session === sessionRef.current) reportGrabError(err);
    } finally {
      savingRef.current = false;
      setSaving(false);
      setHidden(false);
    }
  }, [canvasEl, shootFrame]);

  const save = useCallback(() => {
    void runCapture();
  }, [runCapture]);

  const savePending = useCallback(() => {
    if (!pending) return;
    shotCountRef.current += 1;
    downloadBlob(pending.blob, pending.fileName);
    addCaptured(pending.groups, pending.order);
    setPending(null);
  }, [pending, addCaptured]);

  const retake = useCallback(() => {
    setPending(null);
    void runCapture();
  }, [runCapture]);

  const discardPending = useCallback(() => setPending(null), []);

  // ── 다음 구역 ─────────────────────────────────────────────────────────────
  const onNext = useCallback(() => {
    const ctx = mapRef.current;
    if (!ctx?.map || savingRef.current || autoRef.current) return;
    const { frameRect: frame, labelByGroup: labels, capturedGroups: done } = latestRef.current;
    const pts = pointsRef.current;
    const remaining = remainingWithPoints(pts, labels, done);
    const anchor = nextAnchor(remaining, (id) => labels.get(id));
    if (anchor === undefined) return;
    const plan = planNextWindow(pts, remaining, anchor, frame, MARKER_RADIUS_PX);
    if (!plan) return;
    const { dx, dy } = roundPan(pts, plan, frame, MARKER_RADIUS_PX);
    if (dx !== 0 || dy !== 0) ctx.panBy(dx, dy);
    // 이미 그 자리면 눌러도 아무 변화가 없어 고장처럼 보인다. 다음 할 일을 알려 준다.
    else showInfo('지금 프레임이 이미 다음 구역입니다.');
  }, []);

  // ── 남은 구역 모두 저장 ───────────────────────────────────────────────────
  const stopAuto = useCallback(() => {
    autoRef.current?.stop();
  }, []);

  const runAuto = useCallback(async () => {
    const el = canvasEl;
    const ctx = mapRef.current;
    if (!el || !ctx?.map || savingRef.current || autoRef.current) return;

    const { frameRect: frame0, labelByGroup: labels, capturedGroups: done0 } = latestRef.current;
    const orderOf = (id: number) => labels.get(id);
    const estimate = planPages(
      pointsRef.current,
      remainingWithPoints(pointsRef.current, labels, done0),
      orderOf,
      frame0,
      MARKER_RADIUS_PX,
      MAX_PLAN_PAGES,
    ).length;
    if (estimate === 0) return;
    const firstShare = latestRef.current.live
      ? ''
      : '\n화면 공유 창이 뜨면 이 탭을 골라 주세요.';
    const ok = window.confirm(
      `지금 확대 수준으로 약 ${estimate}장을 차례로 저장합니다.${firstShare}\n크롬이 '여러 파일 다운로드'를 물으면 허용해 주세요.\n도중에 멈추려면 Esc를 누르세요.`,
    );
    // 확인 창이 떠 있는 동안 캡처 모드가 닫혔거나 다른 저장이 시작됐으면 그만둔다.
    if (!ok || savingRef.current || autoRef.current || mapRef.current !== ctx) return;

    const session = sessionRef.current;
    let stopped = false;
    let wake: () => void = () => {};
    const stopSignal = new Promise<void>((resolve) => {
      wake = resolve;
    });
    const run: AutoRun = {
      stop: () => {
        stopped = true;
        wake();
      },
    };
    const isStopped = () => stopped || session !== sessionRef.current;

    autoRef.current = run;
    savingRef.current = true;
    const local = new Set(done0);
    const order = latestRef.current.finalOrder;
    let done = 0;
    let total = estimate;
    setAutoProgress({ done, total });

    try {
      for (let step = 0; step < estimate + EXTRA_AUTO_STEPS; step++) {
        if (isStopped()) return;
        // 도중에 브라우저의 "공유 중지"를 누르면 다음 장에서 허락 창이 다시 뜬다. 그 전에 멈춘다.
        if (done > 0 && !latestRef.current.live) {
          showInfo('화면 공유가 끝나 자동 저장을 멈췄습니다.');
          break;
        }
        const pts = pointsRef.current;
        const frame = latestRef.current.frameRect;
        const remaining = remainingWithPoints(pts, labels, local);
        const anchor = nextAnchor(remaining, orderOf);
        if (anchor === undefined) break;
        const plan = planNextWindow(pts, remaining, anchor, frame, MARKER_RADIUS_PX);
        if (!plan) break;
        const { dx, dy } = roundPan(pts, plan, frame, MARKER_RADIUS_PX);

        if (dx !== 0 || dy !== 0) {
          const before = pts.find((p) => p.groupId === anchor);
          ctx.panBy(dx, dy);
          // 이벤트를 놓치지 않게 panBy 바로 뒤, 다른 await보다 먼저 건다.
          const idle = ctx.whenIdle(IDLE_TIMEOUT_MS);
          await Promise.race([idle, stopSignal]);
          if (isStopped()) return;
          // `tilesloaded`가 이동 애니메이션 도중에 올 수도 있다. anchor 좌표가 옮긴 자리로
          // 갱신될 때까지(MarkerPositions는 rAF로 묶어 올린다) 프레임 단위로 기다린다.
          if (before) {
            const tx = before.x - dx;
            const ty = before.y - dy;
            const deadline = performance.now() + SETTLE_TIMEOUT_MS;
            for (;;) {
              await nextFrames(1);
              if (isStopped()) return;
              const now = pointsRef.current.find((p) => p.groupId === anchor);
              if (now && Math.abs(now.x - tx) < 1 && Math.abs(now.y - ty) < 1) break;
              if (performance.now() > deadline) break;
            }
          }
        }
        // 좌표 갱신(rAF) → 렌더가 한 번 더 지나가게 둔다.
        await nextFrames(1);
        if (isStopped()) return;

        setHidden(true);
        await nextFrames(2);
        if (isStopped()) return;
        const shot = await shootFrame(el, session);
        setHidden(false);
        // 중지를 눌렀어도 이미 찍은 장은 내려받는다. 캡처 모드를 닫았거나 화면을 떠났으면 버린다.
        if (session !== sessionRef.current) return;

        const fresh = shot.inside.filter((id) => !local.has(id));
        if (fresh.length === 0) {
          showInfo('더 옮겨 찍을 곳을 찾지 못해 자동 저장을 멈췄습니다.');
          break;
        }
        shotCountRef.current += 1;
        downloadBlob(shot.blob, mapImageFileName(latestRef.current.fileName, shotCountRef.current));
        for (const id of fresh) local.add(id);
        addCaptured(fresh, order);

        done++;
        const left = remainingWithPoints(pointsRef.current, labels, local).size;
        if (left === 0) total = done;
        else if (done >= total) total = done + 1;
        setAutoProgress({ done, total });
        if (left === 0) break;
      }
    } catch (err) {
      if (session === sessionRef.current) reportGrabError(err);
    } finally {
      if (autoRef.current === run) autoRef.current = null;
      savingRef.current = false;
      setHidden(false);
      setAutoProgress(null);
    }
  }, [canvasEl, shootFrame, addCaptured]);

  const saveAll = useCallback(() => {
    void runAuto();
  }, [runAuto]);

  // ── 지도 조작 ─────────────────────────────────────────────────────────────
  // `panBy`는 `dragstart`/`zoom_start`를 내지 않으므로, 자동 저장 중 시작 이벤트는 사용자 조작이다.
  const onInteractStart = useCallback(() => {
    setFaded(true);
    if (autoRef.current) {
      autoRef.current.stop();
      showInfo('지도를 움직여 자동 저장을 멈췄습니다.');
    }
  }, []);
  const onInteractEnd = useCallback(() => setFaded(false), []);

  // 캡처하는 동안에는 토스트도 숨긴다. 토스트는 닫을 때까지 남아 있어서(예: 엑셀 다운로드 안내)
  // 지도 아래쪽에 그대로 찍힌다. 토스트는 화면 밖(App)에 있으므로 루트 클래스로 숨긴다.
  useEffect(() => {
    if (!hidden) return;
    document.documentElement.classList.add('ro-capturing');
    return () => document.documentElement.classList.remove('ro-capturing');
  }, [hidden]);

  // Esc: 자동 저장 중이면 중지, 아니면 캡처 모드 닫기. 미리보기가 떠 있으면 Esc는 모달이 받는다.
  const autoRunning = autoProgress !== null;
  useEffect(() => {
    if (!active || (pending && !autoRunning)) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== 'Escape' || e.defaultPrevented || e.isComposing) return;
      if (autoRef.current) {
        e.preventDefault();
        autoRef.current.stop();
        return;
      }
      if (isTypingTarget(e.target)) return;
      // 다른 모달(예: 한도 초과 안내)이 떠 있으면 그 모달만 닫히게 둔다.
      if (document.querySelector('.ro-modal')) return;
      exit();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [active, pending, autoRunning, exit]);

  const frame: CaptureFrameProps | null = active
    ? {
        rect: frameRect,
        label,
        edgeMarkers,
        capturedMarkers,
        markerRadius: MARKER_RADIUS_PX,
        hidden,
        faded,
      }
    : null;

  const toolbar: CaptureToolbarProps = {
    orientation,
    onOrientation: setOrientation,
    remainingCount,
    totalCount,
    onNext,
    onSave: save,
    onSaveAll: saveAll,
    autoProgress,
    onStopAuto: stopAuto,
    saving,
    supported: capture.supported,
    faded,
    hidden,
    onClose: exit,
  };

  const preview: CapturePreviewModalProps | null =
    active && pending
      ? {
          blob: pending.blob,
          fileName: pending.fileName,
          onSave: savePending,
          onRetake: retake,
          onClose: discardPending,
        }
      : null;

  return {
    active,
    enter,
    exit,
    attachCanvas: setCanvasEl,
    onPoints,
    onMapContext,
    interaction: { onInteractStart, onInteractEnd },
    frame,
    toolbar,
    preview,
  };
}
