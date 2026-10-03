import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import {
  MARKER_RADIUS_PX,
  classifyPoints,
  clampRect,
  formatOrderRanges,
  includesKakaoLogo,
} from '../capture/geometry';
import type { MarkerPoint, Rect, Size } from '../capture/types';
import { WRONG_SURFACE_ERROR, useTabCapture } from '../capture/useTabCapture';
import type {
  CaptureFrameProps,
  CapturePreviewModalProps,
  CaptureToolbarProps,
} from '../components';
import { downloadBlob, mapImageFileName } from '../io';
import { showInfo } from '../store/toast';
import type { LatLng, PrimaryGroup } from '../types';
import { showApiError } from './helpers';

/** 영역 최소 한 변(px). `CaptureFrame`의 최소 크기와 같다 */
const MIN_REGION_PX = 120;
/** 영역 지정을 처음 켰을 때 지도 컨테이너 대비 크기 */
const INITIAL_REGION_RATIO = 0.7;
/** 잘라 낸 영역에 카카오 로고가 없을 때 이미지에 덧그리는 출처 문구 */
const ATTRIBUTION = '지도 © Kakao';

const EMPTY_SET: ReadonlySet<number> = new Set();

export interface UseMapCaptureInput {
  /** 지도 마커(1차 그룹) 목록. `MarkerLayer`에 넘기는 배열과 같다 */
  groups: PrimaryGroup[];
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
  /** 캡처 모드를 끝낸다(스트림 종료·영역 지정 해제) */
  exit: () => void;
  /** `.ro-mapscreen__canvas`에 붙이는 ref. 크기를 재고 캡처 좌표의 기준으로 쓴다 */
  attachCanvas: (el: HTMLDivElement | null) => void;
  /** `<MarkerPositions onChange>`에 연결 */
  onPoints: (points: MarkerPoint[]) => void;
  /** `<MapFit points>`에 연결. `남은 순번 보기`를 누를 때마다 새 배열이 된다 */
  fitPoints: LatLng[];
  /** 영역 지정 중일 때만 값이 있다. 없으면 `CaptureFrame`을 마운트하지 않는다 */
  frame: CaptureFrameProps | null;
  /** 캡처 모드 툴바 props */
  toolbar: CaptureToolbarProps;
  /** 미리보기를 띄울 때만 값이 있다 */
  preview: CapturePreviewModalProps | null;
}

/** 미리보기 대기 중인 한 장. 찍힌 그룹은 저장 시점이 아니라 grab 시점 기준이다 */
interface PendingShot {
  blob: Blob;
  fileName: string;
  /** grab 시점에 영역 안에 온전히 들어 있던 그룹 */
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

/** 지도 컨테이너 가운데에 놓인 `INITIAL_REGION_RATIO` 크기의 사각형 */
function centeredRegion(bounds: Size): Rect {
  const width = bounds.width * INITIAL_REGION_RATIO;
  const height = bounds.height * INITIAL_REGION_RATIO;
  return {
    x: (bounds.width - width) / 2,
    y: (bounds.height - height) / 2,
    width,
    height,
  };
}

function isTypingTarget(target: EventTarget | null): boolean {
  return (
    target instanceof HTMLElement &&
    target.closest('input, textarea, select, [contenteditable=""], [contenteditable="true"]') !==
      null
  );
}

/**
 * S6 지도 캡처 모드의 상태와 동작을 한데 묶는다.
 *
 * 화면(`ResultScreen`)은 돌려받은 props를 `CaptureFrame`·`CaptureToolbar`·
 * `CapturePreviewModal`·`MarkerPositions`·`MapFit`에 꽂기만 한다.
 *
 * - 영역: 영역 지정이 꺼져 있으면 지도 컨테이너 전체, 켜져 있으면 프레임 사각형이다.
 * - 찍힌 순번: 미리보기에서 `저장`을 누른 장만 센다. 어느 그룹이 찍혔는지는
 *   grab 시점의 영역 판정을 그대로 쓴다(저장을 누르기 전에 지도가 움직여도 흔들리지 않게).
 * - 최종 순서가 바뀌면 찍은 순번 기록은 의미가 없으므로 비운다. 캡처 모드를 닫았다
 *   다시 열어도 같은 순서라면 기록은 남는다.
 */
export function useMapCapture({
  groups,
  labelByGroup,
  finalOrder,
  fileName,
}: UseMapCaptureInput): MapCapture {
  const capture = useTabCapture();

  const [active, setActive] = useState(false);
  const [regionMode, setRegionMode] = useState(false);
  /** 사용자가 정한 영역. null이면 가운데 기본 영역을 쓴다 */
  const [rect, setRect] = useState<Rect | null>(null);
  /** 캡처 직전 장식 숨김 */
  const [hidden, setHidden] = useState(false);
  const [saving, setSaving] = useState(false);
  const [points, setPoints] = useState<MarkerPoint[]>([]);
  const [fitPoints, setFitPoints] = useState<LatLng[]>([]);
  const [pending, setPending] = useState<PendingShot | null>(null);
  /** 지금까지 저장한 장 수. 파일 이름 번호로 쓰며, 이름이 겹치지 않도록 화면이 살아 있는 동안 계속 센다 */
  const [shotCount, setShotCount] = useState(0);
  /** 찍은 그룹. 어느 최종 순서 기준인지 함께 들고 있다가 순서가 바뀌면 빈 것으로 본다 */
  const [captured, setCaptured] = useState<{
    order: readonly number[];
    groups: ReadonlySet<number>;
  }>(() => ({ order: finalOrder, groups: EMPTY_SET }));
  const capturedGroups = captured.order === finalOrder ? captured.groups : EMPTY_SET;

  // ── 지도 컨테이너 크기 ────────────────────────────────────────────────────
  const [canvasEl, setCanvasEl] = useState<HTMLDivElement | null>(null);
  const [bounds, setBounds] = useState<Size>({ width: 0, height: 0 });

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

  // ── 영역과 마커 판정 ──────────────────────────────────────────────────────
  // 저장된 영역은 그대로 두고 화면에 쓸 값만 잘라 낸다. 창을 줄였다 다시 키우면
  // 사용자가 정한 영역이 되살아난다.
  const regionRect = useMemo(
    () => clampRect(rect ?? centeredRegion(bounds), bounds, MIN_REGION_PX),
    [rect, bounds],
  );
  const effectiveRect = useMemo<Rect>(
    () => (regionMode ? regionRect : { x: 0, y: 0, width: bounds.width, height: bounds.height }),
    [regionMode, regionRect, bounds],
  );

  /** 순번이 붙은 그룹의 마커만. 순번이 없으면 찍을 대상이 아니다 */
  const labeledPoints = useMemo(
    () => points.filter((p) => labelByGroup.has(p.groupId)),
    [points, labelByGroup],
  );
  const placement = useMemo(
    () => classifyPoints(labeledPoints, effectiveRect, MARKER_RADIUS_PX),
    [labeledPoints, effectiveRect],
  );
  const insideGroups = useMemo(
    () => labeledPoints.filter((p) => placement.get(p.groupId) === 'inside').map((p) => p.groupId),
    [labeledPoints, placement],
  );
  // 전체 화면일 때 지도 테두리에 잘리는 마커는 표시하지 않는다(프레임이 없다).
  const edgeMarkers = useMemo(
    () =>
      regionMode ? labeledPoints.filter((p) => placement.get(p.groupId) === 'edge') : undefined,
    [regionMode, labeledPoints, placement],
  );

  const insideText = useMemo(() => {
    if (insideGroups.length === 0) return '';
    const nums = insideGroups.flatMap((id) => labelByGroup.get(id) ?? []);
    return `${formatOrderRanges(nums)}번(${insideGroups.length}곳)`;
  }, [insideGroups, labelByGroup]);

  // ── 찍은 순번 / 남은 순번 ─────────────────────────────────────────────────
  const remainingGroups = useMemo(
    () => [...labelByGroup.keys()].filter((id) => !capturedGroups.has(id)),
    [labelByGroup, capturedGroups],
  );
  const remainingText = useMemo(
    () => formatOrderRanges(remainingGroups.flatMap((id) => labelByGroup.get(id) ?? [])),
    [remainingGroups, labelByGroup],
  );
  const totalCount = labelByGroup.size;
  const capturedCount = totalCount - remainingGroups.length;

  const groupById = useMemo(() => new Map(groups.map((g) => [g.id, g])), [groups]);

  const showRemaining = useCallback(() => {
    const next = remainingGroups.flatMap((id) => {
      const g = groupById.get(id);
      return g ? [{ lat: g.lat, lon: g.lon }] : [];
    });
    // 매번 새 배열이라 같은 버튼을 다시 눌러도 MapFit이 다시 맞춘다.
    if (next.length > 0) setFitPoints(next);
  }, [remainingGroups, groupById]);

  // ── 진입 · 종료 ───────────────────────────────────────────────────────────
  /** 캡처 모드 세대. 닫는 순간 올려, 진행 중이던 캡처 결과가 늦게 도착해도 버린다 */
  const sessionRef = useRef(0);
  const savingRef = useRef(false);

  const enter = useCallback(() => setActive(true), []);

  const { stop } = capture;
  const exit = useCallback(() => {
    sessionRef.current++;
    stop();
    setActive(false);
    setRegionMode(false);
    setRect(null);
    setHidden(false);
    setPending(null);
    setPoints([]);
  }, [stop]);

  const toggleRegion = useCallback(() => setRegionMode((v) => !v), []);

  // ── 저장 흐름 ─────────────────────────────────────────────────────────────
  // 비동기 흐름 도중(허락 창·공유 중 바로 레이아웃이 바뀐 뒤)에도 최신 값을 읽도록 ref로 들고 있는다.
  const latestRef = useRef({ effectiveRect, bounds, insideGroups, shotCount, fileName, finalOrder });
  useEffect(() => {
    latestRef.current = { effectiveRect, bounds, insideGroups, shotCount, fileName, finalOrder };
  });

  const { grab } = capture;
  const wasLive = capture.status === 'live';

  const runCapture = useCallback(async () => {
    const el = canvasEl;
    if (!el || savingRef.current) return;
    const session = sessionRef.current;
    savingRef.current = true;
    setSaving(true);
    setHidden(true);

    /** 지금 영역을 한 장 찍는다. 좌표는 찍는 순간 다시 읽는다 */
    const shoot = async () => {
      const { effectiveRect: r, bounds: b, insideGroups: inside } = latestRef.current;
      const layout = readLayout(el);
      const blob = await grab(
        { x: layout.left + r.x, y: layout.top + r.y, width: r.width, height: r.height },
        { attribution: includesKakaoLogo(r, b) ? undefined : ATTRIBUTION },
      );
      return { blob, layout, inside: inside.slice() };
    };

    try {
      // 툴바·어두운 처리를 숨긴 화면이 실제로 그려질 때까지 기다린다.
      await nextFrames(2);
      let shot = await shoot();
      // 첫 캡처는 허락 직후 크롬의 "공유 중" 바가 뜨며 화면 높이가 바뀔 수 있다.
      // 그러면 앞서 읽은 좌표가 어긋나므로, 배치가 자리 잡은 뒤 한 번 더 찍는다.
      if (!wasLive && session === sessionRef.current) {
        await nextFrames(3);
        if (!sameLayout(shot.layout, readLayout(el))) shot = await shoot();
      }
      if (session !== sessionRef.current) return;
      const { shotCount: n, fileName: name, finalOrder: order } = latestRef.current;
      setPending({
        blob: shot.blob,
        fileName: mapImageFileName(name, n + 1),
        groups: shot.inside,
        order,
      });
    } catch (err) {
      if (session !== sessionRef.current) return;
      const errName = err instanceof DOMException ? err.name : '';
      if (errName === 'NotAllowedError') {
        showInfo('화면 공유를 허용해야 이미지로 저장할 수 있습니다.');
      } else if (errName === WRONG_SURFACE_ERROR) {
        showInfo('공유 창에서 이 탭을 골라야 지도를 저장할 수 있습니다.');
      } else if (errName !== 'AbortError') {
        // 브라우저가 던진 DOMException 메시지는 영어라 한국어 문구로 바꿔 보여 준다.
        showApiError(err instanceof DOMException ? null : err, '지도 이미지를 만들지 못했습니다.');
      }
    } finally {
      savingRef.current = false;
      setSaving(false);
      setHidden(false);
    }
  }, [canvasEl, grab, wasLive]);

  const save = useCallback(() => {
    void runCapture();
  }, [runCapture]);

  const savePending = useCallback(() => {
    if (!pending) return;
    downloadBlob(pending.blob, pending.fileName);
    setShotCount((n) => n + 1);
    setCaptured((prev) => {
      if (pending.order !== finalOrder) return prev;
      const base = prev.order === finalOrder ? prev.groups : EMPTY_SET;
      return { order: finalOrder, groups: new Set([...base, ...pending.groups]) };
    });
    setPending(null);
  }, [pending, finalOrder]);

  const retake = useCallback(() => {
    setPending(null);
    void runCapture();
  }, [runCapture]);

  const discardPending = useCallback(() => setPending(null), []);

  // 캡처하는 동안에는 토스트도 숨긴다. 토스트는 닫을 때까지 남아 있어서(예: 엑셀 다운로드 안내)
  // 지도 아래쪽에 그대로 찍힌다. 토스트는 화면 밖(App)에 있으므로 루트 클래스로 숨긴다.
  useEffect(() => {
    if (!hidden) return;
    document.documentElement.classList.add('ro-capturing');
    return () => document.documentElement.classList.remove('ro-capturing');
  }, [hidden]);

  // Esc로 캡처 모드 닫기. 미리보기가 떠 있으면 Esc는 모달이 받는다.
  useEffect(() => {
    if (!active || pending) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== 'Escape' || e.defaultPrevented || e.isComposing) return;
      if (isTypingTarget(e.target)) return;
      // 다른 모달(예: 한도 초과 안내)이 떠 있으면 그 모달만 닫히게 둔다.
      if (document.querySelector('.ro-modal')) return;
      exit();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [active, pending, exit]);

  const frame: CaptureFrameProps | null =
    active && regionMode
      ? {
          rect: regionRect,
          bounds,
          onChange: setRect,
          edgeMarkers,
          markerRadius: MARKER_RADIUS_PX,
          hidden,
        }
      : null;

  const toolbar: CaptureToolbarProps = {
    supported: capture.supported,
    regionMode,
    onToggleRegion: toggleRegion,
    onSave: save,
    saving,
    onShowRemaining: showRemaining,
    remainingText,
    insideText,
    capturedCount,
    totalCount,
    onClose: exit,
    hidden,
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
    onPoints: setPoints,
    fitPoints,
    frame,
    toolbar,
    preview,
  };
}
