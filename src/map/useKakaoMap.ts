import { useCallback, useEffect, useRef, useState } from 'react';
import type { RefObject } from 'react';

import { clampLevel, createWheelAccumulator, normalizeWheelDelta } from './wheelZoom';
import type { LatLng } from '../types';

/**
 * 카카오 SDK가 만들어 준 지도 인스턴스.
 * 공식 타입 패키지가 없으므로 내부적으로는 `any`로 다룬다(공개 props에는 노출하지 않는다).
 */
export type KakaoMap = any;

/** 지도 초기 설정. 최초 1회 생성에만 쓰이고 이후 변경은 무시한다. */
export interface UseKakaoMapOptions {
  /** 초기 중심 좌표 */
  center: LatLng;
  /** 초기 확대 레벨(작을수록 확대) */
  level?: number;
}

/** `useKakaoMap`이 돌려주는 값. */
export interface UseKakaoMapResult {
  /** 생성된 지도. 준비 전에는 null(= 아직 준비되지 않음) */
  map: KakaoMap | null;
  /** SDK 미로드 등 치명적 오류 메시지. 정상이면 null */
  error: string | null;
  /** 주어진 좌표들이 모두 보이도록 화면을 맞춘다. 좌표가 없으면 아무 일도 하지 않는다 */
  fitBounds: (points: LatLng[]) => void;
  /**
   * 컨테이너 크기가 바뀐 뒤 지도에 다시 재라고 알린다(`map.relayout()`).
   * 창 크기 변경은 SDK가 스스로 처리하므로, 레이아웃 변화로 컨테이너만 바뀐 경우에만 부른다.
   */
  relayout: () => void;
  /** 지도를 화면 px만큼 옮긴다(`map.panBy(dx, dy)`). 양수 dx는 지도를 왼쪽으로 밀어 오른쪽을 보여 준다 */
  panBy: (dx: number, dy: number) => void;
  /**
   * 부른 뒤 지도가 멈추고(`idle`) 타일까지 받으면(`tilesloaded`, 순서 무관) 끝나는 Promise.
   * `idle` 뒤 `tilesloaded`가 잠깐(약 0.6초) 안에 오지 않으면(이미 받은 타일 등) 그때 끝나고,
   * 이벤트가 `timeoutMs` 안에 오지 않으면(움직임이 없어 이벤트가 안 오는 경우 등) 그때 끝난다.
   * 지도가 없으면 바로 끝난다. 실패(reject)하지 않는다.
   */
  whenIdle: (timeoutMs: number) => Promise<void>;
  /**
   * 지도 중심을 기준으로 확대 레벨을 `delta`만큼 바꾼다(부드럽게). 카카오 레벨은 클수록 축소라
   * `+1`은 축소, `−1`은 확대다. 레벨 범위(기본 1~14, 지도에 설정된 min/max)로 자르고,
   * 바뀌지 않으면 아무것도 하지 않는다. 지도가 없으면 무시한다.
   */
  zoomBy: (delta: number) => void;
}

/** 카카오 지도 레벨 범위(작을수록 확대) */
const LEVEL_MIN = 1;
const LEVEL_MAX = 14;

/** 휠·버튼으로 레벨을 바꿀 때의 애니메이션 시간(ms) */
const ZOOM_ANIMATE_MS = 200;

/**
 * 트랙패드 핀치(크롬·사파리는 `ctrlKey: true`인 wheel로 온다)의 이동량 배율.
 * 핀치는 한 번에 몇 px씩만 와서 그대로면 한 단계까지 너무 오래 걸린다.
 */
const PINCH_GAIN = 5;

/** 더블클릭 뒤 카카오가 스스로 확대했는지 확인하기까지 기다리는 시간(ms) */
const DBLCLICK_CHECK_MS = 60;

/** 지도에 설정된 레벨 범위. SDK에 getter가 없으면 기본 1~14 */
function levelRange(m: KakaoMap): { min: number; max: number } {
  const min = typeof m.getMinLevel === 'function' ? Number(m.getMinLevel()) : NaN;
  const max = typeof m.getMaxLevel === 'function' ? Number(m.getMaxLevel()) : NaN;
  return {
    min: Number.isFinite(min) ? Math.max(min, LEVEL_MIN) : LEVEL_MIN,
    max: Number.isFinite(max) ? Math.min(max, LEVEL_MAX) : LEVEL_MAX,
  };
}

/** 터치가 주 입력인 기기인지(휴대폰·태블릿). 이 경우 카카오 기본 핀치 확대를 그대로 둔다 */
function isCoarsePointer(): boolean {
  return typeof window.matchMedia === 'function' && window.matchMedia('(pointer: coarse)').matches;
}

/**
 * 카카오 기본 휠 확대를 끄고 안정화한 휠 확대로 바꾼다. 해제 함수를 돌려준다.
 *
 * - `map.setZoomable(false)`로 SDK 휠 확대를 끄고, 컨테이너에 `wheel`(캡처 단계, passive: false)을 단다.
 * - 휠 입력은 `createWheelAccumulator`로 모아 한 칸(크롬 100px·파이어폭스 48px)마다 정확히 1레벨, 바꾼 뒤 250ms는 무시한다.
 * - 커서 위치(`coordsFromContainerPoint`)를 기준점(anchor)으로 200ms 애니메이션한다.
 * - `ctrlKey`가 켜진 wheel(트랙패드 핀치, Ctrl+휠)도 확대로 처리하고 브라우저 페이지 확대는 막는다.
 * - `setZoomable(false)`가 더블클릭 확대까지 끄는 경우에 대비해, 더블클릭 뒤 카카오가 확대를
 *   시작하지 않았으면(`zoom_start` 없음, 레벨 그대로) 더블클릭 지점을 기준으로 1레벨 확대한다.
 *   카카오가 스스로 확대했다면 아무것도 하지 않는다(두 번 확대되지 않게).
 * - 터치가 주 입력인 기기(`pointer: coarse`)에서는 아무것도 하지 않는다 — `setZoomable(false)`가
 *   핀치 확대까지 끌 수 있어서다. 터치 노트북처럼 주 입력이 마우스인 기기에서는 터치스크린 핀치가
 *   꺼질 수 있다(수동 확인 필요).
 */
function stabilizeWheelZoom(m: KakaoMap, el: HTMLElement): () => void {
  if (isCoarsePointer()) return () => {};

  m.setZoomable(false);
  const accumulate = createWheelAccumulator({ cooldownMs: 250 });

  const onWheel = (e: WheelEvent) => {
    // 세로 이동이 없는 가로 스크롤은 지도와 무관하다.
    if (e.deltaY === 0) return;
    e.preventDefault();
    const rect = el.getBoundingClientRect();
    const px = normalizeWheelDelta(e, rect.height) * (e.ctrlKey ? PINCH_GAIN : 1);
    const step = accumulate(px, performance.now());
    if (step === 0) return;

    const { min, max } = levelRange(m);
    const cur = Number(m.getLevel());
    const next = clampLevel(cur + step, min, max);
    if (next === cur) return;
    const point = new kakao.maps.Point(e.clientX - rect.left, e.clientY - rect.top);
    const anchor = m.getProjection().coordsFromContainerPoint(point);
    m.setLevel(next, { anchor, animate: { duration: ZOOM_ANIMATE_MS } });
  };

  // 더블클릭 확대 복원: 카카오가 확대를 시작했는지 `zoom_start` 시각으로 본다.
  // 카카오 내부 더블클릭 확대가 우리 `dblclick` 리스너보다 먼저 돌 수도 있어(그러면 `before`가 이미
  // 바뀐 레벨이다), 더블클릭 직전 짧은 구간의 `zoom_start`도 카카오가 확대한 것으로 친다.
  let lastZoomStart = -Infinity;
  let dblTimer: ReturnType<typeof setTimeout> | undefined;
  const onZoomStart = () => {
    lastZoomStart = performance.now();
  };
  const onDblClick = (mouseEvent: { latLng?: unknown }) => {
    const at = performance.now();
    const before = Number(m.getLevel());
    const anchor = mouseEvent?.latLng;
    clearTimeout(dblTimer);
    dblTimer = setTimeout(() => {
      if (lastZoomStart >= at - DBLCLICK_CHECK_MS || Number(m.getLevel()) !== before) return;
      const { min, max } = levelRange(m);
      const next = clampLevel(before - 1, min, max);
      if (next === before) return;
      m.setLevel(next, anchor ? { anchor, animate: true } : { animate: true });
    }, DBLCLICK_CHECK_MS);
  };

  el.addEventListener('wheel', onWheel, { capture: true, passive: false });
  kakao.maps.event.addListener(m, 'zoom_start', onZoomStart);
  kakao.maps.event.addListener(m, 'dblclick', onDblClick);

  return () => {
    clearTimeout(dblTimer);
    el.removeEventListener('wheel', onWheel, { capture: true });
    kakao.maps.event.removeListener(m, 'zoom_start', onZoomStart);
    kakao.maps.event.removeListener(m, 'dblclick', onDblClick);
    m.setZoomable(true);
  };
}

/** `whenIdle`: `idle` 뒤 `tilesloaded`를 더 기다리는 최대 시간(ms) */
const TILE_GRACE_MS = 600;

const SDK_MISSING =
  '카카오맵 SDK를 불러오지 못했습니다. JavaScript 키 설정(.env.local의 VITE_KAKAO_JS_KEY)을 확인하세요.';

/**
 * 카카오맵 SDK 로드를 기다렸다가 지도를 **한 번만** 만든다.
 *
 * `index.html`이 `autoload=false`로 SDK를 넣으므로 반드시 `kakao.maps.load` 콜백 뒤에 생성한다.
 * StrictMode 이중 마운트에 대비해 인스턴스는 ref로 보관한다.
 */
export function useKakaoMap(
  containerRef: RefObject<HTMLDivElement | null>,
  options: UseKakaoMapOptions,
): UseKakaoMapResult {
  const mapRef = useRef<KakaoMap | null>(null);
  const [map, setMap] = useState<KakaoMap | null>(null);
  // SDK 존재 여부는 렌더 시점에 그대로 읽어 파생한다(effect에서 setState 하지 않는다).
  // index.html이 autoload=false로 스크립트를 넣으므로 마운트 시점엔 이미 결정돼 있다.
  const sdkPresent = typeof window !== 'undefined' && Boolean(window.kakao?.maps);
  const error = sdkPresent ? null : SDK_MISSING;

  // 초기 옵션은 생성 시점에만 읽는다. 이후 center/level 변경은 지도 API로 직접 다룬다.
  // (렌더 중 ref 쓰기를 피하려고 아래 생성 effect보다 먼저 선언한 effect에서 갱신한다.)
  const optionsRef = useRef(options);
  useEffect(() => {
    optionsRef.current = options;
  });

  useEffect(() => {
    const sdk = window.kakao;
    if (!sdk?.maps) return;

    let cancelled = false;
    sdk.maps.load(() => {
      if (cancelled) return;
      const el = containerRef.current;
      if (!el) return;
      if (!mapRef.current) {
        const { center, level } = optionsRef.current;
        mapRef.current = new kakao.maps.Map(el, {
          center: new kakao.maps.LatLng(center.lat, center.lon),
          level: level ?? 6,
        });
      }
      setMap(mapRef.current);
    });

    return () => {
      cancelled = true;
    };
  }, [containerRef]);

  // 휠 확대 안정화. 지도가 생긴 뒤 한 번 단다.
  // 현재 레벨은 여기서 상태로 들고 있지 않는다(`MapLevel.tsx`의 `useMapLevel`이 따로 구독한다).
  useEffect(() => {
    if (!map) return;
    const el = containerRef.current;
    return el ? stabilizeWheelZoom(map, el) : undefined;
  }, [map, containerRef]);

  const fitBounds = useCallback((points: LatLng[]) => {
    const m = mapRef.current;
    if (!m || points.length === 0) return;
    const bounds = new kakao.maps.LatLngBounds();
    for (const p of points) bounds.extend(new kakao.maps.LatLng(p.lat, p.lon));
    m.setBounds(bounds);
  }, []);

  const relayout = useCallback(() => {
    mapRef.current?.relayout();
  }, []);

  const panBy = useCallback((dx: number, dy: number) => {
    mapRef.current?.panBy(dx, dy);
  }, []);

  const whenIdle = useCallback((timeoutMs: number) => {
    const m = mapRef.current;
    if (!m) return Promise.resolve();
    return new Promise<void>((resolve) => {
      let sawIdle = false;
      let sawTiles = false;
      let grace: ReturnType<typeof setTimeout> | undefined;
      const done = () => {
        clearTimeout(timer);
        clearTimeout(grace);
        kakao.maps.event.removeListener(m, 'idle', onIdle);
        kakao.maps.event.removeListener(m, 'tilesloaded', onTiles);
        resolve();
      };
      // `idle`은 이동이 끝나면 오지만 타일은 아직 받는 중일 수 있다. 멀리 옮겨 새 타일을 받는 경우
      // `tilesloaded`가 뒤따르므로 조금 더 기다린다. 이미 받아 둔 타일이면 `tilesloaded`가 오지
      // 않을 수 있어 `idle` 뒤 TILE_GRACE_MS가 지나면 끝낸다.
      const onIdle = () => {
        sawIdle = true;
        if (sawTiles) done();
        else if (grace === undefined) grace = setTimeout(done, TILE_GRACE_MS);
      };
      const onTiles = () => {
        sawTiles = true;
        if (sawIdle) done();
      };
      const timer = setTimeout(done, timeoutMs);
      kakao.maps.event.addListener(m, 'idle', onIdle);
      kakao.maps.event.addListener(m, 'tilesloaded', onTiles);
    });
  }, []);

  const zoomBy = useCallback((delta: number) => {
    const m = mapRef.current;
    if (!m || !Number.isFinite(delta) || delta === 0) return;
    const { min, max } = levelRange(m);
    const cur = Number(m.getLevel());
    const next = clampLevel(cur + delta, min, max);
    if (next === cur) return;
    m.setLevel(next, { animate: { duration: ZOOM_ANIMATE_MS } });
  }, []);

  return { map, error, fitBounds, relayout, panBy, whenIdle, zoomBy };
}
