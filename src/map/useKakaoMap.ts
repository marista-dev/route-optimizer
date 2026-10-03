import { useCallback, useEffect, useRef, useState } from 'react';
import type { RefObject } from 'react';

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

  return { map, error, fitBounds, relayout, panBy, whenIdle };
}
