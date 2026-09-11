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
}

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

  return { map, error, fitBounds };
}
