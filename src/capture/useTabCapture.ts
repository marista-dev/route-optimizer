import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import { cropToVideo } from './geometry';
import type { Rect, TabCapture, TabCaptureStatus } from './types';

/**
 * `getDisplayMedia` 옵션. 현재 탭을 먼저 권하고, 다른 탭으로 바꿔 타지 못하게 막는다.
 * `preferCurrentTab` 등 크롬 전용 필드는 lib.dom 타입에 없어 따로 넓힌다.
 */
const DISPLAY_MEDIA_OPTIONS = {
  video: { displaySurface: 'browser', frameRate: 30 },
  audio: false,
  preferCurrentTab: true,
  selfBrowserSurface: 'include',
  surfaceSwitching: 'exclude',
  monitorTypeSurfaces: 'exclude',
} as DisplayMediaStreamOptions;

/**
 * 새 프레임을 기다리는 최대 시간(ms).
 * 탭 캡처는 화면이 바뀔 때만 프레임을 보내므로, 아무것도 안 바뀌면 영영 오지 않는다.
 */
const FRAME_TIMEOUT_MS = 200;

/** 공유 창에서 탭이 아닌 창·전체 화면을 골랐을 때 `grab`이 던지는 `DOMException`의 name */
export const WRONG_SURFACE_ERROR = 'WrongSurfaceError';

/**
 * 탭 단위 캡처를 쓸 수 있는지.
 *
 * `getDisplayMedia`만 보면 파이어폭스·사파리도 통과하지만, 둘은 창·전체 화면만 공유할 수 있어
 * 탭 좌표로 잘라 내면 엉뚱한 곳이 찍힌다. 탭 캡처를 지원하는 크로미움 계열(크롬·엣지 109+)에만
 * 있는 `CaptureController`를 함께 보고, 나머지는 OS 캡처 안내로 돌린다.
 */
function canCaptureTab(): boolean {
  return (
    typeof navigator !== 'undefined' &&
    !!navigator.mediaDevices?.getDisplayMedia &&
    typeof window !== 'undefined' &&
    'CaptureController' in window
  );
}

/**
 * 탭 화면 캡처 스트림의 수명을 관리한다.
 *
 * 첫 `grab`에서 허락을 받아 스트림을 열고, 이후로는 같은 스트림을 다시 쓴다 —
 * 캡처 모드 동안 허락 창이 한 번만 뜨게 하려는 것이다. 사용자가 브라우저의
 * "공유 중지"를 누르면 `idle`로 돌아가고, 다음 `grab`에서 다시 허락을 받는다.
 * 화면이 언마운트되면 스트림도 자동으로 끝난다.
 */
export function useTabCapture(): TabCapture {
  const supported = canCaptureTab();

  const [status, setStatus] = useState<TabCaptureStatus>('idle');
  const streamRef = useRef<MediaStream | null>(null);
  const videoRef = useRef<HTMLVideoElement | null>(null);
  /** 시작 중인 요청. 동시에 들어온 `grab`이 허락 창을 두 번 띄우지 않게 나눠 쓴다. */
  const startingRef = useRef<Promise<HTMLVideoElement> | null>(null);
  /** `stop()`마다 올린다. 시작 도중에 멈췄는지 알아보는 표식이다. */
  const generationRef = useRef(0);

  const stop = useCallback((): void => {
    generationRef.current++;
    streamRef.current?.getTracks().forEach((t) => t.stop());
    streamRef.current = null;
    const video = videoRef.current;
    if (video) {
      video.pause();
      video.srcObject = null;
    }
    videoRef.current = null;
    startingRef.current = null;
    setStatus('idle');
  }, []);

  const ensureStream = useCallback((): Promise<HTMLVideoElement> => {
    if (videoRef.current) return Promise.resolve(videoRef.current);
    if (startingRef.current) return startingRef.current;
    if (!supported) {
      return Promise.reject(new DOMException('화면 캡처를 지원하지 않는 브라우저입니다', 'NotSupportedError'));
    }

    const generation = generationRef.current;
    setStatus('starting');
    const promise = (async () => {
      const stream = await navigator.mediaDevices.getDisplayMedia(DISPLAY_MEDIA_OPTIONS);
      const video = document.createElement('video');
      // 여기서 실패하면 받은 스트림이 아무 데도 남지 않으므로 직접 닫는다(녹화 표시가 남지 않게).
      const discard = () => {
        stream.getTracks().forEach((t) => t.stop());
        video.srcObject = null;
      };
      try {
        // 허락 창이 떠 있는 사이 stop()이 불렸다 — 받은 스트림은 바로 닫는다.
        if (generation !== generationRef.current) {
          throw new DOMException('캡처를 중단했습니다', 'AbortError');
        }
        // 탭이 아닌 창·화면을 골랐다면 좌표 비율이 맞지 않아 엉뚱한 곳이 잘린다.
        // 값을 알려 주지 않는 브라우저는 미리보기에서 사용자가 확인한다.
        const surface = stream.getVideoTracks()[0]?.getSettings().displaySurface;
        if (surface && surface !== 'browser') {
          throw new DOMException('브라우저 탭이 아닌 화면을 골랐습니다', WRONG_SURFACE_ERROR);
        }

        video.muted = true;
        video.playsInline = true;
        video.srcObject = stream;
        await video.play();
        await waitForFrame(video);

        if (generation !== generationRef.current) {
          throw new DOMException('캡처를 중단했습니다', 'AbortError');
        }
      } catch (err) {
        discard();
        throw err;
      }

      // 브라우저의 "공유 중지"로 끝난 경우. 이미 다른 스트림으로 바뀌었다면 건드리지 않는다.
      stream.getVideoTracks()[0]?.addEventListener('ended', () => {
        if (streamRef.current === stream) stop();
      });

      streamRef.current = stream;
      videoRef.current = video;
      return video;
    })();

    startingRef.current = promise;
    promise.then(
      () => {
        if (startingRef.current !== promise) return;
        startingRef.current = null;
        setStatus('live');
      },
      () => {
        if (startingRef.current !== promise) return;
        startingRef.current = null;
        setStatus('idle');
      },
    );
    return promise;
  }, [supported, stop]);

  const grab = useCallback(
    async (viewportRect: Rect, options?: { attribution?: string }): Promise<Blob> => {
      const video = await ensureStream();
      // 화면 쪽에서 툴바를 숨긴 직후라, 그 변화가 담긴 새 프레임을 잠깐 기다린다.
      await waitForFrame(video);

      const videoSize = { width: video.videoWidth, height: video.videoHeight };
      if (videoSize.width === 0 || videoSize.height === 0) {
        throw new Error('캡처 화면을 아직 받지 못했습니다');
      }
      const viewport = { width: window.innerWidth, height: window.innerHeight };
      const crop = cropToVideo(viewportRect, viewport, videoSize);

      const canvas = document.createElement('canvas');
      canvas.width = crop.width;
      canvas.height = crop.height;
      const ctx = canvas.getContext('2d');
      if (!ctx) throw new Error('canvas를 쓸 수 없습니다');
      ctx.drawImage(video, crop.x, crop.y, crop.width, crop.height, 0, 0, crop.width, crop.height);

      if (options?.attribution) {
        // 화면에서 본 크기와 비슷하게 보이도록 비디오 배율(대개 DPR)만큼 키운다.
        drawAttribution(ctx, options.attribution, videoSize.width / viewport.width);
      }

      return new Promise<Blob>((resolve, reject) => {
        canvas.toBlob((blob) => {
          if (blob) resolve(blob);
          else reject(new Error('이미지를 만들지 못했습니다'));
        }, 'image/png');
      });
    },
    [ensureStream],
  );

  useEffect(() => stop, [stop]);

  return useMemo(() => ({ supported, status, grab, stop }), [supported, status, grab, stop]);
}

/**
 * 비디오에 새 프레임이 그려질 때까지 기다린다.
 * `requestVideoFrameCallback`이 없으면 첫 프레임(`loadeddata`)만 기다린다.
 * 어느 쪽이든 {@link FRAME_TIMEOUT_MS}가 지나면 그냥 넘어간다.
 */
function waitForFrame(video: HTMLVideoElement): Promise<void> {
  return new Promise<void>((resolve) => {
    let done = false;
    const finish = () => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      video.removeEventListener('loadeddata', finish);
      resolve();
    };
    const timer = setTimeout(finish, FRAME_TIMEOUT_MS);

    // lib.dom에는 늘 있는 것으로 적혀 있지만 파이어폭스 구버전 등에는 없다.
    const rvfc = (video as Partial<Pick<HTMLVideoElement, 'requestVideoFrameCallback'>>)
      .requestVideoFrameCallback;
    if (rvfc) {
      rvfc.call(video, () => finish());
    } else if (video.readyState >= HTMLMediaElement.HAVE_CURRENT_DATA) {
      finish();
    } else {
      video.addEventListener('loadeddata', finish);
    }
  });
}

/**
 * 출처 문구를 오른쪽 아래에 흰 반투명 알약 위에 그린다.
 * `scale`은 CSS px 대비 canvas 픽셀 배율이다.
 */
function drawAttribution(ctx: CanvasRenderingContext2D, text: string, scale: number): void {
  const s = Number.isFinite(scale) && scale > 0 ? scale : 1;
  const fontSize = 11 * s;
  const padX = 6 * s;
  const height = 18 * s;
  const margin = 6 * s;

  ctx.save();
  ctx.font = `600 ${fontSize}px system-ui, -apple-system, 'Malgun Gothic', sans-serif`;
  const width = ctx.measureText(text).width + padX * 2;
  const x = ctx.canvas.width - width - margin;
  const y = ctx.canvas.height - height - margin;

  ctx.fillStyle = 'rgba(255, 255, 255, 0.85)';
  ctx.beginPath();
  ctx.roundRect(x, y, width, height, height / 2);
  ctx.fill();

  ctx.fillStyle = '#333';
  ctx.textBaseline = 'middle';
  ctx.fillText(text, x + padX, y + height / 2);
  ctx.restore();
}
