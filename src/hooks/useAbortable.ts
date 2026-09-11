import { useCallback, useEffect, useRef } from 'react';

/**
 * 오래 걸리는 작업 하나를 중단할 수 있게 감싼다.
 * `start()`는 직전 작업을 취소하고 새 `AbortSignal`을 준다.
 * 화면이 언마운트되면 진행 중이던 작업도 자동으로 중단된다.
 */
export function useAbortable() {
  const ref = useRef<AbortController | null>(null);

  const start = useCallback((): AbortSignal => {
    ref.current?.abort();
    const controller = new AbortController();
    ref.current = controller;
    return controller.signal;
  }, []);

  const abort = useCallback((): void => {
    ref.current?.abort();
    ref.current = null;
  }, []);

  useEffect(() => () => ref.current?.abort(), []);

  return { start, abort };
}
