/**
 * volatile.ts — localStorage에 못 넣는(또는 넣으면 안 되는) 메모리 전용 상태.
 *
 * - REST 키 자체는 `restKey`(sessionStorage)에만 있고, 여기에는 "있다/없다"만 둔다.
 *   키 문자열은 이 스토어·로그 어디에도 남기지 않는다.
 */

import { create } from 'zustand';

import { restKey, useSessionStore } from './session';

interface VolatileState {
  /** REST 키가 입력되어 있는지 */
  hasKey: boolean;
  /**
   * 저장된 세션을 아직 이어받지 않은 상태.
   * true면 단계와 무관하게 S1을 보여주고 "이어서 하기" 배너를 띄운다.
   */
  resumePending: boolean;
  /** REST 키 저장(sessionStorage) */
  setRestKey: (key: string) => void;
  /** REST 키 삭제 */
  clearRestKey: () => void;
  /** "이어서 하기"/"삭제" 처리 후 호출 */
  resolveResume: () => void;
}

export const useVolatileStore = create<VolatileState>()((set) => ({
  hasKey: restKey.get().length > 0,
  // persist는 동기 하이드레이션이라 모듈 로드 시점에 이미 복원된 단계를 읽을 수 있다.
  resumePending: useSessionStore.getState().step > 1,

  setRestKey: (key) => {
    restKey.set(key);
    set({ hasKey: key.trim().length > 0 });
  },

  clearRestKey: () => {
    restKey.clear();
    set({ hasKey: false });
  },

  resolveResume: () => set({ resumePending: false }),
}));
