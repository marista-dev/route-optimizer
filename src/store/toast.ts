/** 화면 하단 토스트 한 줄. 오류 안내에 쓴다. */
import { create } from 'zustand';

export type ToastKind = 'error' | 'info';

interface ToastState {
  message: string | null;
  kind: ToastKind;
  /** 토스트를 띄운다. 같은 메시지를 다시 부르면 그대로 유지된다 */
  show: (message: string, kind?: ToastKind) => void;
  /** 토스트를 닫는다 */
  clear: () => void;
}

export const useToastStore = create<ToastState>()((set) => ({
  message: null,
  kind: 'error',
  show: (message, kind = 'error') => set({ message, kind }),
  clear: () => set({ message: null }),
}));

/** 렌더 밖(이벤트 핸들러·비동기 함수)에서 바로 부르는 단축 함수. */
export function showError(message: string): void {
  useToastStore.getState().show(message, 'error');
}

/** 오류가 아닌 안내(중단·저장 완료 등). */
export function showInfo(message: string): void {
  useToastStore.getState().show(message, 'info');
}
