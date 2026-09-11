import type { ReactNode } from 'react';
import { useEffect } from 'react';

export interface ModalProps {
  /** 배경/닫기 버튼으로 닫을 때 */
  onClose: () => void;
  /** 좁은 알림용 카드(440px)로 그릴지 */
  compact?: boolean;
  children: ReactNode;
}

/** 화면 전체를 덮는 모달 껍데기. Esc와 배경 클릭으로 닫힌다. */
export function Modal({ onClose, compact, children }: ModalProps) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  return (
    <div className="ro-modal" role="presentation" onClick={onClose}>
      <div
        className={compact ? 'ro-modal__card ro-modal__card--sm' : 'ro-modal__card'}
        role="dialog"
        aria-modal="true"
        onClick={(e) => e.stopPropagation()}
      >
        {children}
      </div>
    </div>
  );
}
