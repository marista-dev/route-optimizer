import type { ReactNode } from 'react';
import { useEffect, useRef } from 'react';

export interface ModalProps {
  /** 배경/닫기 버튼으로 닫을 때 */
  onClose: () => void;
  /** 좁은 알림용 카드(440px)로 그릴지 */
  compact?: boolean;
  /**
   * Esc·배경 클릭으로 닫지 않는다. 선택을 반드시 받아야 하는 모달에 쓴다 —
   * 실수로 배경을 눌러 닫으면 그 선택지를 다시 띄울 방법이 없는 경우.
   */
  sticky?: boolean;
  children: ReactNode;
}

/** 탭 순서에 들어가는 요소들. */
const FOCUSABLE =
  'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';

/**
 * 화면 전체를 덮는 모달 껍데기.
 *
 * 열리면 포커스를 카드 안으로 옮기고 Tab을 가둔다. 닫히면 열기 전 요소로 되돌린다 —
 * 그렇게 하지 않으면 키보드 사용자가 모달 뒤 페이지를 먼저 훑게 되고, 닫은 뒤에도
 * 표 첫머리부터 다시 Tab을 밟아야 한다(주소 수정처럼 여러 건을 연달아 고치는 화면에서 비용이 크다).
 */
export function Modal({ onClose, compact, sticky, children }: ModalProps) {
  const cardRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const opener = document.activeElement as HTMLElement | null;
    const card = cardRef.current;
    const firstFocusable = card?.querySelector<HTMLElement>(FOCUSABLE);
    if (firstFocusable) firstFocusable.focus();
    else card?.focus();

    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !sticky) {
        onClose();
        return;
      }
      if (e.key !== 'Tab' || !card) return;
      const items = Array.from(card.querySelectorAll<HTMLElement>(FOCUSABLE));
      if (items.length === 0) return;
      const first = items[0];
      const last = items[items.length - 1];
      const active = document.activeElement;
      // 카드 밖으로 나가려는 Tab만 되돌린다. 안에서의 이동은 브라우저에 맡긴다.
      if (!e.shiftKey && (active === last || !card.contains(active))) {
        e.preventDefault();
        first.focus();
      } else if (e.shiftKey && (active === first || !card.contains(active))) {
        e.preventDefault();
        last.focus();
      }
    };

    window.addEventListener('keydown', onKey);
    return () => {
      window.removeEventListener('keydown', onKey);
      opener?.focus?.();
    };
  }, [onClose, sticky]);

  return (
    <div
      className="ro-modal"
      role="presentation"
      onClick={sticky ? undefined : onClose}
    >
      <div
        ref={cardRef}
        className={compact ? 'ro-modal__card ro-modal__card--sm' : 'ro-modal__card'}
        role="dialog"
        aria-modal="true"
        tabIndex={-1}
        onClick={(e) => e.stopPropagation()}
      >
        {children}
      </div>
    </div>
  );
}
