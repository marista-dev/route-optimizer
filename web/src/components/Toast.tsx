import { useEffect } from 'react';
import { X } from 'lucide-react';

import { useToastStore } from '../store/toast';

/** 화면 하단 한 줄 알림. 6초 뒤 자동으로 사라진다. */
export function Toast() {
  const message = useToastStore((s) => s.message);
  const kind = useToastStore((s) => s.kind);
  const clear = useToastStore((s) => s.clear);

  useEffect(() => {
    if (!message) return;
    const id = setTimeout(clear, 6000);
    return () => clearTimeout(id);
  }, [message, clear]);

  if (!message) return null;
  return (
    <div className={`ro-toast${kind === 'error' ? ' is-error' : ''}`} role="alert">
      <span>{message}</span>
      <button type="button" className="ro-toast__x" aria-label="닫기" onClick={clear}>
        <X size={16} />
      </button>
    </div>
  );
}
