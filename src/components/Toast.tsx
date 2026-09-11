import { useEffect } from 'react';
import { X } from 'lucide-react';

import { useToastStore } from '../store/toast';

/** 화면 하단 한 줄 알림. 안내는 6초 뒤 사라지고, 오류는 닫을 때까지 남는다. */
export function Toast() {
  const message = useToastStore((s) => s.message);
  const kind = useToastStore((s) => s.kind);
  const clear = useToastStore((s) => s.clear);

  useEffect(() => {
    // 안내는 흘려보내도 되지만 오류는 사용자가 다음 수를 정해야 하는 내용이다.
    // 자리를 비운 사이 사라지면 왜 실패했는지 알 방법이 없어, 오류는 직접 닫을 때까지 남긴다.
    if (!message || kind === 'error') return;
    const id = setTimeout(clear, 6000);
    return () => clearTimeout(id);
  }, [message, kind, clear]);

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
