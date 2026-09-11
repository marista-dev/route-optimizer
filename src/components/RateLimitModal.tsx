import { useState } from 'react';

import { KeyInput } from './KeyInput';
import { Modal } from './Modal';

export interface RateLimitModalProps {
  /** 진행 상황 한 줄(예: "클러스터 3 / 12") */
  progress: string;
  /** 한도에 걸리기 전까지 받아 둔 쌍 수(재시도 때 건너뛴다) */
  savedPairs: number;
  /** 이 클러스터에 필요한 전체 쌍 수 */
  totalPairs: number;
  /** 새 키로 다시 시도 */
  onRetry: (newKey: string) => void;
  /** 남은 칸을 Haversine 추정치로 채우고 계속 */
  onFallback: () => void;
  /** 명시적 닫기. 닫은 뒤에도 같은 선택지를 다시 열 수 있어야 한다 */
  onClose: () => void;
}

/** 일일 한도(rate limit) 감지 시 뜨는 키 재입력 모달. */
export function RateLimitModal({
  progress,
  savedPairs,
  totalPairs,
  onRetry,
  onFallback,
  onClose,
}: RateLimitModalProps) {
  const [key, setKey] = useState('');

  /*
   * Esc·배경 클릭으로 닫지 않는다(`sticky`). 이 모달을 실수로 닫으면 세 선택지를
   * 다시 띄울 방법이 한도에 또 걸리는 것뿐이고, 그 재시도는 쌍당 최대 90초가 걸린다.
   * 대신 아래에 명시적 닫기를 두었다 — 닫아도 패널 배너에서 다시 열 수 있다.
   */
  return (
    <Modal onClose={onClose} compact sticky>
      <div className="ro-alert">
        <div className="ro-alert__icon">!</div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          <div className="ro-alert__title">API 호출 한도에 도달했습니다</div>
          <div className="ro-alert__body">
            카카오 모빌리티 길찾기 요청이 거듭 거부되었습니다(429). 일일 한도가 소진되었거나 잠시
            제한 중입니다. 다른 REST 키를 입력하거나 잠시 후 다시 시도하세요. 이미 받아 둔 쌍은
            다시 호출하지 않습니다.
          </div>
        </div>
      </div>
      <div className="ro-note">
        <span>
          {progress} · {savedPairs}/{totalPairs} 쌍 저장됨
        </span>
        <span>실패 셀은 Haversine으로 대체 가능</span>
      </div>
      <div className="ro-hint">
        키를 바꾸지 않고 그냥 다시 호출하면 쌍당 최대 90초를 기다린 뒤 실패합니다.
      </div>
      <div className="ro-field">
        <span className="ro-label">새 REST API 키</span>
        <KeyInput value={key} onChange={setKey} autoFocus />
      </div>
      <div className="ro-row" style={{ justifyContent: 'space-between' }}>
        <button type="button" className="ro-btn ro-btn--quiet" onClick={onClose}>
          닫기
        </button>
        <button type="button" className="ro-btn ro-btn--quiet" onClick={onFallback}>
          Haversine으로 계속
        </button>
        <button
          type="button"
          className="ro-btn ro-btn--primary"
          disabled={key.trim().length === 0}
          onClick={() => onRetry(key.trim())}
        >
          키 교체 후 재시도
        </button>
      </div>
    </Modal>
  );
}
