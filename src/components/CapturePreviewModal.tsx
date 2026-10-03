import { useEffect, useRef } from 'react';
import { X } from 'lucide-react';

import { Modal } from './Modal';

export interface CapturePreviewModalProps {
  /** 잘라 낸 PNG */
  blob: Blob;
  /** 저장될 파일 이름(예: `명단_배송지도_1.png`) */
  fileName: string;
  /** `저장` — 실제 다운로드는 호출부가 한다 */
  onSave: () => void;
  /** `다시 찍기` — 이 이미지를 버리고 캡처 모드로 돌아간다 */
  onRetake: () => void;
  /** 배경·Esc·닫기 버튼 */
  onClose: () => void;
}

/**
 * 캡처 결과 미리보기.
 *
 * 공유 창에서 다른 탭이나 창을 골랐다면 여기서 바로 알아챌 수 있다. 그래서 저장 전에
 * 반드시 한 번 보여 준다. object URL은 이 모달의 수명과 같게 만들고 해제한다.
 */
export function CapturePreviewModal({
  blob,
  fileName,
  onSave,
  onRetake,
  onClose,
}: CapturePreviewModalProps) {
  const imgRef = useRef<HTMLImageElement | null>(null);

  // URL 생성은 외부 자원 할당이라 effect에서 하고, 정리할 때 반드시 revoke한다
  // (렌더 중 만들면 StrictMode 이중 렌더에서 해제되지 않는 URL이 남는다).
  // state로 돌리면 렌더가 한 번 더 도므로 img에 직접 꽂는다.
  useEffect(() => {
    const url = URL.createObjectURL(blob);
    const img = imgRef.current;
    if (img) img.src = url;
    return () => {
      if (img) img.removeAttribute('src');
      URL.revokeObjectURL(url);
    };
  }, [blob]);

  return (
    <Modal onClose={onClose}>
      <div className="ro-modal__head">
        <div>
          <div className="ro-modal__title">캡처 미리보기</div>
          <div className="ro-modal__sub ro-capture-preview__name">{fileName}</div>
        </div>
        <button type="button" className="ro-modal__x" aria-label="닫기" onClick={onClose}>
          <X size={18} />
        </button>
      </div>
      <div className="ro-capture-preview">
        <img ref={imgRef} className="ro-capture-preview__img" alt="캡처한 지도 영역 미리보기" />
      </div>
      <div className="ro-modal__foot">
        <span className="ro-capture-preview__hint">지도가 아닌 화면이 찍혔다면 다시 찍으세요.</span>
        <button type="button" className="ro-btn ro-btn--sm" onClick={onRetake}>
          다시 찍기
        </button>
        <button type="button" className="ro-btn ro-btn--sm ro-btn--primary" onClick={onSave}>
          저장
        </button>
      </div>
    </Modal>
  );
}
