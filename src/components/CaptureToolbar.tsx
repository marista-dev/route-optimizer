import { useEffect, useRef } from 'react';
import { Crop, ImageDown, ScanSearch, X } from 'lucide-react';

export interface CaptureToolbarProps {
  /** 이 브라우저가 탭 캡처를 쓸 수 있는지(`useTabCapture`의 `supported`). 아니면 저장 버튼 대신 OS 캡처 안내 */
  supported: boolean;
  /** 영역 지정 프레임이 켜져 있는지 */
  regionMode: boolean;
  /** `영역 지정` 토글 */
  onToggleRegion: () => void;
  /** `이미지 저장` */
  onSave: () => void;
  /** 캡처·미리보기 준비 중. 저장 버튼을 잠근다 */
  saving: boolean;
  /** `남은 순번 보기` — 아직 안 찍은 마커가 모두 보이게 지도를 맞춘다 */
  onShowRemaining: () => void;
  /** 아직 안 찍은 순번(예: "28~40"). 빈 문자열이면 모두 찍은 것으로 본다 */
  remainingText: string;
  /** 지금 영역에 온전히 들어온 순번(예: "12~27번(16곳)"). 빈 문자열이면 "없음" */
  insideText: string;
  /** 지금까지 찍은 마커 수 */
  capturedCount: number;
  /** 전체 마커 수 */
  totalCount: number;
  /** 캡처 모드 닫기 */
  onClose: () => void;
  /** 실제 캡처 직전에 true. 툴바를 숨긴다(`visibility: hidden`) */
  hidden?: boolean;
}

/**
 * 캡처 모드에서 지도 위 가운데에 뜨는 툴바.
 *
 * 지도 컨테이너 위의 DOM이다(카카오 오버레이가 아니다). Esc로 닫기는 화면 쪽이 처리한다.
 * 상태 문구는 `aria-live`로 읽어 준다 — 지도를 옮길 때마다 "이 영역" 문구가 바뀌므로
 * `polite`로 두어 다른 안내를 끊지 않게 한다.
 */
export function CaptureToolbar({
  supported,
  regionMode,
  onToggleRegion,
  onSave,
  saving,
  onShowRemaining,
  remainingText,
  insideText,
  capturedCount,
  totalCount,
  onClose,
  hidden,
}: CaptureToolbarProps) {
  const allDone = remainingText === '';
  const firstRef = useRef<HTMLButtonElement | null>(null);
  const saveRef = useRef<HTMLButtonElement | null>(null);

  // 포커스가 갈 곳을 잃었을 때(진입하며 패널의 버튼이 사라짐, 캡처 중 숨김·비활성으로 저장 버튼이
  // 포커스를 놓침) 툴바로 되돌린다. 키보드 사용자가 body에서 다시 Tab을 밟지 않게 한다.
  // 미리보기 모달보다 이 effect가 먼저 돌아서, 모달은 닫힐 때 이 버튼으로 포커스를 돌려준다.
  useEffect(() => {
    if (hidden) return;
    const active = document.activeElement;
    if (active && active !== document.body) return;
    (saveRef.current ?? firstRef.current)?.focus({ preventScroll: true });
  }, [hidden]);

  return (
    <div
      className={`ro-capture-toolbar${hidden ? ' is-hidden' : ''}`}
      role="toolbar"
      aria-label="지도 캡처"
      aria-hidden={hidden || undefined}
    >
      <div className="ro-capture-toolbar__actions">
        <button
          ref={firstRef}
          type="button"
          className={`ro-btn ro-btn--sm${regionMode ? ' ro-btn--outline' : ''}`}
          aria-pressed={regionMode}
          title="드래그로 캡처할 영역을 지정합니다"
          onClick={onToggleRegion}
        >
          <Crop size={16} aria-hidden />
          영역 지정
        </button>
        {supported ? (
          <button
            ref={saveRef}
            type="button"
            className="ro-btn ro-btn--sm ro-btn--primary"
            disabled={saving}
            aria-busy={saving || undefined}
            title="지금 영역을 PNG 이미지로 저장합니다"
            onClick={onSave}
          >
            <ImageDown size={16} aria-hidden />
            {saving ? '캡처 중…' : '이미지 저장'}
          </button>
        ) : null}
        <button
          type="button"
          className="ro-btn ro-btn--sm"
          disabled={allDone}
          title="아직 찍지 않은 순번이 모두 보이도록 지도를 맞춥니다"
          onClick={onShowRemaining}
        >
          <ScanSearch size={16} aria-hidden />
          남은 순번 보기
        </button>
      </div>

      <div className="ro-capture-toolbar__status" aria-live="polite">
        <span>{insideText ? `이 영역 ${insideText}` : '이 영역에 온전히 들어온 순번 없음'}</span>
        <span className="ro-capture-toolbar__sep" aria-hidden>
          ·
        </span>
        <span className={allDone ? 'ro-capture-toolbar__done' : undefined}>
          {allDone ? '모든 순번을 찍었습니다' : `남은 순번 ${remainingText}`}
        </span>
        <span className="ro-capture-toolbar__count">
          {capturedCount}/{totalCount}곳 찍음
        </span>
      </div>

      <button
        type="button"
        className="ro-btn ro-btn--sm ro-btn--quiet ro-capture-toolbar__close"
        title="캡처 모드를 닫고 표로 돌아갑니다 (Esc)"
        onClick={onClose}
      >
        <X size={16} aria-hidden />
        닫기
      </button>

      {supported ? null : (
        <p className="ro-capture-toolbar__hint">
          이 브라우저에서는 이미지 저장을 쓸 수 없습니다. 지도를 맞춘 뒤 Win+Shift+S(맥은
          ⌘+Shift+4)로 캡처하세요.
        </p>
      )}
    </div>
  );
}
