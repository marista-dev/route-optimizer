import { useEffect, useRef } from 'react';
import { ArrowRight, ImageDown, Images, Square, X } from 'lucide-react';

import type { AutoProgress, Orientation } from '../capture/types';

export interface CaptureToolbarProps {
  /** 지금 프레임 방향 */
  orientation: Orientation;
  /** `세로 | 가로` 전환 */
  onOrientation: (o: Orientation) => void;
  /** 아직 저장한 장에 들어가지 않은 마커 수 */
  remainingCount: number;
  /** 전체 마커 수 */
  totalCount: number;
  /** `다음 구역` — 남은 마커가 가장 많이 들어가는 자리로 지도를 옮긴다 */
  onNext: () => void;
  /** `이미지 저장` — 지금 프레임을 PNG로 */
  onSave: () => void;
  /** `남은 구역 모두 저장` — 전체 자동 진행 */
  onSaveAll: () => void;
  /** 자동 저장 진행 상황. null이면 진행 중이 아니다 */
  autoProgress: AutoProgress | null;
  /** 자동 저장 `중지` */
  onStopAuto: () => void;
  /** 캡처·미리보기 준비 중. 저장·이동 버튼을 잠근다 */
  saving: boolean;
  /** 이 브라우저가 탭 캡처를 쓸 수 있는지. 아니면 저장 버튼 대신 OS 캡처 안내 */
  supported: boolean;
  /** 지도를 끌거나 확대하는 동안 true. 흐리게 하고 포인터를 통과시킨다 */
  faded: boolean;
  /** 실제 캡처 직전에 true. 독을 숨긴다(`visibility: hidden`) */
  hidden?: boolean;
  /** 캡처 모드 닫기 */
  onClose: () => void;
}

const ORIENTATIONS: { value: Orientation; label: string }[] = [
  { value: 'portrait', label: '세로' },
  { value: 'landscape', label: '가로' },
];

/** 탭 캡처를 못 쓰는 브라우저에서 저장 버튼 대신 보이는 한 줄 안내 */
const UNSUPPORTED_HINT =
  '바로 저장은 PC 크롬·엣지에서 됩니다. Win+Shift+S(맥 ⌘+Shift+4)로 캡처하세요.';

/**
 * 캡처 모드에서 지도 아래 가운데에 뜨는 한 줄짜리 독.
 *
 * 지도 컨테이너 위의 DOM이다(카카오 오버레이가 아니다). Esc로 닫기·중지는 화면 쪽이 처리한다.
 * 높이는 CSS 변수 `--capture-dock-h`로 고정해 두므로, 화면 쪽은 그만큼 프레임 아래를 비운다.
 */
export function CaptureToolbar({
  orientation,
  onOrientation,
  remainingCount,
  totalCount,
  onNext,
  onSave,
  onSaveAll,
  autoProgress,
  onStopAuto,
  saving,
  supported,
  faded,
  hidden,
  onClose,
}: CaptureToolbarProps) {
  const primaryRef = useRef<HTMLButtonElement | null>(null);
  const fallbackRef = useRef<HTMLButtonElement | null>(null);
  const auto = autoProgress !== null;
  const allDone = remainingCount <= 0;
  const busy = saving || auto;

  // 포커스가 갈 곳을 잃었을 때(진입하며 패널의 버튼이 사라짐, 캡처 중 숨김·비활성으로 버튼이
  // 포커스를 놓침, 자동 저장이 끝나 `중지`가 사라짐) 독으로 되돌린다. 키보드 사용자가 body에서
  // 다시 Tab을 밟지 않게 한다. 미리보기 모달보다 이 effect가 먼저 돌아서, 모달은 닫힐 때
  // 이 버튼으로 포커스를 돌려준다.
  useEffect(() => {
    if (hidden) return;
    const active = document.activeElement;
    if (active && active !== document.body) return;
    (primaryRef.current ?? fallbackRef.current)?.focus({ preventScroll: true });
  }, [hidden, auto]);

  const className = [
    'ro-capture-dock',
    faded ? 'is-faded' : '',
    hidden ? 'is-hidden' : '',
  ]
    .filter(Boolean)
    .join(' ');

  return (
    <div
      className={className}
      role="toolbar"
      aria-label="지도 캡처"
      aria-hidden={hidden || undefined}
    >
      <div className="ro-capture-seg" role="group" aria-label="용지 방향">
        {ORIENTATIONS.map((o) => {
          const on = orientation === o.value;
          return (
            <button
              key={o.value}
              type="button"
              aria-pressed={on}
              className={`ro-capture-seg__btn${on ? ' is-on' : ''}`}
              disabled={busy}
              onClick={() => {
                if (!on) onOrientation(o.value);
              }}
            >
              {o.label}
            </button>
          );
        })}
      </div>

      <span
        className={`ro-capture-dock__count${allDone ? ' is-done' : ''}`}
        title={`${totalCount}곳 중 ${totalCount - remainingCount}곳 저장함`}
        aria-live="polite"
      >
        {allDone ? `${totalCount}곳 모두 저장함` : `남은 ${remainingCount}곳`}
      </span>

      {auto ? (
        <div className="ro-capture-dock__actions">
          <span className="ro-capture-dock__progress" aria-live="polite">
            {autoProgress.done}/{autoProgress.total}장 저장 중…
          </span>
          <button
            ref={primaryRef}
            type="button"
            className="ro-btn ro-btn--sm ro-btn--danger-outline"
            title="자동 저장을 멈춥니다 (Esc)"
            onClick={onStopAuto}
          >
            <Square size={14} aria-hidden />
            중지
          </button>
        </div>
      ) : (
        <div className="ro-capture-dock__actions">
          <button
            ref={fallbackRef}
            type="button"
            className="ro-btn ro-btn--sm"
            disabled={allDone || busy}
            title="가장 빠른 남은 순번을 담으면서 남은 곳이 가장 많이 들어가는 자리로 지도를 옮깁니다"
            onClick={onNext}
          >
            다음 구역
            <ArrowRight size={16} aria-hidden />
          </button>
          {supported ? (
            <>
              <button
                ref={primaryRef}
                type="button"
                className="ro-btn ro-btn--sm ro-btn--primary"
                disabled={saving}
                aria-busy={saving || undefined}
                title="지금 프레임을 PNG 이미지로 저장합니다"
                onClick={onSave}
              >
                <ImageDown size={16} aria-hidden />
                {saving ? '캡처 중…' : '이미지 저장'}
              </button>
              <button
                type="button"
                className="ro-btn ro-btn--sm"
                disabled={allDone || saving}
                title="지금 확대 수준으로 남은 곳을 차례로 옮겨 가며 모두 저장합니다"
                onClick={onSaveAll}
              >
                <Images size={16} aria-hidden />
                남은 구역 모두 저장
              </button>
            </>
          ) : (
            <p
              className="ro-capture-dock__hint"
              title={UNSUPPORTED_HINT}
            >
              {UNSUPPORTED_HINT}
            </p>
          )}
        </div>
      )}

      <button
        type="button"
        className="ro-btn ro-btn--sm ro-btn--quiet ro-capture-dock__close"
        title="캡처 모드 닫기 (Esc)"
        onClick={onClose}
      >
        <X size={16} aria-hidden />
        닫기
      </button>
    </div>
  );
}
