export interface ProgressTrackProps {
  /** 채움 비율(0~100) */
  pct: number;
  /** 얇은 막대(패널 안 등에서 쓰는 축소형) */
  thin?: boolean;
  /** 채움 색 변형 */
  tone?: 'primary' | 'muted' | 'ok';
  /** 지정하면 track에 `progressbar` 접근성 속성을 붙인다 */
  ariaLabel?: string;
}

/** 진행률 막대만(라벨·카운트 없음). `ProgressBar`와, 패널 안에서 손으로 그리던 진행률 막대가 함께 쓴다. */
export function ProgressTrack({ pct, thin, tone = 'primary', ariaLabel }: ProgressTrackProps) {
  const fillClass =
    tone === 'muted' ? ' ro-progress__fill--muted' : tone === 'ok' ? ' ro-progress__fill--ok' : '';
  const aria = ariaLabel
    ? {
        role: 'progressbar' as const,
        'aria-valuenow': pct,
        'aria-valuemin': 0,
        'aria-valuemax': 100,
        'aria-label': ariaLabel,
      }
    : {};
  return (
    <div
      className={`ro-progress__track${thin ? ' ro-progress__track--thin' : ''}`}
      {...aria}
    >
      <div className={`ro-progress__fill${fillClass}`} style={{ width: `${pct}%` }} />
    </div>
  );
}

export interface ProgressBarProps {
  /** 왼쪽 라벨 */
  label: string;
  /** 완료 수 */
  done: number;
  /** 전체 수. 0이면 0%로 그린다 */
  total: number;
  /** 오른쪽 보조 문구(기본: "done / total") */
  detail?: string;
  /** 채움 색 변형 */
  tone?: 'primary' | 'muted';
}

/** 라벨 + 카운트 + 막대. 2·3단계 진행률에 쓴다. */
export function ProgressBar({ label, done, total, detail, tone = 'primary' }: ProgressBarProps) {
  const pct = total > 0 ? Math.round((done / total) * 100) : 0;
  return (
    <div className="ro-progress">
      <div className="ro-progress__head">
        <span className="ro-progress__label">{label}</span>
        <span className="ro-muted ro-num">{detail ?? `${done} / ${total}`}</span>
      </div>
      <ProgressTrack pct={pct} tone={tone} ariaLabel={label} />
    </div>
  );
}
