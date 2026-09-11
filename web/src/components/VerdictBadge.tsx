import { VERDICT_HELP, VERDICT_LABEL, type Verdict } from '../types';

/** 판정 배지. 색은 `index.css`의 `.ro-badge--<판정>`이 정한다. */
export function VerdictBadge({ verdict }: { verdict: Verdict }) {
  return (
    <span className={`ro-badge ro-badge--${verdict}`} title={VERDICT_HELP[verdict]}>
      <span className="ro-badge__dot" />
      {VERDICT_LABEL[verdict]}
    </span>
  );
}
