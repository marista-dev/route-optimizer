import type { Verdict } from '../types';

/** 판정 배지. 색은 `index.css`의 `.ro-badge--<판정>`이 정한다. */
export function VerdictBadge({ verdict }: { verdict: Verdict }) {
  return (
    <span className={`ro-badge ro-badge--${verdict}`}>
      <span className="ro-badge__dot" />
      {verdict}
    </span>
  );
}
