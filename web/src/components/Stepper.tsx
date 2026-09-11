import { STEP_LABELS } from './steps';
import type { Step } from '../types';

export interface StepperProps {
  /** 현재 단계 */
  current: Step;
  /** 앞선 단계로 되돌아갈 때. 지난 단계에서만 호출된다 */
  onGo: (step: Step) => void;
}

/** 헤더 가운데 스테퍼. 이미 지난 단계로만 되돌아갈 수 있다. */
export function Stepper({ current, onGo }: StepperProps) {
  return (
    <nav className="ro-stepper" aria-label="진행 단계">
      {STEP_LABELS.map((label, i) => {
        const n = (i + 1) as Step;
        const done = n < current;
        const isCurrent = n === current;
        return (
          <div className="ro-stepper__item" key={label}>
            <button
              type="button"
              className={`ro-step${done ? ' is-done' : ''}${isCurrent ? ' is-current' : ''}`}
              disabled={!done}
              aria-current={isCurrent ? 'step' : undefined}
              onClick={() => onGo(n)}
            >
              <span className="ro-step__dot">{done ? '✓' : n}</span>
              <span>{label}</span>
            </button>
            {n < 6 ? <div className="ro-stepper__line" /> : null}
          </div>
        );
      })}
    </nav>
  );
}
