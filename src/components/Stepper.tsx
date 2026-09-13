import { stepperItems } from './steps';
import { Check } from 'lucide-react';
import type { RouteMode, Step } from '../types';

export interface StepperProps {
  /** 현재 단계 */
  current: Step;
  /** 경로 모드 — 자동 모드는 4번 라벨이 다르고 5번(진입·이탈)을 건너뛴다 */
  mode: RouteMode;
  /** 앞선 단계로 되돌아갈 때. 지난 단계에서만 호출된다 */
  onGo: (step: Step) => void;
}

/**
 * 헤더 가운데 스테퍼. 이미 지난 단계로만 되돌아갈 수 있다.
 *
 * 자동 모드는 목록에서 5번이 빠지므로 "지난 단계인가"는 목록 안 위치(인덱스)가 아니라
 * 실제 단계 번호(`item.step < current`)로 판정해야 한다 — 인덱스로 비교하면 5번이 빠진
 * 자리 밀림 때문에 6번(결과)이 지난 단계로 잘못 판정된다.
 */
export function Stepper({ current, mode, onGo }: StepperProps) {
  const items = stepperItems(mode);
  return (
    <nav className="ro-stepper" aria-label="진행 단계">
      {items.map((item, i) => {
        const done = item.step < current;
        const isCurrent = item.step === current;
        return (
          <div className="ro-stepper__item" key={item.step}>
            <button
              type="button"
              className={`ro-step${done ? ' is-done' : ''}${isCurrent ? ' is-current' : ''}`}
              disabled={!done}
              aria-current={isCurrent ? 'step' : undefined}
              onClick={() => onGo(item.step)}
            >
              <span className="ro-step__dot">
                {/* 배지 숫자는 목록 안 순서(1~N)다. 실제 step 값(예: 5번이 빠진 6)을
                    그대로 쓰면 자동 모드에서 "1,2,3,4,6"처럼 건너뛴 것처럼 보인다. */}
                {done ? <Check size={13} strokeWidth={3} /> : i + 1}
              </span>
              <span>{item.label}</span>
            </button>
            {i < items.length - 1 ? <div className="ro-stepper__line" /> : null}
          </div>
        );
      })}
    </nav>
  );
}
