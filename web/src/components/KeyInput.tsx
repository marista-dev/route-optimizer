import { useState } from 'react';

export interface KeyInputProps {
  /** 현재 입력값 */
  value: string;
  /** 값이 바뀔 때. 키 문자열은 절대 로그에 남기지 않는다 */
  onChange: (value: string) => void;
  /** 자동 포커스 */
  autoFocus?: boolean;
}

/** 입력 칸 placeholder. */
const PLACEHOLDER = '32자리 REST 키';

/** REST 키 입력 칸 + 보기/숨기기 토글. */
export function KeyInput({ value, onChange, autoFocus }: KeyInputProps) {
  const [visible, setVisible] = useState(false);

  return (
    <div className="ro-row">
      <input
        className="ro-input ro-mono"
        style={{ flex: 1 }}
        type={visible ? 'text' : 'password'}
        value={value}
        placeholder={PLACEHOLDER}
        spellCheck={false}
        autoComplete="off"
        autoFocus={autoFocus}
        aria-label="카카오 REST API 키"
        onChange={(e) => onChange(e.target.value)}
      />
      <button
        type="button"
        className="ro-btn ro-btn--soft"
        style={{ fontSize: 12 }}
        onClick={() => setVisible((v) => !v)}
      >
        {visible ? '숨기기' : '보기'}
      </button>
    </div>
  );
}
