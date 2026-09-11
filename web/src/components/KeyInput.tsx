import { useCallback, useEffect, useRef, useState } from 'react';

import { authHeaders, probeRestKey } from '../api';

export interface KeyInputProps {
  /** 현재 입력값 */
  value: string;
  /** 값이 바뀔 때. 키 문자열은 절대 로그에 남기지 않는다 */
  onChange: (value: string) => void;
  /** 자동 포커스 */
  autoFocus?: boolean;
}

/** 입력 칸 placeholder. */
const PLACEHOLDER = '32자리 REST API Key';

/** 카카오 REST 키 길이. 이 길이가 되면 자동으로 확인한다. */
const KEY_LENGTH = 32;

/** 자동 확인 전 대기(ms). 붙여넣기 직후 연타를 막는다. */
const DEBOUNCE_MS = 400;

type Status = 'idle' | 'checking' | 'ok' | 'invalid' | 'unknown';

const MESSAGE: Record<Exclude<Status, 'idle'>, string> = {
  checking: '확인 중…',
  ok: '유효한 키입니다',
  invalid: '카카오가 거부한 키입니다',
  unknown: '확인하지 못했습니다 (네트워크)',
};

/**
 * REST 키 입력 칸 + 유효성 확인.
 *
 * 눈으로 대조하는 '보기' 토글 대신 카카오 로컬 API에 실제로 한 번 요청해
 * 상태 코드로 판정한다. 오타는 눈으로 잘 걸러지지 않고, 잘못된 키는 지오코딩을
 * 시작한 뒤에야 드러나기 때문이다. 32자를 채우면 자동으로, 그 밖에는 버튼으로 확인한다.
 */
export function KeyInput({ value, onChange, autoFocus }: KeyInputProps) {
  const [status, setStatus] = useState<Status>('idle');
  // 확인이 끝난 값. 같은 값을 다시 확인해 쿼터를 낭비하지 않는다.
  const checkedRef = useRef<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const key = value.trim();

  const verify = useCallback((candidate: string) => {
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    setStatus('checking');
    void probeRestKey(authHeaders(candidate), controller.signal).then((result) => {
      if (controller.signal.aborted) return;
      checkedRef.current = candidate;
      setStatus(result);
    });
  }, []);

  // 32자가 채워지면 잠깐 기다렸다가 스스로 확인한다.
  useEffect(() => {
    if (key.length !== KEY_LENGTH || key === checkedRef.current) return;
    const timer = setTimeout(() => verify(key), DEBOUNCE_MS);
    return () => clearTimeout(timer);
  }, [key, verify]);

  // 값이 바뀌면 이전 판정은 더 이상 이 값의 것이 아니다.
  useEffect(() => {
    if (key !== checkedRef.current) {
      abortRef.current?.abort();
      setStatus('idle');
    }
  }, [key]);

  useEffect(() => () => abortRef.current?.abort(), []);

  return (
    <div>
      <div className="ro-row">
        <input
          className="ro-input ro-mono"
          style={{ flex: 1 }}
          type="password"
          value={value}
          placeholder={PLACEHOLDER}
          spellCheck={false}
          autoComplete="off"
          autoFocus={autoFocus}
          aria-label="카카오 REST API Key"
          onChange={(e) => onChange(e.target.value)}
        />
        <button
          type="button"
          className="ro-btn ro-btn--soft"
          disabled={key.length === 0 || status === 'checking'}
          onClick={() => verify(key)}
        >
          확인
        </button>
      </div>
      {status === 'idle' ? null : (
        <p className={`ro-keystate is-${status}`} role="status">
          {MESSAGE[status]}
        </p>
      )}
    </div>
  );
}
