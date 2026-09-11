import { useEffect, useRef, useState } from 'react';
import { X } from 'lucide-react';

import { Modal } from './Modal';
import { buildPostcodeAddress } from '../screens/helpers';
import type { PostcodeData } from '../screens/helpers';

/** 카카오(다음) 우편번호 서비스 번들. 페이지당 한 번만 싣는다. */
const POSTCODE_SRC = 'https://t1.daumcdn.net/mapjsapi/bundle/postcode/prod/postcode.v2.js';

let loading: Promise<void> | null = null;

/** 우편번호 스크립트를 한 번만 로드한다. */
function loadPostcodeScript(): Promise<void> {
  if (window.daum?.Postcode) return Promise.resolve();
  if (loading) return loading;
  loading = new Promise<void>((resolve, reject) => {
    const script = document.createElement('script');
    script.src = POSTCODE_SRC;
    script.async = true;
    script.onload = () => resolve();
    script.onerror = () => {
      loading = null;
      reject(new Error('우편번호 서비스를 불러오지 못했습니다.'));
    };
    document.head.appendChild(script);
  });
  return loading;
}

export interface PostcodeModalProps {
  /** 모달 제목 */
  title: string;
  /** 제목 아래 설명 */
  subtitle?: string;
  /** 주소를 고르면 한 줄 주소 문자열로 넘어온다 */
  onSelect: (address: string) => void;
  /** 닫기 */
  onClose: () => void;
}

/** 우편번호 검색을 모달 안에 embed한다(데스크톱판의 팝업 창 대체). */
export function PostcodeModal({ title, subtitle, onSelect, onClose }: PostcodeModalProps) {
  const hostRef = useRef<HTMLDivElement>(null);
  const [error, setError] = useState<string | null>(null);
  // 콜백이 바뀌어도 embed를 다시 하지 않도록 최신 값만 ref로 들고 있는다.
  const latest = useRef(onSelect);
  useEffect(() => {
    latest.current = onSelect;
  }, [onSelect]);

  useEffect(() => {
    let cancelled = false;
    const host = hostRef.current;
    loadPostcodeScript()
      .then(() => {
        if (cancelled || !host) return;
        // StrictMode 이중 마운트에서 iframe이 두 개 생기지 않도록 비우고 시작한다.
        host.innerHTML = '';
        new window.daum.Postcode({
          oncomplete: (data: PostcodeData) => latest.current(buildPostcodeAddress(data)),
          width: '100%',
          height: '100%',
        }).embed(host);
      })
      .catch((err: Error) => {
        if (!cancelled) setError(err.message);
      });
    return () => {
      cancelled = true;
      if (host) host.innerHTML = '';
    };
  }, []);

  return (
    <Modal onClose={onClose}>
      <div className="ro-modal__head">
        <div>
          <div className="ro-modal__title">{title}</div>
          {subtitle ? <div className="ro-modal__sub">{subtitle}</div> : null}
        </div>
        <button type="button" className="ro-modal__x" aria-label="닫기" onClick={onClose}>
          <X size={18} />
        </button>
      </div>
      {/* 오류 문구는 host 바깥 형제 노드에 둔다. host는 우편번호 iframe이 직접 쓰는 자리라
          React가 함께 렌더하면 innerHTML 비우기와 충돌한다. */}
      {error ? <p style={{ padding: 16, color: 'var(--danger)' }}>{error}</p> : null}
      <div className="ro-postcode" ref={hostRef} />
      <div className="ro-modal__foot">
        <button type="button" className="ro-btn ro-btn--sm" onClick={onClose}>
          닫기
        </button>
      </div>
    </Modal>
  );
}
