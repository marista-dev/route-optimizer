import type { ReactNode, Ref } from 'react';

export interface SidePanelProps {
  /** 패널 제목 */
  title?: ReactNode;
  /** 제목 오른쪽 보조 문구 */
  note?: ReactNode;
  /** 제목 아래 설명 */
  subtitle?: ReactNode;
  /** 헤더 안(설명 아래)에 고정으로 붙는 추가 영역 */
  extraHead?: ReactNode;
  /** 본문 래퍼 클래스(기본은 여백 있는 `ro-panel__body`) */
  bodyClassName?: string;
  /** 본문(스크롤 컨테이너) 참조. 드래그 중 자동 스크롤처럼 요소가 직접 필요할 때 쓴다 */
  bodyRef?: Ref<HTMLDivElement>;
  /** 패널 자체에 붙일 추가 클래스(너비 등) */
  className?: string;
  /** 하단 고정 영역 */
  footer?: ReactNode;
  children?: ReactNode;
}

/** 지도 위에 떠 있는 우측 카드. S3~S6이 공유한다. */
export function SidePanel({
  title,
  note,
  subtitle,
  extraHead,
  bodyClassName = 'ro-panel__body',
  bodyRef,
  className,
  footer,
  children,
}: SidePanelProps) {
  return (
    <aside className={`ro-panel ro-panel--right${className ? ` ${className}` : ''}`}>
      {title ? (
        <div className="ro-panel__head">
          <div className="ro-panel__titlerow">
            <div className="ro-panel__title">{title}</div>
            {note ? <div className="ro-card__note">{note}</div> : null}
          </div>
          {subtitle ? <div className="ro-hint">{subtitle}</div> : null}
          {extraHead}
        </div>
      ) : null}
      <div className={bodyClassName} ref={bodyRef}>
        {children}
      </div>
      {footer ? <div className="ro-panel__foot">{footer}</div> : null}
    </aside>
  );
}
