import { useId } from 'react';
import type { ReactNode, Ref } from 'react';
import { ChevronDown } from 'lucide-react';

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
  /**
   * 접힌 상태. `onToggleCollapsed`를 함께 줄 때만 쓴다.
   * 접히면 제목 줄(제목·보조 문구·`collapsedSummary`)만 남고 설명·추가 영역·본문·하단은 숨는다.
   */
  collapsed?: boolean;
  /** 주면 제목 줄에 접기/펼치기 버튼이 생긴다. 없으면 접을 수 없는 보통 패널이다 */
  onToggleCollapsed?: () => void;
  /** 접혔을 때 제목 줄 아래에 보이는 한 줄 요약 */
  collapsedSummary?: ReactNode;
  /**
   * 지금은 접을 수 없을 때 그 이유. 주면 버튼이 잠기고(`aria-disabled`) 이 문구가 tooltip이 된다.
   * 펼친 상태에서만 의미가 있다(화면 쪽이 `collapsed`를 false로 넘긴다).
   */
  collapseBlockedReason?: string;
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
  collapsed = false,
  onToggleCollapsed,
  collapsedSummary,
  collapseBlockedReason,
  children,
}: SidePanelProps) {
  const idBase = useId();
  const headRestId = `${idBase}-head`;
  const bodyId = `${idBase}-body`;
  const collapsible = onToggleCollapsed !== undefined;
  const isCollapsed = collapsible && collapsed;
  const blocked = !isCollapsed && collapseBlockedReason !== undefined;

  const classes = [
    'ro-panel',
    'ro-panel--right',
    collapsible ? 'ro-panel--collapsible' : '',
    isCollapsed ? 'is-collapsed' : '',
    className ?? '',
  ]
    .filter(Boolean)
    .join(' ');

  const headExtras = (
    <>
      {subtitle ? <div className="ro-hint">{subtitle}</div> : null}
      {extraHead}
    </>
  );
  const body = (
    <>
      <div className={bodyClassName} ref={bodyRef}>
        {children}
      </div>
      {footer ? <div className="ro-panel__foot">{footer}</div> : null}
    </>
  );

  if (!collapsible) {
    return (
      <aside className={classes}>
        {title ? (
          <div className="ro-panel__head">
            <div className="ro-panel__titlerow">
              <div className="ro-panel__title">{title}</div>
              {note ? <div className="ro-card__note">{note}</div> : null}
            </div>
            {headExtras}
          </div>
        ) : null}
        {body}
      </aside>
    );
  }

  // 접을 수 있는 패널. 제목 줄은 늘 보이고, 헤더 추가 영역과 본문·하단은 `hidden`으로 숨긴다
  // (DOM은 그대로라 표 스크롤 위치·참조가 유지된다). 두 감싸개는 펼쳐 있을 때 `display: contents`라
  // 접을 수 없는 패널과 배치가 똑같다.
  return (
    <aside className={classes}>
      <div className="ro-panel__head">
        <div className="ro-panel__titlerow">
          <div className="ro-panel__title">{title}</div>
          {note ? <div className="ro-card__note">{note}</div> : null}
          <button
            type="button"
            className="ro-panel__toggle"
            aria-expanded={!isCollapsed}
            aria-controls={`${headRestId} ${bodyId}`}
            aria-disabled={blocked || undefined}
            title={blocked ? collapseBlockedReason : isCollapsed ? '패널 펼치기' : '패널 접기'}
            onClick={blocked ? undefined : onToggleCollapsed}
          >
            {/* 아이콘만 두면 눈에 잘 안 띄어 글자를 함께 둔다. 글자가 곧 접근 가능한 이름이다. */}
            {isCollapsed ? '펼치기' : '접기'}
            <ChevronDown size={18} aria-hidden />
          </button>
        </div>
        {isCollapsed && collapsedSummary ? (
          <div className="ro-panel__summary">{collapsedSummary}</div>
        ) : null}
        <div className="ro-panel__reveal" id={headRestId} hidden={isCollapsed}>
          {headExtras}
        </div>
      </div>
      <div className="ro-panel__reveal" id={bodyId} hidden={isCollapsed}>
        {body}
      </div>
    </aside>
  );
}
