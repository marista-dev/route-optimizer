import { useEffect, useRef } from 'react';
import type { ReactNode } from 'react';

/** 스크롤 위치를 맞출 때 행 위에 남기는 여백(px). */
const SCROLL_MARGIN_PX = 8;

/** 실제로 스크롤되는 조상 요소를 찾는다. 없으면 null. */
function findScrollParent(el: HTMLElement): HTMLElement | null {
  let node: HTMLElement | null = el.parentElement;
  while (node) {
    const overflowY = getComputedStyle(node).overflowY;
    if ((overflowY === 'auto' || overflowY === 'scroll') && node.scrollHeight > node.clientHeight) {
      return node;
    }
    node = node.parentElement;
  }
  return null;
}

export interface Column<T> {
  /** React key 겸 식별자 */
  key: string;
  /** 헤더 셀 내용 */
  header: ReactNode;
  /** 열 고정 너비(px) */
  width?: number;
  /** 오른쪽 정렬 */
  right?: boolean;
  /** 셀 렌더러 */
  cell: (row: T, index: number) => ReactNode;
  /** 셀에 붙일 클래스 */
  className?: string;
}

export interface DataTableProps<T> {
  columns: Column<T>[];
  rows: readonly T[];
  /** 행 key */
  rowKey: (row: T, index: number) => string | number;
  /** 행에 붙일 클래스(판정별 배경 등) */
  rowClassName?: (row: T, index: number) => string | undefined;
  /** 행이 없을 때 보여줄 내용 */
  empty?: ReactNode;
  /** 마지막 행 뒤에 덧붙일 안내(진행 중 표시 등) */
  trailing?: ReactNode;
  /** 행에 마우스가 올라가거나(row) 벗어날 때(null). S6 결과 표 ↔ 지도 연동 */
  onRowHover?: (row: T | null, index: number) => void;
  /** 행 클릭 */
  onRowClick?: (row: T, index: number) => void;
  /**
   * 행 드래그 재정렬을 켠다. `useDragReorder`와 함께 쓴다 —
   * 훅이 `[data-reorder-item]`을 찾으므로 이 값이 참일 때만 그 속성이 붙는다.
   */
  reorderable?: boolean;
  /** 드래그 시작. `useDragReorder.onItemDragStart` 연결 */
  onRowDragStart?: (index: number) => void;
  /** 드래그 종료. `useDragReorder.onDragEnd` 연결 */
  onRowDragEnd?: () => void;
  /** 끌고 있는 행(흐리게 처리) */
  dragFrom?: number | null;
  /** 삽입될 위치(0..rows.length). 그 자리에 선을 긋는다 */
  dropAt?: number | null;
  /**
   * 이 key의 행을 보이는 곳으로 스크롤한다(지도에서 고른 지점을 표에서 찾아 줄 때).
   * 값이 바뀔 때만 동작하므로 같은 행을 다시 고르려면 호출부가 값을 바꿔 줘야 한다.
   */
  scrollToKey?: string | number | null;
}

/** 스티키 헤더 표. S2 검증 표와 S6 결과 표가 함께 쓴다. */
export function DataTable<T>({
  columns,
  rows,
  rowKey,
  rowClassName,
  empty,
  trailing,
  onRowHover,
  onRowClick,
  reorderable = false,
  onRowDragStart,
  onRowDragEnd,
  dragFrom = null,
  dropAt = null,
  scrollToKey = null,
}: DataTableProps<T>) {
  const bodyRef = useRef<HTMLTableSectionElement | null>(null);

  useEffect(() => {
    if (scrollToKey == null) return;
    const body = bodyRef.current;
    if (!body) return;
    const row = body.querySelector<HTMLElement>(
      `[data-row-key="${CSS.escape(String(scrollToKey))}"]`,
    );
    if (!row) return;

    // 목표는 "찾은 행이 눈에 확실히 들어오는 것"이다.
    // scrollIntoView({block:'nearest'})는 행이 가장자리에 걸쳐 있어도 움직이지 않고,
    // 'start'는 스티키 헤더 밑으로 숨는다. 그래서 헤더 높이를 빼고 직접 올린다.
    const scroller = findScrollParent(row);
    if (!scroller) {
      row.scrollIntoView({ block: 'center', behavior: 'smooth' });
      return;
    }
    const headHeight = body.previousElementSibling?.getBoundingClientRect().height ?? 0;
    const top =
      scroller.scrollTop +
      (row.getBoundingClientRect().top - scroller.getBoundingClientRect().top) -
      headHeight -
      SCROLL_MARGIN_PX;
    scroller.scrollTo({ top: Math.max(0, top), behavior: 'smooth' });
  }, [scrollToKey]);

  return (
    <table className="ro-table">
      <thead>
        <tr>
          {columns.map((col) => (
            <th
              key={col.key}
              style={col.width ? { width: col.width } : undefined}
              className={col.right ? 'ro-td--right' : undefined}
            >
              {col.header}
            </th>
          ))}
        </tr>
      </thead>
      <tbody ref={bodyRef}>
        {rows.map((row, i) => (
          <tr
            key={rowKey(row, i)}
            data-row-key={rowKey(row, i)}
            data-reorder-item={reorderable ? '' : undefined}
            draggable={reorderable || undefined}
            className={[
              rowClassName?.(row, i),
              reorderable ? 'is-reorderable' : null,
              reorderable && dragFrom === i ? 'is-drag' : null,
              // 삽입선은 그 자리 앞뒤 행에 그린다. tr은 margin이 먹지 않아 inset 그림자를 쓴다.
              reorderable && dragFrom !== null && dropAt === i ? 'is-dropbefore' : null,
              reorderable && dragFrom !== null && dropAt === rows.length && i === rows.length - 1
                ? 'is-dropafter'
                : null,
            ]
              .filter(Boolean)
              .join(' ') || undefined}
            onDragStart={reorderable && onRowDragStart ? () => onRowDragStart(i) : undefined}
            onDragEnd={reorderable ? onRowDragEnd : undefined}
            onMouseEnter={onRowHover ? () => onRowHover(row, i) : undefined}
            onMouseLeave={onRowHover ? () => onRowHover(null, i) : undefined}
            onClick={onRowClick ? () => onRowClick(row, i) : undefined}
          >
            {columns.map((col) => (
              <td
                key={col.key}
                className={[col.className, col.right ? 'ro-td--right' : null]
                  .filter(Boolean)
                  .join(' ') || undefined}
              >
                {col.cell(row, i)}
              </td>
            ))}
          </tr>
        ))}
        {rows.length === 0 && empty ? (
          <tr>
            <td colSpan={columns.length} className="ro-table__empty">
              {empty}
            </td>
          </tr>
        ) : null}
        {trailing ? (
          <tr>
            <td colSpan={columns.length} className="ro-table__pending">
              {trailing}
            </td>
          </tr>
        ) : null}
      </tbody>
    </table>
  );
}
