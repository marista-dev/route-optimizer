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
  onRowHover?: (row: T | null) => void;
  /**
   * 행 클릭. 드래그 재정렬 중(`reorder`가 있을 때)에는 부르지 않는다 — 편집 중
   * 드래그가 3px을 못 넘겨 실패하면 click이 발생하는데, 그때 지도까지 튀면 안 된다.
   */
  onRowClick?: (row: T, index: number) => void;
  /**
   * 행 드래그 재정렬. `useDragReorder`가 돌려주는 값을 그대로 넘긴다 — 다섯 개
   * prop을 따로 받으면 "재정렬은 켰는데 핸들러가 없다" 같은 반쪽짜리 조합이 타입을
   * 통과한다. 이 값이 있을 때만 행에 `[data-reorder-item]`이 붙는다.
   */
  reorder?: {
    dragFrom: number | null;
    /** 삽입선을 그릴 자리. `useDragReorder.dropLine`(제자리는 이미 걸러져 있다) */
    dropLine: number | null;
    onItemDragStart: (index: number) => void;
    onDragEnd: () => void;
  } | null;
  /**
   * 이 key의 행을 보이는 곳으로 스크롤한다(지도에서 고른 지점을 표에서 찾아 줄 때).
   * 값이 바뀔 때만 동작하므로 같은 행을 다시 고르려면 호출부가 새 객체를 넣어야 한다.
   */
  scrollTo?: { key: string | number } | null;
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
  reorder = null,
  scrollTo = null,
}: DataTableProps<T>) {
  const bodyRef = useRef<HTMLTableSectionElement | null>(null);
  const reorderable = reorder !== null;
  const dragFrom = reorder?.dragFrom ?? null;
  const dropLine = reorder?.dropLine ?? null;

  useEffect(() => {
    if (scrollTo == null) return;
    const body = bodyRef.current;
    if (!body) return;
    const row = body.querySelector<HTMLElement>(
      `[data-row-key="${CSS.escape(String(scrollTo.key))}"]`,
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
  }, [scrollTo]);

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
              reorderable && dropLine === i ? 'is-dropbefore' : null,
              reorderable && dropLine === rows.length && i === rows.length - 1
                ? 'is-dropafter'
                : null,
            ]
              .filter(Boolean)
              .join(' ') || undefined}
            onDragStart={reorder ? () => reorder.onItemDragStart(i) : undefined}
            onDragEnd={reorder ? reorder.onDragEnd : undefined}
            onMouseEnter={onRowHover ? () => onRowHover(row) : undefined}
            onMouseLeave={onRowHover ? () => onRowHover(null) : undefined}
            // 재정렬 중에는 행 클릭을 끈다 — 드래그가 3px을 못 넘겨 click으로 판정되면
            // (편집 중 흔하다) 지도가 그 지점으로 튀는 일이 없게 한다.
            onClick={onRowClick && !reorderable ? () => onRowClick(row, i) : undefined}
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
