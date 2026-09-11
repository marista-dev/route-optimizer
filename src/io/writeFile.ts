/**
 * 결과 파일 생성.
 *
 * 산출물은 서식을 입힌 xlsx 하나다. 배송 기사가 들고 다니며 보는 종이라
 * 순서·연번·이름·주소 네 열만 담는다. 좌표와 검증 결과는 화면에서 확인이 끝나면
 * 쓸모가 없고, 열이 늘어날수록 주소 칸이 좁아져 읽기 어려워진다.
 *
 * 원본 워크북을 그대로 물려받던 예전 방식은 버렸다. 남길 열이 정해진 이상
 * 원본 서식을 지킬 이유가 없고, 원본 바이트를 들고 있을 이유도 사라진다.
 */
// 루트 export가 없다. 브라우저 번들을 직접 가리킨다(node/universal 빌드도 따로 있다).
import writeXlsxFile from 'write-excel-file/browser';
import type { Row as SheetRow, SheetData } from 'write-excel-file/browser';

import type { Node, Row } from '../types';

/** 우리가 매기는 순번 열. */
export const ORDER_COLUMN = '배송순서';

/**
 * 결과에 남길 원본 열.
 *
 * 여기 없는 이름의 열은 실리지 않는다. 손으로 채우는 빈 칸(서명)이나
 * 연락처처럼 종이에 인쇄하면 곤란한 열을 자동으로 걸러 내기 위한 목록이다.
 * 주소 열은 파일마다 이름이 달라 따로 받는다.
 */
export const KEEP_COLUMNS = ['연번', '이름'] as const;

/** 결과 파일 이름 접미사. `<원본이름>_배송순서완성.xlsx` */
export const OUTPUT_SUFFIX = '_배송순서완성';

/** 업로드 파일명에서 결과 파일명을 만든다. */
export function outputFileName(fileName: string, extension: 'xlsx'): string {
  const base = fileName.replace(/\.[^./\\]+$/, '') || 'result';
  return `${base}${OUTPUT_SUFFIX}.${extension}`;
}

/**
 * 최종 순서를 `원본 행 번호 → 배송순서(1부터)` 맵으로 바꾼다.
 * 좌표가 없어 최종 순서에 못 들어간 노드는 맵에 없다.
 */
export function buildOrderMap(
  nodes: readonly Node[],
  finalOrder: readonly number[],
): Map<number, number> {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const map = new Map<number, number>();
  finalOrder.forEach((nodeId, i) => {
    const node = byId.get(nodeId);
    if (node) map.set(node.rowIndex, i + 1);
  });
  return map;
}

// ── 결과 표 ─────────────────────────────────────────────────────────────────
export interface RecordsInput {
  /** 원본 헤더(열 순서 보존) */
  headers: readonly string[];
  /** 원본 행 */
  rows: readonly Row[];
  /** 좌표·검증 정보를 담은 노드 */
  nodes: readonly Node[];
  /** 최종 배송 순서(노드 id 순서열) */
  finalOrder: readonly number[];
  /** 주소 열 이름. 파일마다 달라 탐지 결과를 그대로 받는다 */
  addressColumn?: string | null;
}

export interface OutputRecord {
  /** 배송순서. 좌표를 못 찾아 경로에서 빠진 행은 null */
  order: number | null;
  /** 남긴 원본 열의 값(열 이름 → 값) */
  values: Record<string, string>;
}

/** 실을 원본 열을 원래 순서대로 고른다. */
export function outputColumns(
  headers: readonly string[],
  addressColumn?: string | null,
): string[] {
  const keep = new Set<string>(KEEP_COLUMNS);
  if (addressColumn) keep.add(addressColumn);
  const picked = headers.filter((h) => h !== ORDER_COLUMN && keep.has(h));
  // 이름이 다른 양식이면 고를 것이 없다. 그때는 버리지 말고 원본 열을 그대로 싣는다 —
  // 아무것도 없는 파일을 주는 것보다 낫다.
  return picked.length > 0 ? picked : headers.filter((h) => h !== ORDER_COLUMN);
}

/**
 * 결과 표를 만든다.
 *
 * 순번이 있는 행이 순번대로 먼저 오고, 순번이 없는 행(주소를 못 찾아 경로에서 빠진 행)은
 * 원래 순서를 지켜 뒤에 붙는다. **빠진 행도 반드시 싣는다** — 여기서 버리면 그 사람이
 * 배송 명단에서 조용히 사라진다.
 */
export function buildRecords(input: RecordsInput): {
  columns: string[];
  records: OutputRecord[];
} {
  const { headers, rows, nodes, finalOrder, addressColumn } = input;
  const orderByRow = buildOrderMap(nodes, finalOrder);
  const columns = outputColumns(headers, addressColumn);

  const toRecord = (row: Row): OutputRecord => {
    const values: Record<string, string> = {};
    for (const col of columns) {
      const raw = row[col];
      values[col] = raw == null ? '' : String(raw);
    }
    return { order: orderByRow.get(row.rowIndex) ?? null, values };
  };

  const ordered = rows
    .filter((row) => orderByRow.has(row.rowIndex))
    .map(toRecord)
    .sort((a, b) => (a.order ?? 0) - (b.order ?? 0));
  const rest = rows.filter((row) => !orderByRow.has(row.rowIndex)).map(toRecord);

  return { columns, records: [...ordered, ...rest] };
}

// ── 서식 ────────────────────────────────────────────────────────────────────
const HEADER_BG = '#1E4ED8';
const HEADER_FG = '#FFFFFF';
const BORDER = '#C7D2F5';
/** 순번이 없는 행(경로에서 빠진 행) 배경. 파일만 봐도 눈에 띄어야 한다. */
const EXCLUDED_BG = '#FEF3C7';

/** 글자 수로 열 너비를 잡는다. 한글은 대략 두 칸을 차지한다. */
function columnWidth(header: string, values: readonly string[]): number {
  const cells = [header, ...values];
  const longest = cells.reduce((max, v) => Math.max(max, displayWidth(v)), 0);
  return Math.min(Math.max(longest + 4, 8), 64);
}

function displayWidth(text: string): number {
  let width = 0;
  for (const ch of text) width += ch.charCodeAt(0) > 0x2e80 ? 2 : 1;
  return width;
}

/** 가운데로 모을 열 — 숫자이거나 짧은 값이라 왼쪽 정렬이 오히려 읽기 나쁘다. */
const CENTERED = new Set<string>([ORDER_COLUMN, '연번']);

/**
 * 서식을 입힌 xlsx Blob을 만든다.
 *
 * 머리글은 고정(첫 행 sticky)이라 아래로 내려도 어느 열인지 보인다.
 * 순번이 없는 행은 노란 배경으로 구분한다 — 실려는 있지만 경로에는 없는 사람이다.
 */
export async function buildXlsx(input: RecordsInput, sheetName = '배송순서'): Promise<Blob> {
  const { columns, records } = buildRecords(input);
  const allColumns = [ORDER_COLUMN, ...columns];

  const header: SheetRow = allColumns.map((name) => ({
    value: name,
    fontWeight: 'bold' as const,
    backgroundColor: HEADER_BG,
    textColor: HEADER_FG,
    align: 'center' as const,
    alignVertical: 'center' as const,
    borderColor: BORDER,
    borderStyle: 'thin' as const,
    height: 22,
  }));

  const body: SheetRow[] = records.map((record) => {
    const excluded = record.order === null;
    return allColumns.map((name) => {
      const isOrder = name === ORDER_COLUMN;
      return {
        // 순번은 숫자로 써야 엑셀에서 정렬·필터가 제대로 동작한다.
        // 순번이 없는 행은 값을 비운다(0으로 쓰면 1번 앞에 서 버린다).
        ...(isOrder
          ? record.order === null
            ? {}
            : { type: Number, value: record.order }
          : { value: record.values[name] }),
        align: CENTERED.has(name) ? ('center' as const) : ('left' as const),
        alignVertical: 'center' as const,
        borderColor: BORDER,
        borderStyle: 'thin' as const,
        ...(excluded ? { backgroundColor: EXCLUDED_BG } : {}),
      };
    });
  });

  const widths = allColumns.map((name) => ({
    width: columnWidth(
      name,
      records.map((r) => (name === ORDER_COLUMN ? String(r.order ?? '') : r.values[name] ?? '')),
    ),
  }));

  const data: SheetData = [header, ...body];
  return writeXlsxFile(
    data,
    { sheet: sheetName, columns: widths, stickyRowsCount: 1 },
    { fontFamily: 'Malgun Gothic', fontSize: 11 },
  ).toBlob();
}

// ── 내려받기 ────────────────────────────────────────────────────────────────
/** Blob을 파일로 내려받는다. */
export function downloadBlob(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}
