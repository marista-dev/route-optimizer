/**
 * 결과 파일 생성 — `src/app.py`의 `_save_xlsx` 이식.
 *
 * - xlsx: 원본 워크북을 다시 읽어 대상 시트 1열에 '배송순서'를 끼워 넣고
 *         (이미 있으면 값을 지우고 덮어씀) 순번대로 행을 정렬한다.
 *         SheetJS CE는 셀 서식을 보존하지 못한다(계획 12절).
 * - csv : 원본 열 + 좌표·검증 열을 모두 싣고 '배송순서'를 맨 앞에 둔다. UTF-8 BOM.
 */
import Papa from 'papaparse';
import * as XLSX from 'xlsx';

import type { Node, Row } from '../types';

/** 결과 열 이름 — 데스크톱 산출물과 글자 하나까지 같아야 한다. */
export const ORDER_COLUMN = '배송순서';
export const LAT_COLUMN = 'Latitude';
export const LON_COLUMN = 'Longitude';
export const KAKAO_ADDR_COLUMN = '카카오_확인주소';
export const REVERSE_ADDR_COLUMN = '역지오코딩_주소';
export const VERDICT_COLUMN = '주소검증결과';

/** 결과 파일 이름 접미사. `<원본이름>_배송순서완성.<확장자>` */
export const OUTPUT_SUFFIX = '_배송순서완성';

/** 업로드 파일명에서 결과 파일명을 만든다. */
export function outputFileName(fileName: string, extension: 'xlsx' | 'csv'): string {
  const base = fileName.replace(/\.[^./\\]+$/, '') || 'result';
  return `${base}${OUTPUT_SUFFIX}.${extension}`;
}

/**
 * 최종 순서를 `원본 행 번호 → 배송순서(1부터)` 맵으로 바꾼다.
 * 좌표가 없어 최종 순서에 못 들어간 노드는 맵에 없다.
 */
export function buildOrderMap(nodes: readonly Node[], finalOrder: readonly number[]): Map<number, number> {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const map = new Map<number, number>();
  finalOrder.forEach((nodeId, i) => {
    const node = byId.get(nodeId);
    if (node) map.set(node.rowIndex, i + 1);
  });
  return map;
}

// ── CSV ─────────────────────────────────────────────────────────────────────
export interface CsvInput {
  /** 원본 헤더(열 순서 보존) */
  headers: readonly string[];
  /** 원본 행 */
  rows: readonly Row[];
  /** 좌표·검증 정보를 담은 노드 */
  nodes: readonly Node[];
  /** 최종 배송 순서(노드 id 순서열) */
  finalOrder: readonly number[];
}

/** 좌표·검증 열. 원본에 이미 있으면 그 자리에 값만 덮어쓴다. */
const EXTRA_COLUMNS = [
  LAT_COLUMN,
  LON_COLUMN,
  KAKAO_ADDR_COLUMN,
  REVERSE_ADDR_COLUMN,
  VERDICT_COLUMN,
] as const;

/**
 * 결과 표를 만든다. CSV와 "원본 없이 만드는 xlsx"가 같은 열·같은 값을 쓰도록
 * 조립을 한곳에 모았다.
 *
 * 열 구성: 배송순서 → 원본 열 전부 → 원본에 없던 좌표·검증 열.
 * 원본 열은 하나도 버리지 않는다 — 주소·연락처처럼 앱이 직접 쓰지 않는 열도
 * 그대로 실려야 현장에서 쓸 수 있다. 원본이 이미 결과 파일이어서 Latitude 등을
 * 갖고 있으면 **원래 자리에** 두고 값만 새로 쓴다(데스크톱판과 같은 규칙).
 *
 * @param includeUnordered 순번이 없는 행(지오코딩 실패 등)도 뒤에 붙인다.
 *   CSV는 데스크톱판과 같이 제외하고, xlsx는 원본을 통째로 보존하므로 포함한다.
 *   이 구분이 없으면 같은 "엑셀" 버튼이 원본 유무에 따라 다른 행 수를 뱉는다.
 */
export function buildRecords(
  input: CsvInput,
  includeUnordered = false,
): {
  columns: string[];
  records: Record<string, unknown>[];
} {
  const { headers, rows, nodes, finalOrder } = input;
  const orderByRow = buildOrderMap(nodes, finalOrder);
  const nodeByRow = new Map(nodes.map((n) => [n.rowIndex, n]));

  const keptColumns = headers.filter((h) => h !== ORDER_COLUMN);
  const appendedColumns = EXTRA_COLUMNS.filter((c) => !keptColumns.includes(c));
  const dataColumns = [...keptColumns, ...appendedColumns];
  const columns = [ORDER_COLUMN, ...dataColumns];

  const valueOf = (column: string, row: Row, node: Node | undefined): unknown => {
    switch (column) {
      case LAT_COLUMN: return node?.lat ?? '';
      case LON_COLUMN: return node?.lon ?? '';
      case KAKAO_ADDR_COLUMN: return node?.kakaoAddr ?? '';
      case REVERSE_ADDR_COLUMN: return node?.reverseAddr ?? '';
      case VERDICT_COLUMN: return node?.verdict ?? '';
      default: return row[column] ?? '';
    }
  };

  const toRecord = (row: Row): Record<string, unknown> => {
    const node = nodeByRow.get(row.rowIndex);
    const record: Record<string, unknown> = { [ORDER_COLUMN]: orderByRow.get(row.rowIndex) ?? '' };
    for (const col of dataColumns) record[col] = valueOf(col, row, node);
    return record;
  };

  const ordered = rows
    .filter((row) => orderByRow.has(row.rowIndex))
    .map(toRecord)
    .sort((a, b) => (a[ORDER_COLUMN] as number) - (b[ORDER_COLUMN] as number));

  if (!includeUnordered) return { columns, records: ordered };

  // 순번 없는 행은 원래 순서를 지켜 뒤에 붙인다(파이썬 `_row_sort_key`와 같은 규칙).
  const rest = rows.filter((row) => !orderByRow.has(row.rowIndex)).map(toRecord);
  const records = [...ordered, ...rest];

  return { columns, records };
}

export function buildCsv(input: CsvInput): Blob {
  const { columns, records } = buildRecords(input);

  const csv = Papa.unparse(records, { columns });
  // 엑셀이 한글을 깨지 않게 UTF-8 BOM을 붙인다(pandas `encoding='utf-8-sig'`와 동일).
  return new Blob(['\uFEFF', csv], { type: 'text/csv;charset=utf-8;' });
}

// ── xlsx ────────────────────────────────────────────────────────────────────
/** 숫자 순번이 먼저, 순번 없는 행은 뒤에 원래 순서 그대로(파이썬 `_row_sort_key`). */
function compareByOrder(a: unknown, b: unknown): number {
  const aNum = typeof a === 'number' && Number.isFinite(a);
  const bNum = typeof b === 'number' && Number.isFinite(b);
  if (aNum && bNum) return (a as number) - (b as number);
  if (aNum) return -1;
  if (bNum) return 1;
  return 0;
}

/** xlsx/xlsm/ods는 ZIP(`PK\x03\x04`), 구형 xls는 OLE2(`D0CF11E0`)로 시작한다. */
function looksLikeBinaryWorkbook(buffer: ArrayBuffer): boolean {
  const head = new Uint8Array(buffer, 0, Math.min(8, buffer.byteLength));
  if (head[0] === 0x50 && head[1] === 0x4b) return true; // "PK"
  return (
    head[0] === 0xd0 && head[1] === 0xcf && head[2] === 0x11 && head[3] === 0xe0
  );
}

/**
 * 업로드 바이트를 워크북으로 읽는다.
 *
 * CSV는 반드시 UTF-8로 **직접 디코딩**해서 문자열로 넘긴다. 바이트를 그대로
 * 주면 SheetJS가 BOM 없는 UTF-8을 cp1252로 오인해 한글이 전부 깨진다
 * (읽기 경로 `readFile.parseCsv`는 처음부터 TextDecoder를 썼기 때문에
 * 화면은 멀쩡하고 내려받은 파일만 깨지는 형태로 드러났다).
 * TextDecoder가 BOM을 벗겨 주지만 이중 안전장치로 한 번 더 지운다.
 */
function readWorkbook(originalBuffer: ArrayBuffer): XLSX.WorkBook {
  if (looksLikeBinaryWorkbook(originalBuffer)) {
    return XLSX.read(originalBuffer, { type: 'array' });
  }
  const text = new TextDecoder('utf-8').decode(originalBuffer).replace(/^\uFEFF/, '');
  return XLSX.read(text, { type: 'string' });
}

/**
 * 원본 워크북에 '배송순서' 열을 넣어 xlsx Blob을 만든다.
 *
 * @param originalBuffer 업로드한 파일의 원본 바이트. CSV면 UTF-8로 디코딩해 읽는다
 * @param sheetName 주소 열을 찾은 시트명. null이면 첫 시트
 * @param orders 원본 행 번호(0부터) → 배송순서
 */
export function buildXlsx(
  originalBuffer: ArrayBuffer,
  sheetName: string | null,
  orders: ReadonlyMap<number, number>,
): Blob {
  const wb = readWorkbook(originalBuffer);
  const target = sheetName && wb.Sheets[sheetName] ? sheetName : wb.SheetNames[0];
  const sheet = wb.Sheets[target];
  if (!sheet) throw new Error('저장할 시트를 찾지 못했습니다.');

  const aoa = XLSX.utils.sheet_to_json<unknown[]>(sheet, {
    header: 1,
    blankrows: true,
    defval: null,
    raw: true,
  });
  const header = (aoa[0] ?? []).map((v) => (v == null ? '' : v));
  const dataRows = aoa.slice(1);

  let col = header.findIndex((v) => String(v) === ORDER_COLUMN);
  if (col >= 0) {
    // 재실행 시 이전 값 잔존 방지 — 열 전체 초기화
    for (const row of dataRows) row[col] = null;
  } else {
    header.unshift(ORDER_COLUMN);
    for (const row of dataRows) row.unshift(null);
    col = 0;
  }

  orders.forEach((order, rowIndex) => {
    const row = dataRows[rowIndex];
    if (row) row[col] = order;
  });

  // Array.prototype.sort는 안정 정렬이라 순번 없는 행의 원래 순서가 유지된다.
  dataRows.sort((a, b) => compareByOrder(a[col], b[col]));

  const nextSheet = XLSX.utils.aoa_to_sheet([header, ...dataRows]);
  wb.Sheets[target] = nextSheet;

  const out = XLSX.write(wb, { bookType: 'xlsx', type: 'array' }) as ArrayBuffer;
  return new Blob([out], {
    type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  });
}

/**
 * 원본 파일 없이 결과만으로 xlsx Blob을 만든다.
 *
 * 원본 바이트는 메모리에만 있어(localStorage에 못 담는다) 새로고침하면 사라진다.
 * 그때도 xlsx를 받을 수 있어야 하므로 CSV와 **같은 열·같은 값**으로 시트를 짠다.
 * 원본 열은 전부 살리고, 좌표·검증 열만 뒤에 덧붙는다.
 * 원본 워크북의 서식은 재현하지 못한다(값만 옮긴다).
 */
export function buildXlsxFromRecords(input: CsvInput, sheetName = '배송순서'): Blob {
  // 원본 워크북 경로(`buildXlsx`)는 순번 없는 행도 뒤에 남긴다. 같은 버튼이
  // 원본 유무에 따라 사람을 빠뜨리면 안 되므로 여기서도 똑같이 포함한다.
  const { columns, records } = buildRecords(input, true);
  const aoa: unknown[][] = [
    columns,
    ...records.map((r) => columns.map((c) => r[c] ?? '')),
  ];
  const sheet = XLSX.utils.aoa_to_sheet(aoa);
  // 첫 행 고정 — 124행을 훑을 때 머리글이 따라다녀야 쓸 만하다.
  sheet['!freeze'] = { xSplit: 0, ySplit: 1 };
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, sheet, sheetName.slice(0, 31));
  const out = XLSX.write(wb, { bookType: 'xlsx', type: 'array' }) as ArrayBuffer;
  return new Blob([out], {
    type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  });
}

// ── 다운로드 ────────────────────────────────────────────────────────────────
/** Blob을 파일로 내려받는다. 브라우저에서만 동작한다. */
export function downloadBlob(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.style.display = 'none';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  // 클릭 직후 revoke하면 일부 브라우저에서 저장이 취소된다.
  setTimeout(() => URL.revokeObjectURL(url), 1_000);
}
