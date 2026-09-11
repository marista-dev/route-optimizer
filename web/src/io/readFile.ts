/**
 * 업로드 파일 파싱 — `src/app.py` 1단계(시트·주소 열 탐색) 이식.
 *
 * 모든 시트를 훑어 '택배받을 주소'가 들어간 열을 찾고, 없으면 첫 시트를 쓴다.
 * 원본 열은 하나도 버리지 않는다(결과 CSV·xlsx가 원본 열을 그대로 싣기 때문).
 */
import Papa from 'papaparse';
import * as XLSX from 'xlsx';

import type { Row } from '../types';

/** 주소 열 판별 기준. 실제 열 이름은 '택배받을 주소 (도로명)'처럼 뒤가 붙는다. */
export const ADDRESS_COLUMN_HINT = '택배받을 주소';
/** 이름 열 이름. 없으면 빈 문자열로 둔다. */
export const NAME_COLUMN = '이름';

/** 지오코딩 대상 행 하나의 씨앗. `Node`는 여기에 좌표·판정을 붙여 만든다. */
export interface NodeSeed {
  rowIndex: number;
  name: string;
  address: string;
}

export interface ParsedFile {
  /** 주소 열을 찾은 시트명. CSV면 null */
  sheetName: string | null;
  /** 주소가 들어 있는 열의 실제 이름 */
  addressColumn: string;
  /** 원본 헤더(열 순서 보존) */
  headers: string[];
  /** 주소가 빈 행을 걸러낸 원본 행 목록 */
  rows: Row[];
  /** 지오코딩 씨앗 */
  nodesSeed: NodeSeed[];
  /** 원본 파일 바이트. xlsx 저장 때 원본 워크북을 다시 읽는 데 쓴다 */
  originalBuffer: ArrayBuffer;
}

/** 업로드 입력으로 받아들이는 형태. */
export type UploadInput = File | Blob | ArrayBuffer | Uint8Array;

async function toArrayBuffer(input: UploadInput): Promise<ArrayBuffer> {
  if (input instanceof ArrayBuffer) return input;
  if (input instanceof Uint8Array) {
    return input.buffer.slice(
      input.byteOffset,
      input.byteOffset + input.byteLength,
    ) as ArrayBuffer;
  }
  return await input.arrayBuffer();
}

function isAddressHeader(header: unknown): boolean {
  return String(header ?? '').includes(ADDRESS_COLUMN_HINT);
}

/** 헤더 배열에서 주소 열 이름을 찾는다. 없으면 null. */
function findAddressColumn(headers: string[]): string | null {
  return headers.find(isAddressHeader) ?? null;
}

/** 시트를 (헤더, 데이터 행 배열)로 읽는다. 빈 행도 버리지 않아 행 번호가 원본과 맞는다. */
function sheetToAoa(sheet: XLSX.WorkSheet): unknown[][] {
  return XLSX.utils.sheet_to_json<unknown[]>(sheet, {
    header: 1,
    blankrows: true,
    defval: null,
    raw: true,
  });
}

function headerNames(aoa: unknown[][]): string[] {
  return (aoa[0] ?? []).map((v) => (v == null ? '' : String(v)));
}

/**
 * 헤더·데이터 행에서 `Row[]`와 씨앗을 만든다.
 * 주소가 빈 행은 버리되 `rowIndex`는 버리기 전 순번을 그대로 쓴다
 * (원본 시트의 행 위치여야 xlsx 저장 때 같은 줄에 순번을 넣을 수 있다).
 */
function buildRows(
  headers: string[],
  dataRows: unknown[][],
  addressColumn: string,
): { rows: Row[]; nodesSeed: NodeSeed[] } {
  const hasName = headers.includes(NAME_COLUMN);
  const rows: Row[] = [];
  const nodesSeed: NodeSeed[] = [];

  dataRows.forEach((cells, rowIndex) => {
    const row: Row = { rowIndex };
    headers.forEach((header, col) => {
      if (header === '') return;
      row[header] = cells[col] ?? null;
    });
    const address = String(row[addressColumn] ?? '').trim();
    if (!address) return;
    row[addressColumn] = address;
    rows.push(row);
    nodesSeed.push({
      rowIndex,
      name: hasName ? String(row[NAME_COLUMN] ?? '').trim() : '',
      address,
    });
  });

  return { rows, nodesSeed };
}

function parseCsv(buffer: ArrayBuffer): {
  headers: string[];
  dataRows: unknown[][];
} {
  // UTF-8 BOM은 TextDecoder가 알아서 벗겨준다.
  const text = new TextDecoder('utf-8').decode(buffer).replace(/^\uFEFF/, '');
  const parsed = Papa.parse<string[]>(text, { header: false, skipEmptyLines: false });
  const table = parsed.data.filter((r) => Array.isArray(r));
  const headers = (table[0] ?? []).map((v) => String(v ?? '').trim());
  // 마지막 줄바꿈 때문에 생기는 완전 빈 행 하나만 떨어낸다.
  const dataRows = table.slice(1);
  while (dataRows.length > 0) {
    const last = dataRows[dataRows.length - 1];
    if (last.length === 0 || last.every((v) => String(v ?? '').trim() === '')) dataRows.pop();
    else break;
  }
  return { headers, dataRows };
}

/**
 * 업로드 파일을 파싱한다.
 * @throws 주소 열을 어느 시트에서도 못 찾으면 Error
 */
export async function parseUploadedFile(
  file: UploadInput,
  fileName: string,
): Promise<ParsedFile> {
  const originalBuffer = await toArrayBuffer(file);
  const isCsv = /\.csv$/i.test(fileName);

  let sheetName: string | null = null;
  let headers: string[];
  let dataRows: unknown[][];

  if (isCsv) {
    const parsed = parseCsv(originalBuffer);
    headers = parsed.headers;
    dataRows = parsed.dataRows;
  } else {
    const wb = XLSX.read(originalBuffer, { type: 'array' });
    if (wb.SheetNames.length === 0) throw new Error('시트가 없는 파일입니다.');

    let picked: unknown[][] | null = null;
    for (const name of wb.SheetNames) {
      const aoa = sheetToAoa(wb.Sheets[name]);
      if (findAddressColumn(headerNames(aoa))) {
        sheetName = name;
        picked = aoa;
        break;
      }
    }
    if (!picked) {
      sheetName = wb.SheetNames[0];
      picked = sheetToAoa(wb.Sheets[sheetName]);
    }
    headers = headerNames(picked);
    dataRows = picked.slice(1);
  }

  const addressColumn = findAddressColumn(headers);
  if (!addressColumn) {
    throw new Error(`'${ADDRESS_COLUMN_HINT}' 열이 없습니다.`);
  }

  const { rows, nodesSeed } = buildRows(headers, dataRows, addressColumn);
  return { sheetName, addressColumn, headers, rows, nodesSeed, originalBuffer };
}
