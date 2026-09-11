import { describe, expect, it } from 'vitest';
import * as XLSX from 'xlsx';

import type { Node, Row } from '../types';
import { buildOrderMap, buildXlsx, buildXlsxFromRecords, outputFileName } from './writeFile';

/** 모든 데이터는 합성값이다. */
const ADDRESS_HEADER = '택배받을 주소 (도로명)';
const HEADERS = ['이름', ADDRESS_HEADER, '연락처'];

function makeXlsx(aoa: unknown[][], sheetName = '배송목록'): ArrayBuffer {
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(aoa), sheetName);
  return XLSX.write(wb, { bookType: 'xlsx', type: 'array' }) as ArrayBuffer;
}

function readBack(blob: ArrayBuffer, sheetName = '배송목록'): unknown[][] {
  const wb = XLSX.read(blob, { type: 'array' });
  return XLSX.utils.sheet_to_json<unknown[]>(wb.Sheets[sheetName], {
    header: 1,
    blankrows: true,
    defval: null,
  });
}

const BODY: unknown[][] = [
  ['가나다', '광주 북구 테스트로 1', '000-0000-0001'],
  ['라마바', '광주 북구 시험로 22', '000-0000-0002'],
  ['사아자', '', '000-0000-0003'],
  ['차카타', '광주 북구 예시로 3', '000-0000-0004'],
];

function node(id: number, rowIndex: number, over: Partial<Node> = {}): Node {
  return {
    id,
    rowIndex,
    name: `이름${id}`,
    address: `주소${id}`,
    lat: 35 + id / 100,
    lon: 126 + id / 100,
    kakaoAddr: `카카오주소${id}`,
    reverseAddr: `역주소${id}`,
    verdict: '일치',
    ...over,
  };
}

describe('outputFileName', () => {
  it('확장자를 갈아끼우고 접미사를 붙인다', () => {
    expect(outputFileName('배송목록.xlsx', 'xlsx')).toBe('배송목록_배송순서완성.xlsx');
    expect(outputFileName('배송목록.csv', 'xlsx')).toBe('배송목록_배송순서완성.xlsx');
  });
});

describe('buildOrderMap', () => {
  it('최종 순서를 원본 행 번호 → 1부터의 순번으로 바꾼다', () => {
    const nodes = [node(0, 0), node(1, 1), node(2, 3)];
    expect(buildOrderMap(nodes, [2, 0])).toEqual(new Map([[3, 1], [0, 2]]));
  });
});

describe('buildXlsx', () => {
  it("'배송순서'를 A열에 넣고 순번대로 행을 정렬한다", async () => {
    const buffer = makeXlsx([HEADERS, ...BODY]);
    const orders = new Map([
      [0, 2],
      [1, 3],
      [3, 1],
    ]);

    const aoa = readBack(await buildXlsx(buffer, '배송목록', orders).arrayBuffer());

    expect(aoa[0]).toEqual(['배송순서', ...HEADERS]);
    expect(aoa.slice(1).map((r) => [r[0], r[1]])).toEqual([
      [1, '차카타'],
      [2, '가나다'],
      [3, '라마바'],
      [null, '사아자'],
    ]);
  });

  it("이미 있는 '배송순서' 열은 덮어쓰고 중복 생성하지 않는다", async () => {
    const buffer = makeXlsx([
      ['배송순서', ...HEADERS],
      [99, '가나다', '광주 북구 테스트로 1', '000-0000-0001'],
      [98, '라마바', '광주 북구 시험로 22', '000-0000-0002'],
    ]);

    const aoa = readBack(await buildXlsx(buffer, '배송목록', new Map([[1, 1]])).arrayBuffer());

    expect(aoa[0]).toEqual(['배송순서', ...HEADERS]);
    expect(aoa[0].filter((h) => h === '배송순서')).toHaveLength(1);
    expect(aoa.slice(1).map((r) => [r[0], r[1]])).toEqual([
      [1, '라마바'],
      [null, '가나다'],
    ]);
  });

  it('시트명이 null이면 첫 시트에 쓴다', async () => {
    const buffer = makeXlsx([HEADERS, ...BODY], '첫시트');

    const aoa = readBack(
      await buildXlsx(buffer, null, new Map([[0, 1]])).arrayBuffer(),
      '첫시트',
    );

    expect(aoa[0][0]).toBe('배송순서');
    expect(aoa[1][0]).toBe(1);
  });
});

describe('buildXlsxFromRecords — 열과 값', () => {
  const rows: Row[] = BODY.map((cells, rowIndex) => ({
    rowIndex,
    이름: cells[0],
    [ADDRESS_HEADER]: cells[1],
    연락처: cells[2],
  }));
  const nodes = [node(0, 0), node(1, 1), node(2, 3)];

  it('배송순서가 첫 열이고 좌표·검증 열이 뒤에 붙는다', async () => {
    const blob = buildXlsxFromRecords({ headers: HEADERS, rows, nodes, finalOrder: [2, 0, 1] });
    const aoa = readBack(await blob.arrayBuffer(), '배송순서');

    expect(aoa[0]).toEqual([
      '배송순서', ...HEADERS,
      'Latitude', 'Longitude', '카카오_확인주소', '역지오코딩_주소', '주소검증결과',
    ]);
  });

  it('좌표와 검증 정보를 노드에서 가져온다', async () => {
    const custom = [node(0, 0, { lat: null, lon: null, verdict: '위치없음', reverseAddr: '' })];
    const blob = buildXlsxFromRecords({ headers: HEADERS, rows, nodes: custom, finalOrder: [0] });
    const aoa = readBack(await blob.arrayBuffer(), '배송순서');
    const header = aoa[0] as string[];

    expect(aoa[1][header.indexOf('주소검증결과')]).toBe('위치없음');
    expect(aoa[1][header.indexOf('Latitude')]).toBe('');
  });
});

// ── H4: CSV 원본을 xlsx로 내려받을 때 한글이 깨지지 않아야 한다 ─────────────
describe('buildXlsx — CSV 원본(H4)', () => {
  const CSV_TEXT = [
    '이름,택배받을 주소 (도로명),연락처',
    '가나다,광주 북구 테스트로 1,000-0000-0001',
    '라마바,광주 북구 시험로 22,000-0000-0002',
    '차카타,광주 북구 예시로 3,000-0000-0004',
  ].join('\n');

  /** BOM 없는 UTF-8 바이트 — SheetJS가 cp1252로 오인하던 입력. */
  function utf8NoBom(text: string): ArrayBuffer {
    const bytes = new TextEncoder().encode(text);
    expect(bytes[0]).not.toBe(0xef); // BOM이 없음을 못박는다
    return bytes.buffer.slice(0, bytes.byteLength) as ArrayBuffer;
  }

  function utf8WithBom(text: string): ArrayBuffer {
    const bytes = new TextEncoder().encode(`﻿${text}`);
    return bytes.buffer.slice(0, bytes.byteLength) as ArrayBuffer;
  }

  it('BOM 없는 UTF-8 CSV의 한글 셀이 그대로 나온다', async () => {
    const aoa = readBack(
      await buildXlsx(utf8NoBom(CSV_TEXT), null, new Map([[2, 1]])).arrayBuffer(),
      'Sheet1',
    );

    expect(aoa[0]).toEqual(['배송순서', '이름', '택배받을 주소 (도로명)', '연락처']);
    expect(aoa[1]).toEqual([1, '차카타', '광주 북구 예시로 3', '000-0000-0004']);
    expect(aoa.map((r) => r[1])).toEqual(['이름', '차카타', '가나다', '라마바']);
  });

  it('BOM이 붙은 UTF-8 CSV도 같은 결과다', async () => {
    const aoa = readBack(
      await buildXlsx(utf8WithBom(CSV_TEXT), null, new Map([[0, 1]])).arrayBuffer(),
      'Sheet1',
    );

    expect(aoa[0][1]).toBe('이름');
    expect(aoa[1]).toEqual([1, '가나다', '광주 북구 테스트로 1', '000-0000-0001']);
  });

  it('xlsx 바이트는 여전히 바이너리로 읽는다', async () => {
    const buffer = makeXlsx([HEADERS, ...BODY]);
    const aoa = readBack(await buildXlsx(buffer, '배송목록', new Map([[0, 1]])).arrayBuffer());

    expect(aoa[1]).toEqual([1, '가나다', '광주 북구 테스트로 1', '000-0000-0001']);
  });
});

// ── LOW 11: 원본에 이미 있는 좌표·검증 열은 제자리에서 덮어쓴다 ─────────────
describe('buildXlsxFromRecords — 이미 결과 열이 있는 원본', () => {
  const RESULT_HEADERS = [
    '배송순서', '이름', ADDRESS_HEADER, 'Latitude', 'Longitude', '연락처',
    '카카오_확인주소', '역지오코딩_주소', '주소검증결과',
  ];
  const rows: Row[] = [
    {
      rowIndex: 0,
      배송순서: 9,
      이름: '가나다',
      [ADDRESS_HEADER]: '광주 북구 테스트로 1',
      Latitude: 1,
      Longitude: 2,
      연락처: '000-0000-0001',
      카카오_확인주소: '옛값',
      역지오코딩_주소: '옛값',
      주소검증결과: '옛값',
    },
  ];

  async function headerAndRow(headers: readonly string[], input: Row[]): Promise<unknown[][]> {
    const blob = buildXlsxFromRecords({
      headers,
      rows: input,
      nodes: [node(0, 0)],
      finalOrder: [0],
    });
    return readBack(await blob.arrayBuffer(), '배송순서');
  }

  it('중복 열을 뒤에 덧붙이지 않고 원래 자리를 지킨다', async () => {
    const aoa = await headerAndRow(RESULT_HEADERS, rows);

    expect(aoa[0]).toEqual(RESULT_HEADERS);
    // 이름이 한 번씩만 나온다
    const names = aoa[0] as string[];
    expect(new Set(names).size).toBe(names.length);
  });

  it('제자리 열의 값은 노드 값으로 새로 쓴다', async () => {
    const cells = (await headerAndRow(RESULT_HEADERS, rows))[1];

    expect(cells[0]).toBe(1);             // 옛 배송순서 9를 덮어씀
    expect(cells[3]).toBe(35);            // Latitude ← node(0).lat
    expect(cells[4]).toBe(126);           // Longitude ← node(0).lon
    expect(cells[6]).toBe('카카오주소0');
    expect(cells[7]).toBe('역주소0');
    expect(cells[8]).toBe('일치');
  });

  it('일부만 있으면 없는 것만 뒤에 붙인다', async () => {
    const aoa = await headerAndRow(
      ['이름', ADDRESS_HEADER, '주소검증결과'],
      [{ rowIndex: 0, 이름: '가나다', [ADDRESS_HEADER]: 'x', 주소검증결과: '옛값' }],
    );

    expect(aoa[0]).toEqual([
      '배송순서', '이름', ADDRESS_HEADER, '주소검증결과',
      'Latitude', 'Longitude', '카카오_확인주소', '역지오코딩_주소',
    ]);
  });
});

describe('buildXlsxFromRecords — 원본 없이 만드는 xlsx', () => {
  const rows: Row[] = BODY.map((cells, rowIndex) => ({
    rowIndex,
    이름: cells[0],
    [ADDRESS_HEADER]: cells[1],
    연락처: cells[2],
  }));
  const nodes = [node(0, 0), node(1, 1), node(2, 3)];

  it('원본 열을 하나도 버리지 않고 배송순서 순으로 담는다', async () => {
    const blob = buildXlsxFromRecords({ headers: HEADERS, rows, nodes, finalOrder: [2, 0, 1] });
    const aoa = readBack(await blob.arrayBuffer(), '배송순서');

    const header = aoa[0] as string[];
    expect(header[0]).toBe('배송순서');
    // 원본 열이 전부 살아 있어야 한다 — 주소·연락처는 앱이 쓰지 않지만 현장에서 쓴다.
    for (const col of HEADERS) expect(header).toContain(col);
    // 좌표·검증 열은 뒤에 덧붙는다.
    expect(header).toContain('Latitude');
    expect(header).toContain('주소검증결과');

    // 순번 있는 3건이 1·2·3 순서로 먼저 오고, 순번 없는 행도 뒤에 남는다.
    // (원본 워크북 경로와 같은 규칙 — 새로고침 여부로 사람이 빠지면 안 된다.)
    expect(aoa.length).toBe(BODY.length + 1);
    expect(aoa.slice(1, 4).map((r) => r[0])).toEqual([1, 2, 3]);
    expect(aoa[4][0]).toBe('');

    const addrCol = header.indexOf(ADDRESS_HEADER);
    const nameCol = header.indexOf('이름');
    // finalOrder [2,0,1] → 1번은 node 2(rowIndex 3), 2번은 node 0(rowIndex 0)
    expect(aoa[1][nameCol]).toBe('차카타');
    expect(aoa[1][addrCol]).toBe('광주 북구 예시로 3');
    expect(aoa[2][nameCol]).toBe('가나다');
    expect(aoa[2][addrCol]).toBe('광주 북구 테스트로 1');
  });

  it('한글이 깨지지 않는다', async () => {
    const blob = buildXlsxFromRecords({ headers: HEADERS, rows, nodes, finalOrder: [0] });
    const aoa = readBack(await blob.arrayBuffer(), '배송순서');
    expect(JSON.stringify(aoa)).toContain('광주 북구 테스트로 1');
  });
});

describe('xlsx 두 경로가 같은 행 집합을 낸다', () => {
  const rows: Row[] = BODY.map((cells, rowIndex) => ({
    rowIndex,
    이름: cells[0],
    [ADDRESS_HEADER]: cells[1],
    연락처: cells[2],
  }));
  // rowIndex 2(주소 빈 행)는 노드가 없어 순번을 못 받는다.
  const nodes = [node(0, 0), node(1, 1), node(2, 3)];
  const finalOrder = [2, 0, 1];

  it('원본이 있든 없든 순번 없는 행까지 그대로 남는다', async () => {
    const withOriginal = readBack(
      await buildXlsx(makeXlsx([HEADERS, ...BODY]), '배송목록', buildOrderMap(nodes, finalOrder)).arrayBuffer(),
    );
    const withoutOriginal = readBack(
      await buildXlsxFromRecords({ headers: HEADERS, rows, nodes, finalOrder }).arrayBuffer(),
      '배송순서',
    );

    // 데이터 행 수가 같아야 한다 — 새로고침 여부로 사람이 빠지면 안 된다.
    expect(withoutOriginal.length - 1).toBe(withOriginal.length - 1);
    expect(withoutOriginal.length - 1).toBe(BODY.length);

    const nameCol = (withoutOriginal[0] as string[]).indexOf('이름');
    const names = withoutOriginal.slice(1).map((r) => r[nameCol]);
    // 순번 있는 3건이 먼저, 순번 없는 '사아자'가 마지막.
    expect(names[names.length - 1]).toBe('사아자');
    expect(names).toContain('사아자');
  });
});
