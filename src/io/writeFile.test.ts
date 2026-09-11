import { describe, expect, it } from 'vitest';
import * as XLSX from 'xlsx';

import type { Node, Row } from '../types';
import {
  ORDER_COLUMN,
  buildOrderMap,
  buildRecords,
  buildXlsx,
  outputColumns,
  outputFileName,
} from './writeFile';

/** 모든 데이터는 합성값이다. */
const ADDRESS_HEADER = '택배받을 주소';
const HEADERS = ['연번', '이름', ADDRESS_HEADER, '서명', '연락처'];

function row(rowIndex: number, values: Record<string, unknown>): Row {
  return { rowIndex, ...values } as Row;
}

const ROWS: Row[] = [
  row(0, { 연번: 13, 이름: '가나다', [ADDRESS_HEADER]: '광주 북구 테스트로 1', 서명: '', 연락처: '000-0001' }),
  row(1, { 연번: 2, 이름: '라마바', [ADDRESS_HEADER]: '광주 북구 시험로 22', 서명: '', 연락처: '000-0002' }),
  row(2, { 연번: 7, 이름: '사아자', [ADDRESS_HEADER]: '', 서명: '', 연락처: '000-0003' }),
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

/** 만들어진 xlsx를 셀 값 2차원 배열로 되읽는다. */
async function readBack(blob: Blob, sheetName = '배송순서'): Promise<unknown[][]> {
  const wb = XLSX.read(await blob.arrayBuffer(), { type: 'array' });
  return XLSX.utils.sheet_to_json<unknown[]>(wb.Sheets[sheetName], {
    header: 1,
    blankrows: true,
    defval: null,
  });
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

describe('outputColumns', () => {
  it('연번·이름·주소만 남기고 나머지는 버린다', () => {
    expect(outputColumns(HEADERS, ADDRESS_HEADER)).toEqual(['연번', '이름', ADDRESS_HEADER]);
  });

  it('원본 열 순서를 지킨다', () => {
    const shuffled = ['이름', '서명', ADDRESS_HEADER, '연번'];
    expect(outputColumns(shuffled, ADDRESS_HEADER)).toEqual(['이름', ADDRESS_HEADER, '연번']);
  });

  it('이미 배송순서 열이 있으면 중복해서 싣지 않는다', () => {
    expect(outputColumns([ORDER_COLUMN, '이름'], null)).toEqual(['이름']);
  });

  it('아는 이름이 하나도 없으면 원본 열을 그대로 싣는다', () => {
    // 양식이 다른 파일에 빈 표를 주는 것보다 전부 싣는 편이 낫다.
    const foreign = ['Recipient', 'Street'];
    expect(outputColumns(foreign, null)).toEqual(foreign);
  });
});

describe('buildRecords', () => {
  const nodes = [node(0, 0), node(1, 1)];

  it('순번대로 정렬하고 순번 없는 행은 뒤에 남긴다', () => {
    const { records } = buildRecords({
      headers: HEADERS,
      rows: ROWS,
      nodes,
      finalOrder: [1, 0],
      addressColumn: ADDRESS_HEADER,
    });

    expect(records.map((r) => r.order)).toEqual([1, 2, null]);
    expect(records.map((r) => r.values['이름'])).toEqual(['라마바', '가나다', '사아자']);
  });

  it('좌표를 못 찾은 사람도 파일에 남는다', () => {
    // 여기서 버리면 그 사람이 배송 명단에서 조용히 사라진다.
    const { records } = buildRecords({
      headers: HEADERS,
      rows: ROWS,
      nodes,
      finalOrder: [0],
      addressColumn: ADDRESS_HEADER,
    });

    expect(records).toHaveLength(ROWS.length);
    expect(records.filter((r) => r.order === null).map((r) => r.values['이름'])).toEqual([
      '라마바',
      '사아자',
    ]);
  });

  it('버린 열의 값은 담지 않는다', () => {
    const { columns, records } = buildRecords({
      headers: HEADERS,
      rows: ROWS,
      nodes,
      finalOrder: [0],
      addressColumn: ADDRESS_HEADER,
    });

    expect(columns).not.toContain('연락처');
    expect(columns).not.toContain('서명');
    expect(Object.keys(records[0].values)).toEqual(['연번', '이름', ADDRESS_HEADER]);
  });
});

describe('buildXlsx', () => {
  const nodes = [node(0, 0), node(1, 1)];
  const input = {
    headers: HEADERS,
    rows: ROWS,
    nodes,
    finalOrder: [1, 0],
    addressColumn: ADDRESS_HEADER,
  };

  it('머리글은 배송순서 · 연번 · 이름 · 주소 네 열이다', async () => {
    const aoa = await readBack(await buildXlsx(input));

    expect(aoa[0]).toEqual([ORDER_COLUMN, '연번', '이름', ADDRESS_HEADER]);
  });

  it('순번은 숫자로 써서 엑셀에서 정렬·필터가 동작한다', async () => {
    const aoa = await readBack(await buildXlsx(input));

    expect(aoa[1][0]).toBe(1);
    expect(aoa[2][0]).toBe(2);
    expect(typeof aoa[1][0]).toBe('number');
  });

  it('순번 순으로 행이 놓이고 순번 없는 행은 맨 아래에 빈 칸으로 남는다', async () => {
    const aoa = await readBack(await buildXlsx(input));

    expect(aoa.slice(1).map((r) => r[2])).toEqual(['라마바', '가나다', '사아자']);
    expect(aoa[3][0]).toBeFalsy();
  });

  it('원본 행을 하나도 잃지 않는다', async () => {
    const aoa = await readBack(await buildXlsx(input));

    expect(aoa).toHaveLength(ROWS.length + 1);
  });

  it('시트 이름을 정할 수 있다', async () => {
    const aoa = await readBack(await buildXlsx(input, '결과'), '결과');

    expect(aoa[0]).toEqual([ORDER_COLUMN, '연번', '이름', ADDRESS_HEADER]);
  });

  it('한글이 깨지지 않는다', async () => {
    const aoa = await readBack(await buildXlsx(input));

    expect(aoa[1][3]).toBe('광주 북구 시험로 22');
  });
});
