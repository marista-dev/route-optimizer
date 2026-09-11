import { describe, expect, it } from 'vitest';
import * as XLSX from 'xlsx';

import { parseUploadedFile } from './readFile';

/** 모든 데이터는 합성값이다. 실제 배송 데이터는 테스트에 넣지 않는다. */
const ADDRESS_HEADER = '택배받을 주소 (도로명)';

function makeXlsx(sheets: Array<{ name: string; aoa: unknown[][] }>): ArrayBuffer {
  const wb = XLSX.utils.book_new();
  for (const { name, aoa } of sheets) {
    XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(aoa), name);
  }
  return XLSX.write(wb, { bookType: 'xlsx', type: 'array' }) as ArrayBuffer;
}

function makeCsv(text: string, withBom = true): ArrayBuffer {
  const encoded = new TextEncoder().encode((withBom ? '\uFEFF' : '') + text);
  return encoded.buffer.slice(
    encoded.byteOffset,
    encoded.byteOffset + encoded.byteLength,
  ) as ArrayBuffer;
}

const SAMPLE_SHEETS = [
  { name: '안내', aoa: [['공지', '내용'], ['배송 안내', '샘플 시트']] },
  {
    name: '배송목록',
    aoa: [
      ['이름', ADDRESS_HEADER, '연락처'],
      ['가나다', '광주 북구 테스트로 1, 101동 202호', '000-0000-0001'],
      ['라마바', '  광주 북구 시험로 22  ', '000-0000-0002'],
      ['사아자', '', '000-0000-0003'],
      ['차카타', '광주 북구 예시로 3', '000-0000-0004'],
    ],
  },
];

describe('parseUploadedFile — xlsx', () => {
  it('주소 열이 있는 시트를 모든 시트에서 찾아낸다', async () => {
    const parsed = await parseUploadedFile(makeXlsx(SAMPLE_SHEETS), 'sample.xlsx');

    expect(parsed.sheetName).toBe('배송목록');
    expect(parsed.addressColumn).toBe(ADDRESS_HEADER);
    expect(parsed.headers).toEqual(['이름', ADDRESS_HEADER, '연락처']);
  });

  it('주소가 빈 행은 버리고 원본 행 번호는 그대로 둔다', async () => {
    const parsed = await parseUploadedFile(makeXlsx(SAMPLE_SHEETS), 'sample.xlsx');

    expect(parsed.rows.map((r) => r.rowIndex)).toEqual([0, 1, 3]);
    expect(parsed.nodesSeed).toEqual([
      { rowIndex: 0, name: '가나다', address: '광주 북구 테스트로 1, 101동 202호' },
      { rowIndex: 1, name: '라마바', address: '광주 북구 시험로 22' },
      { rowIndex: 3, name: '차카타', address: '광주 북구 예시로 3' },
    ]);
  });

  it('원본 열은 하나도 버리지 않는다', async () => {
    const parsed = await parseUploadedFile(makeXlsx(SAMPLE_SHEETS), 'sample.xlsx');

    expect(parsed.rows[0]).toMatchObject({
      이름: '가나다',
      연락처: '000-0000-0001',
      [ADDRESS_HEADER]: '광주 북구 테스트로 1, 101동 202호',
    });
  });

  it('주소 열이 어느 시트에도 없으면 오류를 던진다', async () => {
    const buffer = makeXlsx([{ name: '안내', aoa: [['공지'], ['내용']] }]);

    await expect(parseUploadedFile(buffer, 'sample.xlsx')).rejects.toThrow('택배받을 주소');
  });
});

describe('parseUploadedFile — csv', () => {
  const CSV = [
    `이름,${ADDRESS_HEADER},연락처`,
    '가나다,"광주 북구 테스트로 1, 101동 202호",000-0000-0001',
    '라마바,광주 북구 시험로 22,000-0000-0002',
    '사아자,,000-0000-0003',
    '',
  ].join('\n');

  it('UTF-8 BOM이 붙어도 헤더를 깨지 않고 읽는다', async () => {
    const parsed = await parseUploadedFile(makeCsv(CSV), 'sample.csv');

    expect(parsed.sheetName).toBeNull();
    expect(parsed.headers).toEqual(['이름', ADDRESS_HEADER, '연락처']);
    expect(parsed.addressColumn).toBe(ADDRESS_HEADER);
  });

  it('BOM이 없어도 같은 결과가 나온다', async () => {
    const parsed = await parseUploadedFile(makeCsv(CSV, false), 'sample.csv');
    expect(parsed.headers[0]).toBe('이름');
  });

  it('주소가 빈 행은 버리고 따옴표 안 쉼표는 유지한다', async () => {
    const parsed = await parseUploadedFile(makeCsv(CSV), 'sample.csv');

    expect(parsed.nodesSeed).toEqual([
      { rowIndex: 0, name: '가나다', address: '광주 북구 테스트로 1, 101동 202호' },
      { rowIndex: 1, name: '라마바', address: '광주 북구 시험로 22' },
    ]);
  });
});
