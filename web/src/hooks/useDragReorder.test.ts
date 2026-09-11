import { describe, expect, it } from 'vitest';

import { targetIndex } from './useDragReorder';

describe('targetIndex — 삽입 인덱스를 이동 대상으로 바꾼다', () => {
  it('자기보다 뒤에 꽂으면 한 칸 당긴다', () => {
    // [A,B,C,D]에서 A(0)를 C와 D 사이(3)에 꽂으면 결과는 [B,C,A,D] → to = 2
    expect(targetIndex(3, 0)).toBe(2);
  });

  it('자기보다 앞에 꽂으면 그대로', () => {
    // [A,B,C,D]에서 D(3)를 A와 B 사이(1)에 꽂으면 to = 1
    expect(targetIndex(1, 3)).toBe(1);
  });

  it('맨 끝으로 보내기', () => {
    expect(targetIndex(4, 1)).toBe(3);
  });

  it('맨 앞으로 보내기', () => {
    expect(targetIndex(0, 2)).toBe(0);
  });

  it('제자리면 null — 호출부가 아무것도 하지 않는다', () => {
    expect(targetIndex(1, 1)).toBeNull(); // 자기 앞
    expect(targetIndex(2, 1)).toBeNull(); // 자기 뒤
  });
});
