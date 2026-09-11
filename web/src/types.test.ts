import { describe, expect, it } from 'vitest';

import { DEFAULT_THRESHOLD_M } from './types';

describe('types', () => {
  it('기본 클러스터 임계값은 400m다', () => {
    expect(DEFAULT_THRESHOLD_M).toBe(400);
  });
});
