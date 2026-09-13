import type { RouteMode, Step } from '../types';

/**
 * 6단계 스테퍼 라벨(수동 모드 기준). 인덱스 + 1 = 단계 번호.
 * `StartScreen`이 "이어서 하기" 배너에 저장된 단계 이름을 보여줄 때도 그대로 쓴다.
 */
export const STEP_LABELS = [
  '시작',
  '주소검증',
  '클러스터링',
  '순서배정',
  '진입·이탈',
  '결과',
] as const;

/** 스테퍼가 실제로 그릴 항목 하나. */
export interface StepperItem {
  /** 이동할 실제 단계 번호(`Step` 타입은 모드와 무관하게 1~6 그대로다) */
  step: Step;
  label: string;
}

/**
 * 모드별 스테퍼 항목.
 *
 * `Step` 타입 자체는 바꾸지 않는다 — 자동 모드에서도 4번이 `AutoRouteScreen`을 그리고
 * 5번(진입·이탈)은 아예 방문하지 않을 뿐이다. 그래서 여기서는 항목 목록만 다르게
 * 돌려준다: 자동 모드는 4번 라벨을 "자동 계산"으로 바꾸고 5번을 뺀 5개 항목이다.
 */
export function stepperItems(mode: RouteMode): StepperItem[] {
  const items: StepperItem[] = STEP_LABELS.map((label, i) => ({ step: (i + 1) as Step, label }));
  if (mode !== 'auto') return items;
  return items
    .filter((item) => item.step !== 5)
    .map((item) => (item.step === 4 ? { ...item, label: '자동 계산' } : item));
}
