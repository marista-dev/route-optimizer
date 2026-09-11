// @vitest-environment jsdom
/**
 * 세션 스토어의 무효화 규칙 테스트.
 *
 * 이 파일이 지키려는 것: 앞 단계 입력이 바뀌면 뒤 단계 산출물이 남아 있으면 안 된다.
 * (남으면 S4~S6가 존재하지 않는 그룹 id를 다루게 된다 — 리뷰 H6/M9.)
 */
import { beforeEach, describe, expect, it } from 'vitest';

import {
  SESSION_TTL_MS,
  isSessionExpired,
  mergePersistedSession,
  useSessionStore,
} from './session';
import { DEFAULT_THRESHOLD_M } from '../types';
import type { Cluster, Node, PrimaryGroup } from '../types';

const node = (id: number): Node => ({
  id,
  rowIndex: id,
  name: `n${id}`,
  address: `주소 ${id}`,
  lat: 35 + id / 1000,
  lon: 126 + id / 1000,
  kakaoAddr: '',
  reverseAddr: '',
  verdict: '일치',
});

const group = (id: number): PrimaryGroup => ({
  id,
  rep: id,
  members: [id],
  complexKey: `c${id}`,
  lat: 35 + id / 1000,
  lon: 126 + id / 1000,
});

const cluster = (id: number, groupIds: number[]): Cluster => ({
  id,
  groupIds,
  centroid: { lat: 35, lon: 126 },
  hull: [
    { lat: 35, lon: 126 },
    { lat: 35.001, lon: 126 },
    { lat: 35, lon: 126.001 },
  ],
});

/** 4단계까지 진행한 상태를 만든다. */
function seedProgress(): void {
  const store = useSessionStore.getState();
  store.setNodes([node(0), node(1)]);
  store.setClusters([group(0), group(1)], [cluster(0, [0]), cluster(1, [1])]);
  store.setClusterOrder([0, 1]);
  store.setClusterEntryExit(0, 0, 0);
  store.setClusterInnerOrder(0, [0], { '0-0': 12 }, 1, ['0-0']);
  useSessionStore.setState({ finalOrder: [0, 1] });
}

describe('세션 스토어 무효화', () => {
  beforeEach(() => {
    useSessionStore.getState().reset();
  });

  it('setNodes는 그룹·클러스터·순서·최종 순서를 모두 버린다', () => {
    seedProgress();
    expect(useSessionStore.getState().clusters).toHaveLength(2);

    useSessionStore.getState().setNodes([node(0)]);

    const state = useSessionStore.getState();
    expect(state.nodes).toHaveLength(1);
    expect(state.groups).toEqual([]);
    expect(state.clusters).toEqual([]);
    expect(state.clusterOrder).toEqual([]);
    expect(state.finalOrder).toEqual([]);
    expect(state.clusterPicks).toEqual({});
  });

  it('setClusterOrder는 모든 클러스터의 진입·이탈과 최종 순서를 지운다', () => {
    seedProgress();
    expect(useSessionStore.getState().clusterPicks[0]?.innerOrder).toEqual([0]);

    useSessionStore.getState().setClusterOrder([1, 0]);

    const state = useSessionStore.getState();
    expect(state.clusterOrder).toEqual([1, 0]);
    expect(state.clusterPicks).toEqual({});
    expect(state.finalOrder).toEqual([]);
  });

  it('setClusters는 순서·선택을 초기화한다', () => {
    seedProgress();
    useSessionStore.getState().setClusters([group(0)], [cluster(0, [0])]);

    const state = useSessionStore.getState();
    expect(state.clusterOrder).toEqual([]);
    expect(state.finalOrder).toEqual([]);
    expect(state.clusterPicks).toEqual({});
  });

  it('진입·이탈과 내부 순서를 저장해도 clusters 배열 정체성은 그대로다', () => {
    seedProgress();
    const before = useSessionStore.getState().clusters;

    useSessionStore.getState().setClusterEntryExit(1, 1, 1);
    useSessionStore.getState().setClusterInnerOrder(1, [1], { '1-1': 30 }, 0, []);

    const state = useSessionStore.getState();
    expect(state.clusters).toBe(before);
    expect(state.clusterPicks[1]).toEqual({
      entry: 1,
      exit: 1,
      innerOrder: [1],
      timeMatrix: { '1-1': 30 },
      haversineFallbacks: 0,
      haversineKeys: [],
    });
  });

  it('reset은 출발지만 남기고 모두 비운다', () => {
    useSessionStore.getState().setOrigin({ address: '광주 북구', lat: 35.1, lon: 126.9 });
    seedProgress();

    useSessionStore.getState().reset();

    const state = useSessionStore.getState();
    expect(state.origin?.address).toBe('광주 북구');
    expect(state.step).toBe(1);
    expect(state.rows).toEqual([]);
    expect(state.nodes).toEqual([]);
    expect(state.clusterPicks).toEqual({});
    expect(state.thresholdM).toBe(DEFAULT_THRESHOLD_M);
  });
});

describe('isSessionExpired', () => {
  const now = 1_800_000_000_000;

  it('저장한 적 없으면(0) 만료가 아니다', () => {
    expect(isSessionExpired(0, now)).toBe(false);
    expect(isSessionExpired(undefined, now)).toBe(false);
  });

  it('TTL 이내는 살리고 넘어서면 버린다', () => {
    expect(isSessionExpired(now - SESSION_TTL_MS + 1_000, now)).toBe(false);
    expect(isSessionExpired(now - SESSION_TTL_MS - 1_000, now)).toBe(true);
  });

  it('TTL은 7일이다', () => {
    expect(SESSION_TTL_MS).toBe(7 * 24 * 60 * 60 * 1000);
  });
});

describe('mergePersistedSession(복원 TTL)', () => {
  const now = 1_800_000_000_000;

  /** 복원 대상이 되는 초기 상태(액션까지 필요하지 않으므로 스토어에서 가져온다). */
  const freshState = () => {
    useSessionStore.getState().reset();
    return useSessionStore.getState();
  };

  it('TTL 이내면 저장본을 얹는다', () => {
    const current = freshState();
    const merged = mergePersistedSession(
      { fileName: '배송.csv', step: 4, savedAt: now - 1_000 },
      current,
      now,
    );
    expect(merged.fileName).toBe('배송.csv');
    expect(merged.step).toBe(4);
  });

  it('TTL이 지나면 저장본을 버리고 localStorage에서도 지운다', () => {
    localStorage.setItem('route-optimizer-session', '{"state":{}}');
    const current = freshState();
    const merged = mergePersistedSession(
      { fileName: '배송.csv', step: 4, rows: [{ rowIndex: 0 }], savedAt: now - SESSION_TTL_MS - 1 },
      current,
      now,
    );
    expect(merged.fileName).toBe('');
    expect(merged.step).toBe(1);
    expect(merged.rows).toEqual([]);
    expect(localStorage.getItem('route-optimizer-session')).toBeNull();
  });
});
