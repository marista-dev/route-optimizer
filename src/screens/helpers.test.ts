import { describe, expect, it } from 'vitest';

import {
  assembleFinalOrder,
  buildPostcodeAddress,
  clusterOfNode,
  columnLetter,
  emptyAddressCount,
  escapeHtml,
  fullPathPoints,
  groupBuildingLabel,
  groupPath,
  isFixable,
  makeOrderOf,
  makeTimeSec,
  moveItem,
  sameClustering,
  seedsFromRows,
  verdictCounts,
  withPicks,
} from './helpers';
import type { Cluster, Node, PrimaryGroup, Row, Verdict } from '../types';

function node(id: number, verdict: Verdict, hasCoords = true, address = `주소 ${id}`): Node {
  return {
    id,
    rowIndex: id,
    name: `이름${id}`,
    address,
    lat: hasCoords ? 35 + id / 1000 : null,
    lon: hasCoords ? 126 + id / 1000 : null,
    kakaoAddr: '',
    reverseAddr: '',
    verdict,
  };
}

function group(id: number, members: number[], lat: number, lon: number, key: string): PrimaryGroup {
  return { id, rep: members[0], members, complexKey: key, lat, lon };
}

describe('seedsFromRows', () => {
  it('이름·주소 열을 읽어 씨앗을 만든다', () => {
    const rows: Row[] = [
      { rowIndex: 0, 이름: ' 김OO ', '택배받을 주소': ' 서울 강서구 1 ' },
      { rowIndex: 1, '택배받을 주소': '서울 강서구 2' },
    ];
    expect(seedsFromRows(rows, '택배받을 주소')).toEqual([
      { rowIndex: 0, name: '김OO', address: '서울 강서구 1' },
      { rowIndex: 1, name: '', address: '서울 강서구 2' },
    ]);
  });

  it('주소 열이 없으면 빈 배열', () => {
    expect(seedsFromRows([{ rowIndex: 0 }], null)).toEqual([]);
  });
});

describe('verdictCounts', () => {
  const nodes = [
    node(0, '일치'),
    node(1, '수정됨'),
    node(2, '요확인'),
    node(3, '확인불가'),
    node(4, '위치없음', false),
  ];

  it('판정별 건수를 센다', () => {
    expect(verdictCounts(nodes)).toEqual({
      전체: 5,
      일치: 1,
      요확인: 1,
      확인불가: 1,
      위치없음: 1,
      수정됨: 1,
    });
  });

  it('수정 가능한 판정만 true', () => {
    expect(['요확인', '확인불가', '위치없음'].every((v) => isFixable(v as Verdict))).toBe(true);
    expect(isFixable('일치')).toBe(false);
    expect(isFixable('수정됨')).toBe(false);
  });
});

describe('makeTimeSec', () => {
  const groups = [group(0, [0], 35.0, 126.0, 'a'), group(1, [1], 35.1, 126.0, 'b')];

  it('저장된 값을 그대로 돌려준다', () => {
    expect(makeTimeSec({ '0-1': 123 }, groups)(0, 1)).toBe(123);
  });

  it('빠진 칸은 Haversine 추정치로 메운다', () => {
    const sec = makeTimeSec({}, groups)(0, 1);
    expect(sec).toBeGreaterThan(0);
    expect(sec).toBeLessThan(3600);
  });

  it('모르는 그룹이면 0', () => {
    expect(makeTimeSec({}, groups)(0, 99)).toBe(0);
  });
});

describe('assembleFinalOrder', () => {
  // 클러스터 0: 그룹 0(노드 0,1) + 그룹 1(노드 2) — 다른 단지라 블록 2개
  // 클러스터 1: 그룹 2(노드 3)
  const nodes = [
    node(0, '일치', true, '가로 1 101동 202호'),
    node(1, '일치', true, '가로 1 101동 101호'),
    node(2, '일치', true, '나로 2'),
    node(3, '일치', true, '다로 3'),
  ];
  const groups = [
    group(0, [0, 1], 35.0, 126.0, '가로 1'),
    group(1, [2], 35.001, 126.0, '나로 2'),
    group(2, [3], 35.2, 126.2, '다로 3'),
  ];
  const clusters: Cluster[] = [
    {
      id: 0,
      groupIds: [0, 1],
      centroid: { lat: 35.0005, lon: 126.0 },
      hull: [],
      entry: 0,
      exit: 1,
      timeMatrix: { '0-1': 60, '1-0': 60 },
    },
    { id: 1, groupIds: [2], centroid: { lat: 35.2, lon: 126.2 }, hull: [], entry: 2, exit: 2 },
  ];

  it('방문 순서대로 클러스터 내부 순서를 이어 붙인다', () => {
    expect(assembleFinalOrder(clusters, [0, 1], groups, nodes)).toEqual([1, 0, 2, 3]);
  });

  it('클러스터 순서를 뒤집으면 결과도 뒤집힌다', () => {
    expect(assembleFinalOrder(clusters, [1, 0], groups, nodes)).toEqual([3, 1, 0, 2]);
  });

  it('진입·이탈이 없는 클러스터는 건너뛴다', () => {
    const pending = clusters.map((c) => (c.id === 1 ? { ...c, entry: undefined, exit: undefined } : c));
    expect(assembleFinalOrder(pending, [0, 1], groups, nodes)).toEqual([1, 0, 2]);
  });

  it('순서가 비어 있으면 빈 배열', () => {
    expect(assembleFinalOrder(clusters, [], groups, nodes)).toEqual([]);
  });
});

describe('지도용 좌표 변환', () => {
  const nodes = [node(0, '일치'), node(1, '위치없음', false), node(2, '일치')];

  it('출발지를 맨 앞에 두고 좌표 없는 노드는 건너뛴다', () => {
    const path = fullPathPoints({ address: 'x', lat: 1, lon: 2 }, [0, 1, 2], nodes);
    expect(path).toHaveLength(3);
    expect(path[0]).toEqual({ lat: 1, lon: 2 });
  });

  it('출발지가 없으면 노드만', () => {
    expect(fullPathPoints(null, [0, 2], nodes)).toHaveLength(2);
  });

  it('그룹 순서열을 좌표열로 바꾼다', () => {
    const groups = [group(0, [0], 35, 126, 'a'), group(1, [2], 36, 127, 'b')];
    expect(groupPath([1, 0], groups)).toEqual([
      { lat: 36, lon: 127 },
      { lat: 35, lon: 126 },
    ]);
    expect(groupPath(undefined, groups)).toEqual([]);
  });

  it('노드 → 클러스터 매핑', () => {
    const groups = [group(0, [0, 1], 35, 126, 'a'), group(1, [2], 36, 127, 'b')];
    const clusters: Cluster[] = [
      { id: 7, groupIds: [0], centroid: { lat: 35, lon: 126 }, hull: [] },
      { id: 8, groupIds: [1], centroid: { lat: 36, lon: 127 }, hull: [] },
    ];
    const map = clusterOfNode(groups, clusters);
    expect(map.get(0)).toBe(7);
    expect(map.get(1)).toBe(7);
    expect(map.get(2)).toBe(8);
  });
});

describe('buildPostcodeAddress', () => {
  it('도로명 + 법정동 + 아파트명', () => {
    expect(
      buildPostcodeAddress({
        userSelectedType: 'R',
        roadAddress: '서울 강서구 공항대로 247',
        bname: '마곡동',
        buildingName: '마곡퀸즈파크나인',
        apartment: 'Y',
      }),
    ).toBe('서울 강서구 공항대로 247 (마곡동, 마곡퀸즈파크나인)');
  });

  it('아파트가 아니면 건물명을 붙이지 않는다', () => {
    expect(
      buildPostcodeAddress({
        userSelectedType: 'R',
        roadAddress: '서울 강서구 공항대로 247',
        bname: '마곡동',
        buildingName: '상가',
        apartment: 'N',
      }),
    ).toBe('서울 강서구 공항대로 247 (마곡동)');
  });

  it('동/로/가로 끝나지 않는 법정동은 무시한다', () => {
    expect(
      buildPostcodeAddress({
        userSelectedType: 'R',
        roadAddress: '서울 강서구 공항대로 247',
        bname: '마곡제1',
        buildingName: '',
        apartment: 'N',
      }),
    ).toBe('서울 강서구 공항대로 247');
  });

  it('지번 선택이면 괄호를 붙이지 않는다', () => {
    expect(
      buildPostcodeAddress({
        userSelectedType: 'J',
        roadAddress: '서울 강서구 공항대로 247',
        jibunAddress: '서울 강서구 마곡동 727',
        bname: '마곡동',
        apartment: 'Y',
        buildingName: '아파트',
      }),
    ).toBe('서울 강서구 마곡동 727');
  });
});

describe('escapeHtml', () => {
  it('인포윈도우에 넣기 전에 꺾쇠를 막는다', () => {
    expect(escapeHtml('<b>"김&이"</b>')).toBe('&lt;b&gt;&quot;김&amp;이&quot;&lt;/b&gt;');
  });

  it('작은따옴표도 막는다', () => {
    expect(escapeHtml("onclick='x'")).toBe('onclick=&#39;x&#39;');
  });
});

describe('withPicks', () => {
  const base: Cluster[] = [
    { id: 0, groupIds: [0], centroid: { lat: 35, lon: 126 }, hull: [] },
    { id: 1, groupIds: [1], centroid: { lat: 35, lon: 126 }, hull: [] },
  ];

  it('선택이 있는 클러스터에만 값을 얹는다', () => {
    const merged = withPicks(base, { 1: { entry: 1, exit: 1, innerOrder: [1] } });
    expect(merged[0]).toBe(base[0]);
    expect(merged[1]).toMatchObject({ id: 1, entry: 1, exit: 1, innerOrder: [1] });
    expect(base[1].entry).toBeUndefined();
  });
});

describe('moveItem', () => {
  it('from에서 to로 옮긴 새 배열을 돌려준다', () => {
    expect(moveItem([1, 2, 3, 4], 0, 2)).toEqual([2, 3, 1, 4]);
    expect(moveItem([1, 2, 3, 4], 3, 0)).toEqual([4, 1, 2, 3]);
  });

  it('원본 배열을 바꾸지 않는다', () => {
    const list = [1, 2, 3];
    moveItem(list, 0, 2);
    expect(list).toEqual([1, 2, 3]);
  });

  it('제자리로 옮기면 원본을 그대로 돌려준다', () => {
    const list = [1, 2, 3];
    expect(moveItem(list, 1, 1)).toBe(list);
  });

  it('범위를 벗어나면 원본을 그대로 돌려준다', () => {
    const list = [1, 2, 3];
    expect(moveItem(list, 0, -1)).toBe(list);
    expect(moveItem(list, 0, 3)).toBe(list);
  });
});

describe('makeOrderOf', () => {
  it('클러스터 id → 1부터 시작하는 방문 순번', () => {
    const orderOf = makeOrderOf([5, 2, 8]);
    expect(orderOf(5)).toBe(1);
    expect(orderOf(2)).toBe(2);
    expect(orderOf(8)).toBe(3);
  });

  it('순서에 없는 id는 undefined', () => {
    expect(makeOrderOf([5, 2])(99)).toBeUndefined();
  });
});

describe('columnLetter', () => {
  it('0-based 번호를 엑셀 열 문자로 바꾼다', () => {
    expect(columnLetter(0)).toBe('A');
    expect(columnLetter(4)).toBe('E');
    expect(columnLetter(25)).toBe('Z');
    expect(columnLetter(26)).toBe('AA');
    expect(columnLetter(27)).toBe('AB');
  });

  it('열을 못 찾았을 때(-1)는 빈 문자열', () => {
    expect(columnLetter(-1)).toBe('');
  });
});

describe('emptyAddressCount', () => {
  it('원본 행 번호와 남은 행 수의 차이로 빈 주소 수를 센다', () => {
    expect(emptyAddressCount([])).toBe(0);
    expect(emptyAddressCount([{ rowIndex: 0 }, { rowIndex: 1 }])).toBe(0);
    expect(emptyAddressCount([{ rowIndex: 0 }, { rowIndex: 3 }])).toBe(2);
  });
});


describe('groupBuildingLabel', () => {
  it('멤버가 하나면 그 건물 이름', () => {
    expect(groupBuildingLabel(['광주 북구 삼정로 7, 207동 403호(두암동, 주공2단지@)']))
      .toBe('주공2단지@ 207동');
  });

  it('같은 동의 여러 호수는 한 이름으로', () => {
    expect(
      groupBuildingLabel([
        '광주 북구 매곡로 92, 101동 105호(매곡동, 아남@)',
        '광주 북구 매곡로 92, 101동 1205',
      ]),
    ).toBe('아남@ 101동');
  });

  it('동이 갈리면 단지 이름만', () => {
    expect(
      groupBuildingLabel([
        '광주 북구 매곡로 92, 102동 106호(매곡동,아남@)',
        '광주 북구 매곡로 92, 제201동 제1504호 (매곡동, 아남@)',
      ]),
    ).toBe('아남@');
  });

  it('빈 주소는 무시하고, 전부 비면 빈 문자열', () => {
    expect(groupBuildingLabel(['', '광주 북구 매곡로 92'])).toBe('광주 북구 매곡로 92');
    expect(groupBuildingLabel([])).toBe('');
    expect(groupBuildingLabel(['', '  '])).toBe('');
  });
});

describe('sameClustering — 값이 같으면 다시 쓰지 않는다(NEW-2)', () => {
  const groups = [
    { id: 0, rep: 1, members: [1, 2], complexKey: 'a', lat: 35.1, lon: 126.9 },
    { id: 1, rep: 3, members: [3], complexKey: 'b', lat: 35.2, lon: 126.8 },
  ];
  const clusters = [
    { id: 0, groupIds: [0, 1], centroid: { lat: 35.15, lon: 126.85 }, hull: [] },
  ];

  it('같은 구성이면 true', () => {
    expect(sameClustering(groups, clusters, structuredClone(groups), structuredClone(clusters))).toBe(true);
  });

  it('그룹 멤버가 달라지면 false', () => {
    const changed = structuredClone(groups);
    changed[0].members = [1];
    expect(sameClustering(groups, clusters, changed, clusters)).toBe(false);
  });

  it('클러스터 묶음이 달라지면 false', () => {
    const changed = [
      { id: 0, groupIds: [0], centroid: { lat: 35.1, lon: 126.9 }, hull: [] },
      { id: 1, groupIds: [1], centroid: { lat: 35.2, lon: 126.8 }, hull: [] },
    ];
    expect(sameClustering(groups, clusters, groups, changed)).toBe(false);
  });

  it('저장된 값이 비어 있으면 false — 첫 진입에서는 반드시 쓴다', () => {
    expect(sameClustering([], [], groups, clusters)).toBe(false);
  });
});
