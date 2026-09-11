import { useCallback, useMemo, useRef, useState } from 'react';
import { ChevronDown, ChevronUp, Download, GripVertical } from 'lucide-react';

import { DataTable, MapPan, SidePanel } from '../components';
import { useDragReorder } from '../hooks/useDragReorder';
import type { Column } from '../components';
import { MapCanvas, MarkerLayer, RouteLayer } from '../map';
import {
  buildCsv,
  buildOrderMap,
  buildXlsx,
  buildXlsxFromRecords,
  downloadBlob,
  outputFileName,
  parseUploadedFile,
} from '../io';
import { useSessionStore } from '../store/session';
import { showError, showInfo } from '../store/toast';
import { getOriginalBuffer, useVolatileStore } from '../store/volatile';
import type { LatLng, Node } from '../types';
import {
  assembleFinalOrder,
  computeWarnCount,
  fullPathPoints,
  groupBuildingLabel,
  groupOfNode,
  moveItem,
  reuploadMismatch,
  showApiError,
  withPicks,
} from './helpers';

interface ResultRow {
  no: number;
  node: Node;
  /** 소속 1차 그룹 id(= 지도 마커 1개). 좌표가 없으면 undefined */
  groupId: number | undefined;
  /** 건물(단지) 이름 — 지도 라벨·툴팁과 같은 문구 */
  building: string;
}

/** S6 결과 — 전체 경로, 표, 순서 편집, CSV·xlsx 다운로드. */
export function ResultScreen() {
  const origin = useSessionStore((s) => s.origin);
  const fileName = useSessionStore((s) => s.fileName);
  const sheetName = useSessionStore((s) => s.sheetName);
  const headers = useSessionStore((s) => s.headers);
  const rows = useSessionStore((s) => s.rows);
  const nodes = useSessionStore((s) => s.nodes);
  const groups = useSessionStore((s) => s.groups);
  const clusters = useSessionStore((s) => s.clusters);
  const clusterOrder = useSessionStore((s) => s.clusterOrder);
  const clusterPicks = useSessionStore((s) => s.clusterPicks);
  const finalOrder = useSessionStore((s) => s.finalOrder);
  const setFinalOrder = useSessionStore((s) => s.setFinalOrder);
  const reset = useSessionStore((s) => s.reset);

  const hasBuffer = useVolatileStore((s) => s.hasBuffer);
  const setBuffer = useVolatileStore((s) => s.setBuffer);
  const reuploadRef = useRef<HTMLInputElement>(null);
  // 개인정보가 담긴 세션은 빨리 비우는 게 좋지만(계획 12절), 대부분 CSV와 xlsx를
  // 둘 다 받는다. 기본으로 켜 두면 첫 다운로드가 두 번째를 불가능하게 만든다 →

  // ── 순서 편집 ──────────────────────────────────────────────────────────────
  // 편집 중에는 draft가 화면(표·지도·순번)의 기준이고, "적용"을 눌러야 스토어에 들어간다.
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState<number[]>([]);
  const order = editing ? draft : finalOrder;

  // ── 표 ↔ 지도 연동 ────────────────────────────────────────────────────────
  const [hoverNodeId, setHoverNodeId] = useState<number | null>(null);
  // 클릭으로 고른 지점. hover는 마우스가 떠나면 풀리지만 이건 남아 있어야
  // "지도에서 누른 배송지"를 표에서 계속 찾아볼 수 있다.
  const [selectedNodeId, setSelectedNodeId] = useState<number | null>(null);
  /** 행 클릭 시 지도를 옮길 지점. 같은 행을 다시 눌러도 움직이도록 매번 새 객체를 넣는다 */
  const [panTarget, setPanTarget] = useState<LatLng | null>(null);

  const nodeById = useMemo(() => new Map(nodes.map((n) => [n.id, n])), [nodes]);
  const groupIdOfNode = useMemo(() => groupOfNode(groups), [groups]);
  const warnCount = useMemo(() => computeWarnCount(nodes), [nodes]);
  const totalCalls = useMemo(
    () =>
      Object.values(clusterPicks).reduce(
        (n, p) => n + Object.keys(p.timeMatrix ?? {}).length,
        0,
      ),
    [clusterPicks],
  );

  /** 그룹 id → 건물(단지) 이름. 사람 이름 대신 지도에 붙는 이름이다 */
  const buildingOfGroup = useMemo(() => {
    const map = new Map<number, string>();
    for (const group of groups) {
      map.set(
        group.id,
        groupBuildingLabel(group.members.map((id) => nodeById.get(id)?.address ?? '')),
      );
    }
    return map;
  }, [groups, nodeById]);

  const resultRows = useMemo<ResultRow[]>(
    () =>
      order.flatMap((id, i) => {
        const node = nodeById.get(id);
        if (!node) return [];
        const groupId = groupIdOfNode.get(id);
        return [
          {
            no: i + 1,
            node,
            groupId,
            building:
              (groupId === undefined ? '' : buildingOfGroup.get(groupId)) || node.address,
          },
        ];
      }),
    [order, nodeById, groupIdOfNode, buildingOfGroup],
  );

  const fullPath = useMemo<LatLng[][]>(() => {
    const points = fullPathPoints(origin, order, nodes);
    return points.length >= 2 ? [points] : [];
  }, [origin, order, nodes]);

  /** 1차 그룹 마커에 붙일 배송 순번 = 그 그룹 멤버 중 가장 빠른 순번. */
  const labelByGroup = useMemo(() => {
    const map = new Map<number, number>();
    order.forEach((nodeId, i) => {
      const groupId = groupIdOfNode.get(nodeId);
      if (groupId === undefined || map.has(groupId)) return;
      map.set(groupId, i + 1);
    });
    return map;
  }, [order, groupIdOfNode]);

  /** 그룹 id → 그 그룹에서 가장 먼저 배송하는 노드. 마커 hover를 표 행으로 옮길 때 쓴다 */
  const firstNodeOfGroup = useMemo(() => {
    const map = new Map<number, number>();
    for (const nodeId of order) {
      const groupId = groupIdOfNode.get(nodeId);
      if (groupId === undefined || map.has(groupId)) continue;
      map.set(groupId, nodeId);
    }
    return map;
  }, [order, groupIdOfNode]);

  const orderLabel = useCallback(
    (groupId: number) => {
      const n = labelByGroup.get(groupId);
      return n === undefined ? undefined : String(n);
    },
    [labelByGroup],
  );

  const tooltipOf = useCallback(
    (groupId: number) => buildingOfGroup.get(groupId),
    [buildingOfGroup],
  );

  const activeNodeId = hoverNodeId ?? selectedNodeId;
  const hoveredRow = useMemo(
    () => resultRows.find((r) => r.node.id === activeNodeId) ?? null,
    [resultRows, activeNodeId],
  );

  const onMarkerHover = useCallback(
    (groupId: number | null) => {
      setHoverNodeId(groupId === null ? null : (firstNodeOfGroup.get(groupId) ?? null));
    },
    [firstNodeOfGroup],
  );

  const onRowHover = useCallback((row: ResultRow | null) => {
    setHoverNodeId(row?.node.id ?? null);
  }, []);

  const onRowClick = useCallback((row: ResultRow) => {
    setSelectedNodeId(row.node.id);
    if (row.node.lat === null || row.node.lon === null) return;
    setPanTarget({ lat: row.node.lat, lon: row.node.lon });
  }, []);

  // 지도에서 순번을 누르면 표에서 그 행을 찾아 보여 준다.
  // 같은 마커를 다시 눌러도 스크롤되도록 매번 새 객체를 만든다.
  const [scrollTarget, setScrollTarget] = useState<{ id: number } | null>(null);
  const onMarkerClick = useCallback(
    (groupId: number) => {
      const nodeId = firstNodeOfGroup.get(groupId);
      if (nodeId === undefined) return;
      setSelectedNodeId(nodeId);
      setScrollTarget({ id: nodeId });
    },
    [firstNodeOfGroup],
  );

  /** 편집 중 행 이동. `to`가 범위를 벗어나면 아무 일도 하지 않는다 */
  const moveRow = useCallback((from: number, to: number) => {
    setDraft((prev) => moveItem(prev, from, to));
  }, []);

  // 표 행 드래그 재정렬. S4 순서 목록과 같은 훅을 써서 조작감이 같다
  // (가장자리 자동 스크롤 + 삽입 위치 표시).
  const { listRef, dragFrom, dropAt, onItemDragStart, onDragEnd } = useDragReorder(moveRow);

  const startEdit = () => {
    setDraft(finalOrder.slice());
    setEditing(true);
  };

  const cancelEdit = () => {
    setEditing(false);
    setDraft([]);
  };

  /** 계산된 순서(진입·이탈 + 도로시간으로 푼 결과)로 되돌린다. */
  const revertToComputed = () => {
    setDraft(
      assembleFinalOrder(withPicks(clusters, clusterPicks), clusterOrder, groups, nodes),
    );
  };

  const applyEdit = () => {
    setFinalOrder(draft);
    setEditing(false);
    setDraft([]);
    showInfo('편집한 순서를 적용했습니다. 다운로드에도 이 순서가 들어갑니다.');
  };

  /** 세션 비우기("처음부터"). */
  const clearSession = useCallback(() => {
    setEditing(false);
    setDraft([]);
    setHoverNodeId(null);
    reset();
    setBuffer(null);
  }, [reset, setBuffer]);

  /** blob 만들기 → 내려받기. 만들기가 실패하면 fail 메시지만 보여준다. */
  const download = (make: () => Blob, ext: 'csv' | 'xlsx', fail: string) => {
    try {
      downloadBlob(make(), outputFileName(fileName, ext));
    } catch (err) {
      showApiError(err, fail);
    }
  };

  const downloadCsv = () =>
    download(() => buildCsv({ headers, rows, nodes, finalOrder }), 'csv', 'CSV를 만들지 못했습니다.');

  const downloadXlsx = () => {
    // 원본 바이트가 있으면 원본 워크북에 '배송순서' 열만 끼워 넣는다(열·시트 그대로).
    // 새로고침 등으로 사라졌으면 결과만으로 새 워크북을 만든다 — 원본 열은 전부 살리고
    // 서식만 잃는다. 예전처럼 버튼을 잠가 xlsx를 아예 못 받게 하지는 않는다.
    const buffer = getOriginalBuffer();
    download(
      () =>
        buffer
          ? buildXlsx(buffer, sheetName, buildOrderMap(nodes, finalOrder))
          : buildXlsxFromRecords({ headers, rows, nodes, finalOrder }),
      'xlsx',
      'xlsx를 만들지 못했습니다.',
    );
  };

  /**
   * 새로고침으로 사라진 원본 바이트를 다시 채운다.
   * 엉뚱한 파일을 받으면 배송순서가 관계없는 행에 적히므로
   * 파일명·헤더(이름과 순서)·행 수가 모두 같을 때만 받아들인다.
   */
  const onReupload = async (file: File) => {
    try {
      const parsed = await parseUploadedFile(file, file.name);
      const reason = reuploadMismatch(
        { fileName, headers, rowCount: rows.length },
        { fileName: file.name, headers: parsed.headers, rowCount: parsed.rows.length },
      );
      if (reason) {
        showError(`세션의 원본과 다른 파일입니다. ${reason}`);
        return;
      }
      setBuffer(parsed.originalBuffer);
      showInfo('원본 파일을 확인했습니다. 이제 xlsx도 받을 수 있습니다.');
    } catch (err) {
      showApiError(err, '파일을 읽지 못했습니다.');
    }
  };

  const columns = useMemo<Column<ResultRow>[]>(() => {
    const base: Column<ResultRow>[] = [
      {
        key: 'no',
        header: '순서',
        width: 56,
        className: 'ro-num',
        cell: (r) => <b>{r.no}</b>,
      },
      { key: 'name', header: '이름', width: 84, cell: (r) => r.node.name },
      { key: 'addr', header: '주소', className: 'ro-muted', cell: (r) => r.node.address },
    ];
    if (!editing) return base;
    return [
      // 끌 수 있다는 표시는 줄이 시작하는 자리에 있어야 한다. 순서·이름 뒤에 두면
      // 무엇을 잡으라는 표시인지 눈에 들어오지 않는다.
      {
        key: 'grip',
        header: '',
        width: 22,
        className: 'ro-td--grip',
        cell: () => (
          <span className="ro-orderitem__handle" aria-hidden>
            <GripVertical size={16} />
          </span>
        ),
      },
      ...base,
      {
        key: 'move',
        header: '이동',
        width: 46,
        right: true,
        cell: (_r, i) => (
          <span className="ro-move">
            <button
              type="button"
              className="ro-orderitem__move"
              aria-label={`${i + 1}번째 행 위로`}
              disabled={i === 0}
              onClick={(e) => {
                e.stopPropagation();
                moveRow(i, i - 1);
              }}
            >
              <ChevronUp size={15} />
            </button>
            <button
              type="button"
              className="ro-orderitem__move"
              aria-label={`${i + 1}번째 행 아래로`}
              disabled={i === draft.length - 1}
              onClick={(e) => {
                e.stopPropagation();
                moveRow(i, i + 1);
              }}
            >
              <ChevronDown size={15} />
            </button>
          </span>
        ),
      },
    ];
  }, [editing, draft.length, moveRow]);

  return (
    <div className="ro-mapscreen">
      <div className="ro-mapscreen__canvas">
        <MapCanvas center={origin ?? undefined}>
          <RouteLayer paths={fullPath} style="solid" />
          <MarkerLayer
            groups={groups}
            origin={origin}
            orderLabel={orderLabel}
            tooltipOf={tooltipOf}
            highlightGroupId={hoveredRow?.groupId}
            highlightText={hoveredRow ? `${hoveredRow.no} · ${hoveredRow.building}` : undefined}
            onGroupHover={onMarkerHover}
            onGroupClick={onMarkerClick}
          />
          <MapPan target={panTarget} />
        </MapCanvas>
      </div>

      <SidePanel
        className="ro-s6"
        title="배송 순서 결과"
        note={`${resultRows.length}건 · 클러스터 ${clusters.length}개 · 도로시간 ${totalCalls}회`}
        bodyClassName="ro-panel__scroll"
        bodyRef={listRef}
        footer={
          <div className="ro-s6__foot">
            <button
              type="button"
              className="ro-btn ro-btn--sm ro-btn--danger"
              onClick={clearSession}
            >
              처음부터 (세션 삭제)
            </button>
          </div>
        }
        extraHead={
          <>
            <div className="ro-s6__downloads">
              {/* 두 형식은 우열이 없다. 한쪽만 칠하면 다른 쪽이 비활성처럼 보여 둘 다 같은 무게로 둔다. */}
              <button
                type="button"
                className="ro-btn ro-btn--dl"
                disabled={editing || finalOrder.length === 0}
                onClick={downloadCsv}
              >
                <Download size={16} />
                CSV
              </button>
              <button
                type="button"
                className="ro-btn ro-btn--dl"
                disabled={editing || finalOrder.length === 0}
                onClick={downloadXlsx}
              >
                <Download size={16} />
                엑셀 (xlsx)
              </button>
            </div>
          {/*
            편집 중에는 안내 한 줄 + 버튼 한 줄로 쌓는다. 520px 패널에서 긴 안내와 버튼
            셋을 한 줄에 나란히 두면 서로 밀려 줄바꿈이 생긴다.
            버튼은 위 다운로드 줄과 같이 균등 폭으로 맞춰 세로선이 흐트러지지 않게 한다.
          */}
          <div className="ro-s6__edit">
            {editing ? (
              <>
                <span className="ro-s6__edithint">끌거나 위·아래 버튼으로 순서를 바꿉니다</span>
                <div className="ro-s6__editbtns">
                  <button type="button" className="ro-btn ro-btn--md" onClick={revertToComputed}>
                    되돌리기
                  </button>
                  <button type="button" className="ro-btn ro-btn--md" onClick={cancelEdit}>
                    취소
                  </button>
                  <button
                    type="button"
                    className="ro-btn ro-btn--md ro-btn--primary"
                    onClick={applyEdit}
                  >
                    적용
                  </button>
                </div>
              </>
            ) : (
              <button
                type="button"
                className="ro-btn ro-btn--md"
                disabled={finalOrder.length === 0}
                onClick={startEdit}
              >
                순서 편집
              </button>
            )}
          </div>
          <div className="ro-s6__notes">
            {warnCount > 0 ? (
              <div className="ro-warn-text">
                · 주소가 다른 채로 둔 {warnCount}건 포함
              </div>
            ) : null}
            {!hasBuffer ? (
              <button
                type="button"
                className="ro-btn ro-btn--sm"
                title="원본 파일을 다시 올리면 xlsx가 원본 서식을 그대로 씁니다"
                onClick={() => reuploadRef.current?.click()}
              >
                원본 다시 올리기
              </button>
            ) : null}
            </div>
          </>
        }
      >
        <DataTable
          columns={columns}
          rows={resultRows}
          rowKey={(r) => r.node.id}
          reorderable={editing}
          onRowDragStart={onItemDragStart}
          onRowDragEnd={onDragEnd}
          dragFrom={dragFrom}
          dropAt={dropAt}
          rowClassName={(r, i) =>
            [
              i % 2 === 1 ? 'is-odd' : null,
              r.node.id === hoverNodeId ? 'is-hover' : null,
              r.node.id === selectedNodeId ? 'is-selected' : null,
            ]
              .filter(Boolean)
              .join(' ') || undefined
          }
          onRowHover={onRowHover}
          onRowClick={onRowClick}
          scrollToKey={scrollTarget?.id ?? null}
          empty="아직 최종 순서가 없습니다"
        />
      </SidePanel>

      <input
        ref={reuploadRef}
        type="file"
        accept=".xlsx,.xls,.csv"
        hidden
        onChange={(e) => {
          const file = e.target.files?.[0];
          if (file) void onReupload(file);
          e.target.value = '';
        }}
      />
    </div>
  );
}
