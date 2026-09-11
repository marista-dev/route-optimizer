import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { ArrowRight } from 'lucide-react';

import {
  KakaoAuthError,
  geocode,
  isPoolAborted,
  probeRestKey,
  reverseGeocode,
  runPool,
} from '../api';
import { verifyAddress } from '../core';
import { DataTable, PostcodeModal, ProgressBar, VerdictBadge } from '../components';
import type { Column } from '../components';
import { kakaoHeaders, useSessionStore } from '../store/session';
import { showError } from '../store/toast';
import { useVolatileStore } from '../store/volatile';
import { VERDICT_HELP, VERDICT_LABEL, type Node, type Verdict } from '../types';
import {
  ALL_VERDICTS,
  VERDICTS,
  isFixable,
  seedsFromRows,
  showApiError,
  verdictCounts,
} from './helpers';

/** 동시 호출 수 — 데스크톱판 `ThreadPoolExecutor(max_workers=3)`와 같다. */
const CONCURRENCY = 3;

/**
 * "다시 시작"으로 재조회할 때 확인을 받는 기준 건수.
 * 지오코딩 + 역지오코딩을 다시 태우므로(= 건수 × 2 콜), 유료 쿼터를 실수로
 * 많이 소모할 수 있는 규모부터만 확인한다. 적은 건수는 매번 물으면 성가시기만 하다.
 */
const RESTART_CONFIRM_THRESHOLD = 50;

/** S2 지오코딩 · 검증. */
export function GeocodeScreen() {
  const rows = useSessionStore((s) => s.rows);
  const addressColumn = useSessionStore((s) => s.addressColumn);
  const nodes = useSessionStore((s) => s.nodes);
  const clusterOrder = useSessionStore((s) => s.clusterOrder);
  const finalOrder = useSessionStore((s) => s.finalOrder);
  const clusterPicks = useSessionStore((s) => s.clusterPicks);
  const setNodes = useSessionStore((s) => s.setNodes);
  const setStep = useSessionStore((s) => s.setStep);
  const clearRestKey = useVolatileStore((s) => s.clearRestKey);

  const controllerRef = useRef<AbortController | null>(null);
  const [running, setRunning] = useState(false);
  const [geoDone, setGeoDone] = useState(0);
  const [revDone, setRevDone] = useState(0);
  const [filter, setFilter] = useState<Verdict | typeof ALL_VERDICTS>(ALL_VERDICTS);
  const [fixing, setFixing] = useState<Node | null>(null);
  // 수정 중인 노드 id. 왕복이 두 번이라 표시가 없으면 같은 행을 두 번 누르게 된다.
  const [fixingId, setFixingId] = useState<number | null>(null);
  const [acknowledged, setAcknowledged] = useState(false);
  // 중단하면 노드가 비어 있어 실행 effect가 다시 돌지 않는다. 이 값을 올려 재실행한다.
  const [attempt, setAttempt] = useState(0);
  /*
   * 조회가 멈춘 이유. 사용자가 누른 "중단"과 네트워크·서버 실패를 함께 담는다.
   * 중단만 담았을 때는 와이파이가 끊기면 재시도 버튼이 아예 렌더되지 않아
   * 화면에 다음 수가 없었다.
   */
  const [stopped, setStopped] = useState<{ aborted: boolean; message: string } | null>(null);

  const seeds = useMemo(() => seedsFromRows(rows, addressColumn), [rows, addressColumn]);
  const total = seeds.length;
  const done = !running && nodes.length > 0;
  const counts = useMemo(() => verdictCounts(nodes), [nodes]);
  const missingCount = counts['위치없음'] ?? 0;
  const needCount = counts['요확인'] ?? 0;

  // 두 수치를 번갈아 쓰면 한쪽이 통째로 감춰진다. 화면의 어휘 그대로, 함께 적는다.
  const clusterSub = [
    missingCount > 0 ? `주소 못 찾음 ${missingCount}건 제외` : null,
    needCount > 0 ? `주소 다름 ${needCount}건` : null,
  ]
    .filter(Boolean)
    .join(' · ');

  // 화면 이름표가 스테퍼밖에 없어 27인치에서는 시선이 닿는 자리에 아무 말이 없다.
  const heading = running
    ? `주소 ${total}건 검증 중`
    : done
      ? needCount > 0
        ? `검증 완료 — 확인이 필요한 ${needCount}건`
        : `검증 완료 — ${nodes.length}건`
      : `주소 ${total}건 검증`;

  /*
   * 주소를 고치면 그 행의 판정이 '직접 수정'으로 바뀐다. 마지막 한 건을 고치는 순간
   * 보고 있던 필터의 대상이 0건이 되어 빈 표만 남으므로, 그때는 전체를 보여 준다.
   * 고른 값(filter)은 그대로 두고 보이는 값만 바꾼다 — 렌더 중에 상태를 되돌리면
   * 렌더가 한 번 더 돈다.
   */
  const activeFilter =
    filter !== ALL_VERDICTS && (counts[filter] ?? 0) === 0 ? ALL_VERDICTS : filter;

  const visible = useMemo(
    () => (activeFilter === ALL_VERDICTS ? nodes : nodes.filter((n) => n.verdict === activeFilter)),
    [nodes, activeFilter],
  );

  const emptyMessage =
    nodes.length === 0
      ? '아직 조회한 주소가 없습니다'
      : activeFilter === ALL_VERDICTS
        ? '표시할 행이 없습니다'
        : `'${VERDICT_LABEL[activeFilter]}'에 해당하는 행이 없습니다`;

  const abort = useCallback(() => controllerRef.current?.abort(), []);

  // 세션을 이어받아 이미 노드가 있으면 다시 돌리지 않는다.
  // StrictMode 이중 마운트에서는 정리 함수가 첫 실행을 중단시키고 두 번째가 다시 시작한다.
  useEffect(() => {
    if (nodes.length > 0 || total === 0) return;

    const controller = new AbortController();
    controllerRef.current = controller;
    const signal = controller.signal;
    let cancelled = false;

    const run = async () => {
      const headers = kakaoHeaders();
      setRunning(true);
      setGeoDone(0);
      setRevDone(0);
      setStopped(null);
      try {
        if ((await probeRestKey(headers, signal)) === 'invalid') {
          showApiError(new KakaoAuthError(401), '');
          // 키를 남겨 두면 입력 칸은 비었는데 헤더 칩은 "키 있음"이라 화면이 모순된다.
          clearRestKey();
          setStep(1);
          return;
        }

        const coords = await runPool(
          seeds.map((seed) => () => geocode(seed.address, headers, { signal })),
          CONCURRENCY,
          { onProgress: setGeoDone, signal },
        );

        const reverses = await runPool(
          coords.map((found) => async () =>
            found ? reverseGeocode(found.lat, found.lon, headers, { signal }) : '',
          ),
          CONCURRENCY,
          { onProgress: setRevDone, signal },
        );

        const built: Node[] = seeds.map((seed, i) => {
          const found = coords[i];
          if (!found) {
            return {
              id: i,
              rowIndex: seed.rowIndex,
              name: seed.name,
              address: seed.address,
              lat: null,
              lon: null,
              kakaoAddr: '',
              reverseAddr: '',
              verdict: '위치없음' as Verdict,
            };
          }
          const { verdict, reverseAddr } = verifyAddress(seed.address, reverses[i]);
          return {
            id: i,
            rowIndex: seed.rowIndex,
            name: seed.name,
            address: seed.address,
            lat: found.lat,
            lon: found.lon,
            kakaoAddr: found.kakaoAddr,
            reverseAddr,
            verdict,
          };
        });

        setNodes(built);
      } catch (err) {
        if (cancelled) return;
        if (isPoolAborted(err)) {
          // 중단하면 받아 둔 좌표도 버려진다 — "이어서"가 아니라 "처음부터"임을 분명히 한다.
          const message = '지오코딩을 중단했습니다. "다시 시작"을 누르면 처음부터 다시 조회합니다.';
          setStopped({ aborted: true, message });
          showError(message);
        } else {
          showApiError(err, '지오코딩에 실패했습니다.');
          if (err instanceof KakaoAuthError) {
            clearRestKey();
            setStep(1);
          } else {
            setStopped({
              aborted: false,
              message: `지오코딩에 실패했습니다 — ${
                err instanceof Error ? err.message : '알 수 없는 오류'
              }. "다시 조회"를 누르면 처음부터 다시 조회합니다.`,
            });
          }
        }
      } finally {
        if (!cancelled) setRunning(false);
      }
    };

    void run();
    return () => {
      cancelled = true;
      controller.abort();
    };
    // attempt는 "다시 시작" 버튼이 올리는 재실행 신호다.
  }, [seeds, total, nodes.length, attempt, setNodes, setStep, clearRestKey]);

  const applyFix = async (address: string) => {
    const target = fixing;
    setFixing(null);
    if (!target) return;

    /*
     * 주소를 고치면 좌표가 바뀌고, `setNodes`는 그 아래 산출물을 전부 비운다.
     * 클러스터 순서와 진입·이탈은 계산으로 되살릴 수 없는 순수 수작업이라
     * 실제로 잃을 것이 있을 때만 무엇을 잃는지 숫자로 밝히고 확인을 받는다.
     */
    if (clusterOrder.length > 0 || finalOrder.length > 0) {
      const pickCount = Object.values(clusterPicks).filter(
        (pick) => pick.entry != null || pick.exit != null,
      ).length;
      const lost =
        [
          clusterOrder.length > 0 ? `클러스터 순서 ${clusterOrder.length}개` : null,
          pickCount > 0 ? `진입·이탈 ${pickCount}개` : null,
        ]
          .filter(Boolean)
          .join(', ') || '이후 단계의 결과';
      if (!window.confirm(`주소를 고치면 ${lost}가 지워집니다. 계속할까요?`)) return;
    }

    setFixingId(target.id);
    const headers = kakaoHeaders();
    try {
      const found = await geocode(address, headers);
      if (!found) {
        showError('고친 주소로도 좌표를 찾지 못했습니다.');
        return;
      }
      const reverseAddr = await reverseGeocode(found.lat, found.lon, headers);
      const next = nodes.map((n) =>
        n.id === target.id
          ? {
              ...n,
              address,
              lat: found.lat,
              lon: found.lon,
              kakaoAddr: found.kakaoAddr,
              reverseAddr,
              // 사용자가 직접 고른 주소이므로 판정 결과와 무관하게 '수정됨'으로 남긴다.
              verdict: '수정됨' as Verdict,
            }
          : n,
      );
      setNodes(next);
    } catch (err) {
      // 다른 두 호출부(초기 실행·재실행)와 마찬가지로 키가 잘못됐으면 S1로 돌려보낸다 —
      // 여기서만 REST 키 오류에 화면에 머무를 이유가 없다.
      showApiError(err, '주소 수정에 실패했습니다.');
      if (err instanceof KakaoAuthError) {
        clearRestKey();
        setStep(1);
      }
    } finally {
      setFixingId(null);
    }
  };

  /**
   * "다시 시작"은 지오코딩·역지오코딩을 처음부터 다시 태운다(중단하면 받아 둔 좌표를 버리므로).
   * 건수가 많으면 유료 쿼터를 실수로 크게 소모할 수 있어 한 번 확인을 받는다.
   * 최초 자동 실행(파일 업로드 직후)에는 이 확인이 없다 — 그건 사용자가 이미 의도한 실행이다.
   */
  const restart = () => {
    if (
      total > RESTART_CONFIRM_THRESHOLD &&
      !window.confirm(
        `주소 ${total}건을 처음부터 다시 조회합니다. 좌표 찾기와 주소 대조까지 API를 약 ${
          total * 2
        }회 호출합니다. 계속할까요?`,
      )
    ) {
      return;
    }
    setStopped(null);
    setAttempt((n) => n + 1);
  };

  const columns: Column<Node>[] = [
    { key: 'idx', header: '#', width: 44, className: 'ro-td--idx', cell: (n) => n.id + 1 },
    { key: 'name', header: '이름', width: 80, className: 'ro-td--name', cell: (n) => n.name },
    { key: 'addr', header: '입력한 주소', cell: (n) => n.address },
    {
      key: 'rev',
      header: '찾은 위치의 주소',
      className: 'ro-muted',
      cell: (n) => n.reverseAddr || '—',
    },
    { key: 'verdict', header: '판정', width: 116, cell: (n) => <VerdictBadge verdict={n.verdict} /> },
    {
      key: 'fix',
      header: '',
      width: 72,
      right: true,
      // 판정이 '일치'라도 사람 눈에는 틀릴 수 있다. 최종 판단은 사용자 몫이라 모든 행에 연다.
      cell: (n) => (
        <button
          type="button"
          className={`ro-btn ro-btn--xs${isFixable(n.verdict) ? ' ro-btn--primary' : ''}`}
          disabled={fixingId === n.id}
          title="주소 찾기 창에서 올바른 주소를 고릅니다"
          onClick={() => setFixing(n)}
        >
          {fixingId === n.id ? '수정 중…' : '주소 수정'}
        </button>
      ),
    },
  ];

  return (
    <div className="ro-s2">
      <div className="ro-s2__bar">
        <div style={{ fontWeight: 700, fontSize: 16 }}>{heading}</div>
        <div className="ro-s2__progress">
          <ProgressBar
            label="주소로 좌표 찾기"
            done={geoDone}
            total={total}
            detail={`${geoDone} / ${total} · 동시 ${CONCURRENCY}`}
          />
          <ProgressBar
            label="찾은 좌표의 주소로 대조"
            done={revDone}
            total={total}
            tone="muted"
          />
          {running ? (
            <button
              type="button"
              className="ro-btn ro-btn--danger-outline"
              // "중단"은 보통 일시정지로 읽힌다. 받아 둔 좌표를 버린다는 말을 누르기 전에 한다.
              title="중단하면 지금까지 받은 좌표를 버리고 처음부터 다시 조회합니다"
              onClick={abort}
            >
              중단(처음부터)
            </button>
          ) : null}
          {stopped && !running ? (
            <button type="button" className="ro-btn ro-btn--primary" onClick={restart}>
              {stopped.aborted ? '다시 시작' : '다시 조회'}
            </button>
          ) : null}
          {done ? (
            <div className="ro-s2__done">
              완료 · {nodes.length}/{total}
            </div>
          ) : null}
        </div>

        {/* 토스트만으로는 자리를 비운 사이 근거가 사라진다. 멈춘 이유는 화면에 남긴다. */}
        {stopped && !running ? (
          <div className="ro-hint" style={{ color: 'var(--danger)' }} role="alert">
            {stopped.message}
          </div>
        ) : null}

        <div className="ro-s2__tools">
          <div className="ro-s2__filters">
            {[ALL_VERDICTS, ...VERDICTS].map((name) => {
              const count = counts[name] ?? 0;
              const isAll = name === ALL_VERDICTS;
              return (
                <button
                  key={name}
                  type="button"
                  className={`ro-filter${activeFilter === name ? ' is-on' : ''}`}
                  // 0건짜리 필터를 고르면 빈 표만 남는다. 아예 못 고르게 막는다.
                  disabled={!isAll && count === 0}
                  title={isAll ? '모든 행을 봅니다' : VERDICT_HELP[name]}
                  onClick={() => setFilter(name)}
                >
                  {isAll ? '전체' : VERDICT_LABEL[name]}
                  <span className="ro-filter__count">{count}</span>
                </button>
              );
            })}
          </div>
          <div className="ro-row ro-row--center">
            {needCount > 0 ? (
              // 한 번 누르면 노란 배경이 세션 내내 사라진다. 다시 켤 수 있어야 한다.
              <button
                type="button"
                className="ro-btn ro-btn--sm"
                onClick={() => setAcknowledged((on) => !on)}
              >
                주소 다름 {needCount}건 {acknowledged ? '다시 표시' : '그대로 두기'}
              </button>
            ) : null}
            <button
              type="button"
              className="ro-btn ro-btn--sm ro-btn--primary"
              disabled={!done}
              onClick={() => setStep(3)}
            >
              클러스터링
              <ArrowRight size={18} />{' '}
              {clusterSub ? <span className="ro-btn__sub">{clusterSub}</span> : null}
            </button>
          </div>
        </div>
      </div>

      <div className="ro-s2__table">
        <DataTable
          columns={columns}
          rows={visible}
          rowKey={(n) => n.id}
          rowClassName={(n) => {
            if (n.verdict === '위치없음') return 'is-missing';
            if (n.verdict === '요확인' && !acknowledged) return 'is-check';
            return undefined;
          }}
          empty={running ? null : emptyMessage}
          trailing={running ? '처리 중…' : null}
        />
      </div>

      {fixing ? (
        <PostcodeModal
          title="주소 수정"
          subtitle={fixing.name || `행 ${fixing.rowIndex + 1}`}
          onSelect={applyFix}
          onClose={() => setFixing(null)}
        />
      ) : null}
    </div>
  );
}
