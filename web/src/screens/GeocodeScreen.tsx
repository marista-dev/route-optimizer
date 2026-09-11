import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import { KakaoAuthError, geocode, isPoolAborted, reverseGeocode, runPool } from '../api';
import { verifyAddress } from '../core';
import { DataTable, PostcodeModal, ProgressBar, VerdictBadge } from '../components';
import type { Column } from '../components';
import { kakaoHeaders, useSessionStore } from '../store/session';
import { showError } from '../store/toast';
import type { KakaoHeaders, Node, Verdict } from '../types';
import {
  ALL_VERDICTS,
  VERDICTS,
  computeWarnCount,
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

/**
 * 키가 유효한지 한 번만 찔러본다.
 *
 * `geocode`는 401을 조용히 넘기고 null을 돌려주므로 잘못된 키와 "주소를 못 찾음"을
 * 구분할 수 없다. 그래서 여기서만 로컬 API를 직접 호출해 상태 코드를 본다.
 */
async function probeRestKey(
  headers: KakaoHeaders,
  signal: AbortSignal,
): Promise<'ok' | 'invalid' | 'unknown'> {
  try {
    const resp = await fetch(
      'https://dapi.kakao.com/v2/local/search/address.json?query=' + encodeURIComponent('서울특별시'),
      { headers, signal },
    );
    if (resp.status === 401) return 'invalid';
    return resp.status === 200 ? 'ok' : 'unknown';
  } catch {
    return 'unknown';
  }
}

/** S2 지오코딩 · 검증. */
export function GeocodeScreen() {
  const rows = useSessionStore((s) => s.rows);
  const addressColumn = useSessionStore((s) => s.addressColumn);
  const nodes = useSessionStore((s) => s.nodes);
  const setNodes = useSessionStore((s) => s.setNodes);
  const setStep = useSessionStore((s) => s.setStep);

  const controllerRef = useRef<AbortController | null>(null);
  const [running, setRunning] = useState(false);
  const [geoDone, setGeoDone] = useState(0);
  const [revDone, setRevDone] = useState(0);
  const [filter, setFilter] = useState<Verdict | typeof ALL_VERDICTS>(ALL_VERDICTS);
  const [fixing, setFixing] = useState<Node | null>(null);
  const [acknowledged, setAcknowledged] = useState(false);
  // 중단하면 노드가 비어 있어 실행 effect가 다시 돌지 않는다. 이 값을 올려 재실행한다.
  const [attempt, setAttempt] = useState(0);
  const [aborted, setAborted] = useState(false);

  const seeds = useMemo(() => seedsFromRows(rows, addressColumn), [rows, addressColumn]);
  const total = seeds.length;
  const done = !running && nodes.length > 0;
  const counts = useMemo(() => verdictCounts(nodes), [nodes]);
  const warnCount = useMemo(() => computeWarnCount(nodes), [nodes]);
  const missingCount = counts['위치없음'] ?? 0;
  const needCount = counts['요확인'] ?? 0;

  const visible = useMemo(
    () => (filter === ALL_VERDICTS ? nodes : nodes.filter((n) => n.verdict === filter)),
    [nodes, filter],
  );

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
      try {
        if ((await probeRestKey(headers, signal)) === 'invalid') {
          showApiError(new KakaoAuthError(401), '');
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
          setAborted(true);
          // 중단하면 받아 둔 좌표도 버려진다 — "이어서"가 아니라 "처음부터"임을 분명히 한다.
          showError('지오코딩을 중단했습니다. "다시 시작"을 누르면 처음부터 다시 조회합니다.');
        } else {
          showApiError(err, '지오코딩에 실패했습니다.');
          if (err instanceof KakaoAuthError) setStep(1);
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
  }, [seeds, total, nodes.length, attempt, setNodes, setStep]);

  const applyFix = async (address: string) => {
    const target = fixing;
    setFixing(null);
    if (!target) return;
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
      if (err instanceof KakaoAuthError) setStep(1);
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
        `주소 ${total}건을 처음부터 다시 조회합니다. 지오코딩과 역지오코딩까지 API를 약 ${
          total * 2
        }회 호출합니다. 계속할까요?`,
      )
    ) {
      return;
    }
    setAborted(false);
    setAttempt((n) => n + 1);
  };

  const columns: Column<Node>[] = [
    { key: 'idx', header: '#', width: 44, className: 'ro-td--idx', cell: (n) => n.id + 1 },
    { key: 'name', header: '이름', width: 80, className: 'ro-td--name', cell: (n) => n.name },
    { key: 'addr', header: '원본 주소', cell: (n) => n.address },
    { key: 'kakao', header: '카카오 확인주소', className: 'ro-muted', cell: (n) => n.kakaoAddr || '—' },
    { key: 'rev', header: '역지오코딩', className: 'ro-muted', cell: (n) => n.reverseAddr || '—' },
    { key: 'verdict', header: '판정', width: 96, cell: (n) => <VerdictBadge verdict={n.verdict} /> },
    {
      key: 'fix',
      header: '',
      width: 72,
      right: true,
      cell: (n) =>
        isFixable(n.verdict) ? (
          <button type="button" className="ro-btn ro-btn--xs" onClick={() => setFixing(n)}>
            수정
          </button>
        ) : null,
    },
  ];

  return (
    <div className="ro-s2">
      <div className="ro-s2__bar">
        <div className="ro-s2__progress">
          <ProgressBar
            label="2단계 · 지오코딩 (주소 → 좌표)"
            done={geoDone}
            total={total}
            detail={`${geoDone} / ${total} · 동시 ${CONCURRENCY}`}
          />
          <ProgressBar
            label="3단계 · 역지오코딩 검증"
            done={revDone}
            total={total}
            tone="muted"
          />
          {running ? (
            <button type="button" className="ro-btn ro-btn--danger-outline" onClick={abort}>
              중단
            </button>
          ) : null}
          {aborted && !running ? (
            <button type="button" className="ro-btn ro-btn--primary" onClick={restart}>
              다시 시작
            </button>
          ) : null}
          {done ? (
            <div className="ro-s2__done">
              완료 · {nodes.length}/{total}
            </div>
          ) : null}
        </div>

        <div className="ro-s2__tools">
          <div className="ro-s2__filters">
            {[ALL_VERDICTS, ...VERDICTS].map((name) => (
              <button
                key={name}
                type="button"
                className={`ro-filter${filter === name ? ' is-on' : ''}`}
                onClick={() => setFilter(name)}
              >
                {name}
                <span className="ro-filter__count">{counts[name] ?? 0}</span>
              </button>
            ))}
          </div>
          <div className="ro-row ro-row--center">
            {needCount > 0 && !acknowledged ? (
              <button
                type="button"
                className="ro-btn ro-btn--sm"
                onClick={() => setAcknowledged(true)}
              >
                요확인 {needCount}건 원본 그대로
              </button>
            ) : null}
            <button
              type="button"
              className="ro-btn ro-btn--sm ro-btn--primary"
              disabled={!done}
              onClick={() => setStep(3)}
            >
              클러스터링 →{' '}
              <span className="ro-btn__sub">
                {missingCount > 0 ? `위치없음 ${missingCount}건 제외` : `경고 ${warnCount}건`}
              </span>
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
          empty={running ? null : '표시할 행이 없습니다'}
          trailing={running ? '처리 중…' : null}
        />
      </div>

      {fixing ? (
        <PostcodeModal
          title="주소 수정"
          subtitle={`${fixing.name || `행 ${fixing.rowIndex + 1}`} · 선택한 주소로 좌표를 다시 확인합니다`}
          onSelect={applyFix}
          onClose={() => setFixing(null)}
        />
      ) : null}
    </div>
  );
}
