import { useState } from 'react';

import { FileDrop, KeyInput, PostcodeModal } from '../components';
import { STEP_LABELS } from '../components';
import { geocode } from '../api';
import { parseUploadedFile } from '../io';
import { kakaoHeaders, useSessionStore } from '../store/session';
import { showError } from '../store/toast';
import { useVolatileStore } from '../store/volatile';
import { columnLetter, emptyAddressCount, showApiError } from './helpers';

/** 저장 시각을 "2026-09-10 17:42"로. */
function formatSavedAt(ms: number): string {
  if (!ms) return '시각 미상';
  const d = new Date(ms);
  const p = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(
    d.getMinutes(),
  )}`;
}

/** S1 시작 — REST 키, 출발지, 파일 업로드. */
export function StartScreen() {
  const origin = useSessionStore((s) => s.origin);
  const fileName = useSessionStore((s) => s.fileName);
  const sheetName = useSessionStore((s) => s.sheetName);
  const addressColumn = useSessionStore((s) => s.addressColumn);
  const headers = useSessionStore((s) => s.headers);
  const rows = useSessionStore((s) => s.rows);
  const savedStep = useSessionStore((s) => s.step);
  const savedAt = useSessionStore((s) => s.savedAt);
  const thresholdM = useSessionStore((s) => s.thresholdM);
  const setOrigin = useSessionStore((s) => s.setOrigin);
  const setFile = useSessionStore((s) => s.setFile);
  const setStep = useSessionStore((s) => s.setStep);
  const reset = useSessionStore((s) => s.reset);

  const hasKey = useVolatileStore((s) => s.hasKey);
  const setRestKey = useVolatileStore((s) => s.setRestKey);
  const setBuffer = useVolatileStore((s) => s.setBuffer);
  const resumePending = useVolatileStore((s) => s.resumePending);
  const resolveResume = useVolatileStore((s) => s.resolveResume);

  const [keyValue, setKeyValue] = useState('');
  const [postcodeOpen, setPostcodeOpen] = useState(false);
  const [busy, setBusy] = useState(false);

  const hasFile = rows.length > 0;
  // 핸드오프의 파일 바: "택배받을 주소 (E열)" · "124행 · 빈 주소 0"
  const addressColumnLabel = addressColumn
    ? `${addressColumn} (${columnLetter(headers.indexOf(addressColumn))}열)`
    : '—';
  const emptyAddresses = emptyAddressCount(rows);
  const canStart = hasKey && hasFile && !busy;

  const onKeyChange = (value: string) => {
    setKeyValue(value);
    setRestKey(value);
  };

  const onPickOrigin = async (address: string) => {
    setPostcodeOpen(false);
    if (!hasKey) {
      showError('REST 키를 먼저 입력하세요.');
      return;
    }
    setBusy(true);
    try {
      const found = await geocode(address, kakaoHeaders());
      if (!found) {
        showError('출발지 좌표를 찾지 못했습니다. 다른 주소로 다시 시도하세요.');
        return;
      }
      setOrigin({ address, lat: found.lat, lon: found.lon });
    } catch (err) {
      showApiError(err, '출발지 지오코딩에 실패했습니다.');
    } finally {
      setBusy(false);
    }
  };

  const onFile = async (file: File) => {
    setBusy(true);
    try {
      const parsed = await parseUploadedFile(file, file.name);
      setFile(file.name, parsed.sheetName, parsed.addressColumn, parsed.headers, parsed.rows);
      setBuffer(parsed.originalBuffer);
    } catch (err) {
      showError(err instanceof Error ? err.message : '파일을 읽지 못했습니다.');
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="ro-screen-scroll">
      <div className="ro-s1">
        {resumePending ? (
          <div className="ro-banner">
            <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              <div className="ro-banner__title">
                저장된 세션이 있습니다 — {formatSavedAt(savedAt)} · {savedStep}단계{' '}
                {STEP_LABELS[savedStep - 1]}까지 진행
              </div>
              <div className="ro-banner__sub">
                {fileName || '파일 없음'} · {rows.length}행 · 임계값 {thresholdM}m. 이어서 하려면
                REST 키만 다시 입력하세요.
              </div>
            </div>
            <div className="ro-row">
              <button
                type="button"
                className="ro-btn ro-btn--sm ro-btn--quiet"
                onClick={() => {
                  reset();
                  setBuffer(null);
                  resolveResume();
                }}
              >
                삭제
              </button>
              <button
                type="button"
                className="ro-btn ro-btn--sm ro-btn--primary"
                disabled={!hasKey}
                onClick={resolveResume}
              >
                이어서 하기
              </button>
            </div>
          </div>
        ) : null}

        <div className="ro-s1__grid">
          <section className="ro-card">
            <div className="ro-card__head">
              <div className="ro-card__title">1. 카카오 REST API 키</div>
              <div className="ro-card__note">이 탭에서만 유지</div>
            </div>
            <KeyInput value={keyValue} onChange={onKeyChange} />
            <p className="ro-hint">
              유효성은 첫 지오코딩이 성공하면 확인됩니다. 키는 저장소나 localStorage에 저장되지
              않습니다.
            </p>
          </section>

          <section className="ro-card">
            <div className="ro-card__head">
              <div className="ro-card__title">2. 출발지</div>
              <div className="ro-card__note">지난 출발지 기억됨</div>
            </div>
            <div className="ro-row ro-row--center">
              <div className="ro-readout">
                <span className="ro-readout__dot" />
                <span className="ro-readout__text">{origin?.address ?? '아직 고르지 않음'}</span>
              </div>
              <button
                type="button"
                className="ro-btn ro-btn--soft"
                style={{ fontSize: 12 }}
                onClick={() => setPostcodeOpen(true)}
              >
                주소 찾기
              </button>
            </div>
            {origin ? (
              <div className="ro-confirm">
                <span>✓</span>지오코딩 확인 · {origin.lat.toFixed(4)}, {origin.lon.toFixed(4)}
              </div>
            ) : (
              <p className="ro-hint">
                출발지를 정하면 첫 클러스터의 진입 지점을 자동으로 제안할 수 있습니다.
              </p>
            )}
          </section>
        </div>

        <section className="ro-card">
          <div className="ro-card__head">
            <div className="ro-card__title">3. 배송 목록 파일</div>
            <div className="ro-card__note">xlsx · csv</div>
          </div>
          {hasFile ? (
            <div className="ro-filebar">
              <div className="ro-filebar__name">
                <div className="ro-filebar__icon">
                  {/\.csv$/i.test(fileName) ? 'CSV' : 'XLS'}
                </div>
                <div style={{ fontWeight: 500 }}>{fileName}</div>
              </div>
              <dl className="ro-filebar__cell">
                <dt>감지된 시트</dt>
                <dd>{sheetName ?? 'CSV (단일 시트)'}</dd>
              </dl>
              <dl className="ro-filebar__cell">
                <dt>주소 열</dt>
                <dd>{addressColumnLabel}</dd>
              </dl>
              <dl className="ro-filebar__cell">
                <dt>행 수</dt>
                <dd>
                  {rows.length}행 · 빈 주소 {emptyAddresses}
                </dd>
              </dl>
              <button
                type="button"
                className="ro-btn ro-btn--sm ro-btn--quiet"
                style={{ margin: '0 12px' }}
                onClick={() => {
                  setFile('', null, null, [], []);
                  setBuffer(null);
                }}
              >
                교체
              </button>
            </div>
          ) : (
            <FileDrop onFile={onFile} busy={busy} />
          )}
        </section>

        <div className="ro-s1__foot">
          <p className="ro-s1__privacy">
            데이터는 브라우저 안에서만 처리됩니다. 카카오 서버로는 주소와 좌표만 전송되며,
            이름·연락처 등 나머지 열은 이 기기를 떠나지 않습니다.
          </p>
          <button
            type="button"
            className="ro-btn ro-btn--lg ro-btn--primary"
            disabled={!canStart}
            onClick={() => {
              resolveResume();
              setStep(2);
            }}
          >
            지오코딩 시작 →
          </button>
        </div>
      </div>

      {postcodeOpen ? (
        <PostcodeModal
          title="출발지 주소 찾기"
          subtitle="우편번호 서비스에서 주소를 선택하세요"
          onSelect={onPickOrigin}
          onClose={() => setPostcodeOpen(false)}
        />
      ) : null}
    </div>
  );
}
