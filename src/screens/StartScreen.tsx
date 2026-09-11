import { useRef, useState } from 'react';
import { Check } from 'lucide-react';

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
  const nodes = useSessionStore((s) => s.nodes);
  const savedStep = useSessionStore((s) => s.step);
  const savedAt = useSessionStore((s) => s.savedAt);
  const thresholdM = useSessionStore((s) => s.thresholdM);
  const setOrigin = useSessionStore((s) => s.setOrigin);
  const setFile = useSessionStore((s) => s.setFile);
  const setStep = useSessionStore((s) => s.setStep);
  const reset = useSessionStore((s) => s.reset);

  const hasKey = useVolatileStore((s) => s.hasKey);
  const setRestKey = useVolatileStore((s) => s.setRestKey);
  const resumePending = useVolatileStore((s) => s.resumePending);
  const resolveResume = useVolatileStore((s) => s.resolveResume);

  const [keyValue, setKeyValue] = useState('');
  const [postcodeOpen, setPostcodeOpen] = useState(false);
  // 파일 파싱과 출발지 지오코딩은 서로 다른 카드에서 돈다. 한 값을 나눠 쓰면
  // 만지지도 않은 드롭존에 "파일을 읽는 중…"이 뜬다.
  const [parsing, setParsing] = useState(false);
  const [originBusy, setOriginBusy] = useState(false);
  // 토스트는 지나가고 만다. 왜 업로드가 안 됐는지는 화면에 남아 있어야 한다.
  const [uploadError, setUploadError] = useState<string | null>(null);
  const replaceInputRef = useRef<HTMLInputElement>(null);

  const hasFile = rows.length > 0;
  // 핸드오프의 파일 바: "택배받을 주소 (E열)" · "124행 · 빈 주소 0"
  const addressColumnLabel = addressColumn
    ? `${addressColumn} (${columnLetter(headers.indexOf(addressColumn))}열)`
    : '—';
  const emptyAddresses = emptyAddressCount(rows);
  const canStart = hasKey && hasFile && !parsing && !originBusy;
  // 회색 버튼만 보여 주면 왜 안 눌리는지 알 길이 없다. 남은 조건을 한 줄로 적는다.
  const startHint = !hasKey
    ? 'REST API 키를 입력하세요'
    : parsing
      ? '파일을 읽는 중입니다'
      : !hasFile
        ? '파일을 올리세요'
        : originBusy
          ? '출발지 좌표를 확인하는 중입니다'
          : null;

  const onKeyChange = (value: string) => {
    setKeyValue(value);
    setRestKey(value);
  };

  const onPickOrigin = async (address: string) => {
    setPostcodeOpen(false);
    if (!hasKey) {
      showError('REST API 키를 먼저 입력하세요.');
      return;
    }
    setOriginBusy(true);
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
      setOriginBusy(false);
    }
  };

  const onFile = async (file: File) => {
    setParsing(true);
    setUploadError(null);
    try {
      const parsed = await parseUploadedFile(file, file.name);
      setFile(file.name, parsed.sheetName, parsed.addressColumn, parsed.headers, parsed.rows);
    } catch (err) {
      const message = err instanceof Error ? err.message : '파일을 읽지 못했습니다.';
      showError(message);
      setUploadError(`${file.name}: ${message}`);
    } finally {
      setParsing(false);
    }
  };

  /**
   * "교체"는 이름대로 교체해야 한다 — 파일 선택 창을 열고, 새 파일을 실제로 고른
   * 뒤에만 `onFile`이 `setFile`을 부른다. 취소하면 지금 파일이 그대로 남는다.
   */
  const onReplace = () => {
    if (
      nodes.length > 0 &&
      !window.confirm(
        `새 파일을 올리면 지오코딩 결과 ${nodes.length}건과 이후 작업이 모두 지워집니다. 계속할까요?`,
      )
    ) {
      return;
    }
    replaceInputRef.current?.click();
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
                title="업로드한 명단과 진행 상황을 이 브라우저에서 지웁니다"
                onClick={() => {
                  if (!window.confirm('업로드한 명단과 진행 상황을 모두 지울까요?')) return;
                  reset();
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
              <div className="ro-card__title">1. REST API 키</div>
              <div className="ro-card__note">이 탭에서만 유지</div>
            </div>
            <KeyInput value={keyValue} onChange={onKeyChange} />
          </section>

          <section className="ro-card">
            <div className="ro-card__head">
              {/* canStart에 들어가지 않는 선택 항목이다. 필수처럼 읽히지 않게 제목에 적는다. */}
              <div className="ro-card__title">2. 출발지 (선택)</div>
              <div className="ro-card__note">
                {origin ? '지난 출발지 기억됨' : '없으면 첫 클러스터의 진입 지점 추천이 빠집니다'}
              </div>
            </div>
            <div className="ro-row ro-row--center">
              {origin ? (
                <div className="ro-readout">
                  <span className="ro-readout__dot" />
                  <span className="ro-readout__text">{origin.address}</span>
                </div>
              ) : null}
              <button
                type="button"
                className="ro-btn ro-btn--soft"
                // 키가 없으면 고른 주소가 그대로 버려진다. 검색을 시작하기 전에 막는다.
                disabled={!hasKey || originBusy}
                title={
                  !hasKey
                    ? 'REST API 키를 먼저 입력하세요'
                    : originBusy
                      ? '출발지 좌표를 확인하는 중입니다'
                      : undefined
                }
                onClick={() => setPostcodeOpen(true)}
              >
                주소 찾기
              </button>
            </div>
            {originBusy ? <p className="ro-hint ro-hint--small">출발지 좌표를 확인하는 중…</p> : null}
            {origin ? (
              <div className="ro-confirm">
                <Check size={15} />지오코딩 확인 · {origin.lat.toFixed(4)}, {origin.lon.toFixed(4)}
              </div>
            ) : null}
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
                disabled={parsing}
                title="다른 파일을 골라 지금 파일을 대신합니다"
                onClick={onReplace}
              >
                {parsing ? '읽는 중…' : '교체'}
              </button>
              <input
                ref={replaceInputRef}
                type="file"
                accept=".xlsx,.xls,.csv"
                hidden
                onChange={(e) => {
                  const file = e.target.files?.[0];
                  if (file) void onFile(file);
                  e.target.value = '';
                }}
              />
            </div>
          ) : (
            <FileDrop onFile={onFile} busy={parsing} />
          )}
          {uploadError ? (
            <p className="ro-keystate is-invalid" role="alert">
              {uploadError}
            </p>
          ) : null}
        </section>

        <div className="ro-s1__foot">
          {startHint ? <span className="ro-hint">{startHint}</span> : null}
          <button
            type="button"
            className="ro-btn ro-btn--xl ro-btn--primary"
            disabled={!canStart}
            onClick={() => {
              resolveResume();
              setStep(2);
            }}
          >
            시작
          </button>
        </div>
      </div>

      {postcodeOpen ? (
        <PostcodeModal
          title="출발지 주소 찾기"
          onSelect={onPickOrigin}
          onClose={() => setPostcodeOpen(false)}
        />
      ) : null}
    </div>
  );
}
