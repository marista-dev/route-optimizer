import { Stepper, Toast } from './components';
import {
  ClusterScreen,
  EntryExitScreen,
  GeocodeScreen,
  OrderScreen,
  ResultScreen,
  StartScreen,
} from './screens';
import { useSessionStore } from './store/session';
import { useVolatileStore } from './store/volatile';
import type { Step } from './types';

const SCREENS: Record<Step, () => React.JSX.Element> = {
  1: StartScreen,
  2: GeocodeScreen,
  3: ClusterScreen,
  4: OrderScreen,
  5: EntryExitScreen,
  6: ResultScreen,
};

/** 헤더 + 6단계 스테퍼 + 현재 화면. */
function App() {
  const savedStep = useSessionStore((s) => s.step);
  const setStep = useSessionStore((s) => s.setStep);

  const hasKey = useVolatileStore((s) => s.hasKey);
  const reset = useSessionStore((s) => s.reset);
  const setBuffer = useVolatileStore((s) => s.setBuffer);
  const hasWork = useSessionStore((s) => s.rows.length > 0);
  // 저장된 세션을 아직 이어받지 않았으면 단계와 무관하게 S1(키 재입력)부터 시작한다.
  const resumePending = useVolatileStore((s) => s.resumePending);
  const step: Step = resumePending ? 1 : savedStep;
  const Screen = SCREENS[step];

  return (
    <div className="ro-app">
      <header className="ro-header">
        <div className="ro-brand">
          <div className="ro-brand__logo">RO</div>
          <div className="ro-brand__name">배송 경로 최적화</div>
        </div>
        <Stepper current={step} onGo={setStep} />
        <div className="ro-headchips">
          <div className={`ro-chip${hasKey ? ' is-on' : ''}`}>
            <span className="ro-chip__dot" />
            {hasKey ? 'REST 키 · 세션' : 'REST 키 없음'}
          </div>
          {/*
            업로드한 원본 행(이름·연락처 포함)은 브라우저에 7일까지 남는다.
            작업을 중간에 그만둘 때 지울 수단이 지금까지 결과 화면에만 있었다.
            어느 단계에서든 즉시 지울 수 있도록 헤더에 둔다.
          */}
          {hasWork ? (
            <button
              type="button"
              className="ro-btn ro-btn--xs ro-btn--danger-outline"
              title="업로드한 명단과 진행 상황을 이 브라우저에서 지웁니다"
              onClick={() => {
                if (!window.confirm('업로드한 명단과 진행 상황을 모두 지울까요?')) return;
                reset();
                setBuffer(null);
              }}
            >
              세션 삭제
            </button>
          ) : null}
        </div>
      </header>

      <main className="ro-main">
        <Screen />
      </main>

      <Toast />
    </div>
  );
}

export default App;
