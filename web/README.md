# 배송 경로 최적화 (Web)

저장소 루트의 [README](../README.md)에 사용법과 구조 설명이 있다. 이 문서는 개발 명령만 다룬다.

## 로컬 실행

```bash
cp .env.example .env.local   # VITE_KAKAO_JS_KEY에 카카오 JavaScript 키를 넣는다
npm install
npm run dev                  # http://localhost:5173
```

카카오 개발자 콘솔 > 플랫폼 > Web에 `http://localhost:5173`과 Pages 도메인을 등록해야 지도가 뜬다.
카카오 **REST 키**는 저장하지 않는다. 접속할 때마다 화면에서 입력하고 `sessionStorage`에만 둔다.

## 스크립트

| 명령 | 설명 |
|---|---|
| `npm run dev` | 개발 서버 |
| `npm run typecheck` | 타입 검사 (`tsc -b --force`). `tsc --noEmit -p .`는 아무것도 검사하지 않으니 쓰지 말 것 |
| `npm run build` | 타입 검사 + 프로덕션 빌드 (`dist/`) |
| `npm test` | vitest 1회 실행 |
| `npm run test:watch` | vitest 감시 모드 |
| `npm run lint` | oxlint |

## 배포

`main`에 `web/**` 변경이 푸시되면 `.github/workflows/pages.yml`이 빌드해 Pages로 배포한다.
JS 키는 저장소 Settings > Secrets and variables > Actions > Variables의 `VITE_KAKAO_JS_KEY`를 쓴다.
