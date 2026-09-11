# 배송 경로 최적화

https://marista-dev.github.io/route-optimizer/

배송 명단(xlsx·csv)의 주소를 좌표로 바꾸고 가까운 배송지끼리 묶는다.
클러스터 방문 순서와 진입·이탈 지점은 사용자가 지도에서 고르고, 클러스터 내부 순서만 카카오 도로시간으로 계산한다.
결과는 csv·xlsx로 내려받는다.

## 단계

| 단계 | 내용 |
|---|---|
| 1. 시작 | REST API Key 입력, 출발지 선택, 명단 파일 업로드 |
| 2. 주소검증 | 주소 → 좌표 변환과 대조. 틀린 주소는 주소 찾기 창에서 수정 |
| 3. 클러스터링 | 임계값 200~500m로 묶음 크기 조절 |
| 4. 순서배정 | 지도에서 다각형을 방문 순서대로 클릭 |
| 5. 진입·이탈 | 클러스터별 진입 지점·이탈 지점 선택 → 내부 순서 계산 |
| 6. 결과 | 경로 확인, 순서 편집, csv·xlsx 다운로드 |

진행 상황은 브라우저에 저장되어 새로고침 후 이어서 진행할 수 있다. REST 키만 다시 입력한다.

카카오 REST API Key가 필요하다([개발자 콘솔](https://developers.kakao.com) 발급). 접속할 때마다 입력하며 `sessionStorage`에만 두므로 탭을 닫으면 사라진다.

명단은 브라우저를 벗어나지 않는다. 카카오로 전송되는 것은 주소와 좌표뿐이다. 이어서 하기를 위해 원본 행이 최대 7일 남으며 `세션 삭제`로 지운다.

## 개발

```bash
cp .env.example .env.local   # VITE_KAKAO_JS_KEY에 카카오 JavaScript 키 입력
npm install
npm run dev                  # http://localhost:5173/route-optimizer/
```

`dev` · `typecheck` · `test` · `test:watch` · `lint` · `build`

타입 검사는 `npm run typecheck`를 쓴다. `npx tsc --noEmit -p .`는 solution 구성이라 아무것도 검사하지 않고 통과한다.
