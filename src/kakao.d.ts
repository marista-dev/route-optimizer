/**
 * 카카오맵 Web SDK와 우편번호 embed는 공식 타입 패키지가 없어 전역 선언만 둔다.
 * SDK는 `index.html`에서 autoload=false로 불러오고 `kakao.maps.load` 콜백 뒤에만 쓴다.
 */

/** 전역 카카오 SDK 네임스페이스 */
declare const kakao: any;

interface Window {
  /** 카카오맵 SDK. 스크립트 로드 실패 시 undefined */
  kakao: any;
  /** 카카오(다음) 우편번호 서비스. 주소 검색 모달에서 쓴다 */
  daum: any;
}
