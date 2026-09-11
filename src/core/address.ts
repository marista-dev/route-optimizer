/**
 * address.ts — `src/core/address.py` 이식.
 *
 * 한국 주소 문자열에서 건물 단위(동/호/층)를 읽어내는 공용 유틸.
 * 정규식은 Python 원본과 글자 그대로 같다. lookbehind `(?<![가-힣0-9])`는
 * 최신 브라우저가 모두 지원한다.
 *
 * 핵심 난점은 '동'이 두 가지 뜻으로 쓰인다는 점이다.
 *   - 법정동/행정동 : 운암동, 운암1동, 오치2동, 문흥1동  → 주소의 일부
 *   - 건물 동       : 101동, 201동, A동, C동            → 건물 식별자
 * 숫자 앞에 한글이 붙으면 법정동으로 보고 건드리지 않는다.
 */

/** 괄호 주기 — '(신용동,용두주공@)', '(두암동 871-28)' 등 */
const PAREN_RE = /\([^)]*\)/gu;

/**
 * 건물 동(숫자) — 앞에 한글/숫자가 붙으면 법정동이거나 다른 번호이므로 제외.
 * '제201동' 표기도 함께 받는다.
 */
const DONG_NUM_RE = /(?<![가-힣0-9])제?\s*(\d+)\s*동(?![가-힣])/u;

/** 건물 동(영문) — 'A동', 'C동' */
const DONG_ALPHA_RE = /(?<![가-힣A-Za-z0-9])([A-Za-z])\s*동(?![가-힣])/u;

/** 호수 — '1402호', '제1504호'. '호반리젠시빌'처럼 숫자 없는 '호'는 매칭되지 않는다. */
const HO_RE = /(?<![가-힣0-9])제?\s*(\d+)\s*호(?![가-힣])/u;
const HO_RE_G = /(?<![가-힣0-9])제?\s*(\d+)\s*호(?![가-힣])/gu;

/** 층 — '3층', '2 층' */
const FLOOR_RE_G = /제?\s*\d+\s*층/gu;

/** 구분자 (정규화 키에서 통째로 제거) */
const SEP_RE_G = /[\s,]+/gu;

/** '호' 접미사 없이 숫자만 적힌 표기('112동 1205')를 받기 위한 폴백 */
const HO_FALLBACK_RE = /^\s*(\d+)(?![\d\-가-힣])/u;

// ── 제거용 패턴 ──────────────────────────────────────────────────────────────
// 위 패턴은 '찾기'용이라 동 뒤 숫자를 남긴다(parseUnit의 호수 폴백이 그 숫자를
// 봐야 하므로). 키 정규화·질의 생성에서는 동과 호수를 한 덩어리로 지워야
// '106동 1504호'와 '112동 1205'(호 접미사 없음)가 같은 규칙으로 정리된다.
//   '101동 15번지'처럼 뒤 숫자가 호수가 아닌 경우는 (?![가-힣0-9])가 걸러낸다.
const UNIT_SUFFIX = '(?:\\s*제?\\s*\\d+\\s*호?(?![가-힣0-9]))?';
const DONG_NUM_BLOCK_RE_G = new RegExp(
  '(?<![가-힣0-9])제?\\s*\\d+\\s*동(?![가-힣])' + UNIT_SUFFIX, 'gu');
const DONG_ALPHA_BLOCK_RE_G = new RegExp(
  '(?<![가-힣A-Za-z0-9])[A-Za-z]\\s*동(?![가-힣])' + UNIT_SUFFIX, 'gu');

/** 동 + 호수 + 층을 제거한다 (키 정규화·지오코딩 질의 공용). */
function removeUnits(s: string): string {
  let out = s.replace(DONG_NUM_BLOCK_RE_G, ' ');
  out = out.replace(DONG_ALPHA_BLOCK_RE_G, ' ');
  out = out.replace(HO_RE_G, ' ');
  out = out.replace(FLOOR_RE_G, ' ');
  return out;
}

/** parseUnit의 결과. Python의 `(동번호, 동문자, 호수)` 튜플에 대응한다. */
export interface ParsedUnit {
  /** 건물 동 번호. 없으면 null */
  dongNum: number | null;
  /** 건물 동 문자(대문자). 없으면 null. dongNum과 배타적이다 */
  dongTxt: string | null;
  /** 호수. 없으면 null */
  ho: number | null;
}

/**
 * 주소에서 (동번호, 동문자, 호수)를 뽑는다.
 *
 * 동번호와 동문자는 배타적이다 — 숫자 동이 있으면 동문자는 null.
 *
 * ```
 * parseUnit('광주 북구 삼정로 7, 207동 403호(두암동, 주공2단지@)')
 *   → { dongNum: 207, dongTxt: null, ho: 403 }
 * parseUnit('광주 북구 안산로 38-3, A동 102호 (오치2동, 영진하우스)')
 *   → { dongNum: null, dongTxt: 'A', ho: 102 }
 * parseUnit('광주 북구 하백로 46번길 9, 809호(매곡동, 부림@)')
 *   → { dongNum: null, dongTxt: null, ho: 809 }
 * parseUnit('광주 북구 첨단연신로 250, 112동 1205(신용동,첨단휴먼시아)')
 *   → { dongNum: 112, dongTxt: null, ho: 1205 }
 * ```
 */
export function parseUnit(address: string): ParsedUnit {
  if (!address) return { dongNum: null, dongTxt: null, ho: null };

  // 괄호 안에는 법정동·아파트명이 들어 있어 오탐만 만든다 → 먼저 제거
  const s = address.replace(PAREN_RE, ' ');

  let dongNum: number | null = null;
  let dongTxt: string | null = null;
  let m = DONG_NUM_RE.exec(s);
  if (m) {
    dongNum = parseInt(m[1], 10);
  } else {
    m = DONG_ALPHA_RE.exec(s);
    if (m) dongTxt = m[1].toUpperCase();
  }

  // 호수는 동 뒤쪽에서만 찾는다 (앞쪽 도로명 번호를 호수로 오인하지 않도록)
  const rest = m ? s.slice(m.index + m[0].length) : s;

  const mh = HO_RE.exec(rest);
  if (mh) return { dongNum, dongTxt, ho: parseInt(mh[1], 10) };

  // '호' 접미사 없이 숫자만 적힌 표기 — '112동 1205'
  // 번지형('20-1')이나 '15번지'처럼 뒤에 다른 말이 붙으면 호수가 아니다
  if (m) {
    const mn = HO_FALLBACK_RE.exec(rest);
    if (mn) return { dongNum, dongTxt, ho: parseInt(mn[1], 10) };
  }

  return { dongNum, dongTxt, ho: null };
}

/**
 * 같은 단지/건물을 하나로 묶기 위한 정규화 키.
 *
 * 동·호·층과 괄호 주기를 지우고 공백·쉼표까지 없앤다.
 * 표기 흔들림('(매곡동,아남@)' vs '(매곡동, 아남@)')으로 같은 단지가
 * 갈라지는 것을 막기 위해서다.
 *
 * ```
 * complexKey('광주 북구 매곡로 92, 102동 106호(매곡동,아남@)')  → '광주북구매곡로92'
 * complexKey('광주 북구 매곡로 92, 101동 105호(매곡동, 아남@)') → '광주북구매곡로92'
 * ```
 */
export function complexKey(address: string): string {
  if (!address) return '';
  let s = removeUnits(address.replace(PAREN_RE, ' '));
  s = s.replace(SEP_RE_G, '');
  return s.toLowerCase();
}

/**
 * 동/호/층만 지운 주소. 괄호와 띄어쓰기는 그대로 둔다.
 *
 * 지오코딩 질의용 — 카카오 로컬 API는 상세 호수가 붙으면 실패하지만
 * 괄호 안 법정동·건물명은 오히려 정확도를 높여준다.
 */
export function stripUnit(address: string): string {
  if (!address) return '';
  let s = removeUnits(address);
  s = s.replace(/\s+/gu, ' ');
  // Python: s.strip().rstrip(',').strip()
  return s.trim().replace(/,+$/u, '').trim();
}

/**
 * 같은 단지 안에서 '동 오름차순 → 호 오름차순'으로 세우기 위한 정렬 키.
 *
 * 숫자 동 → 영문 동 → 동 없음 순으로 묶고, 동/호를 못 읽은 항목은 뒤로 민다.
 * 마지막 tiebreak(보통 노드 id)로 항상 같은 결과가 나오게 한다.
 *
 * Python의 튜플 `(rank, num, txt, hoRank, ho, tiebreak)`을 그대로 배열로 옮긴 것이라
 * 3번째 원소만 문자열이고 나머지는 숫자다. 비교는 {@link compareUnit}을 쓴다.
 */
export type UnitSortKey = [number, number, string, number, number, number];

export function unitSortKey(address: string, tiebreak: number): UnitSortKey {
  const { dongNum, dongTxt, ho } = parseUnit(address);
  let rank: number;
  let num: number;
  let txt: string;
  if (dongNum !== null) {
    rank = 0;
    num = dongNum;
    txt = '';
  } else if (dongTxt) {
    rank = 1;
    num = 0;
    txt = dongTxt;
  } else {
    rank = 2;
    num = 0;
    txt = '';
  }
  return [rank, num, txt, ho !== null ? 0 : 1, ho ?? 0, tiebreak];
}

/** {@link unitSortKey} 두 개를 Python 튜플 비교와 같은 순서로 비교한다. */
export function compareUnit(a: UnitSortKey, b: UnitSortKey): number {
  for (let i = 0; i < a.length; i++) {
    const x = a[i];
    const y = b[i];
    if (typeof x === 'string' || typeof y === 'string') {
      const sx = String(x);
      const sy = String(y);
      if (sx !== sy) return sx < sy ? -1 : 1;
    } else if (x !== y) {
      return x < y ? -1 : 1;
    }
  }
  return 0;
}

/**
 * 주소 + tiebreak 쌍을 바로 비교하는 comparator.
 * `list.sort((a, b) => compareUnitAddr(a.address, a.id, b.address, b.id))` 형태로 쓴다.
 */
export function compareUnitAddr(
  addressA: string, tiebreakA: number,
  addressB: string, tiebreakB: number,
): number {
  return compareUnit(unitSortKey(addressA, tiebreakA), unitSortKey(addressB, tiebreakB));
}

// ── 건물 이름 표시용 ────────────────────────────────────────────────────────
// 진입·이탈은 "누구네 집"이 아니라 "어느 건물"을 고르는 일이다(현장 피드백 3).
// 수령인 이름 대신 단지/건물 이름을 보여주기 위한 유틸.

/** 마지막 괄호 주기 — '(운암동, 광주첨단이지more)' */
const LAST_PAREN_RE = /\(([^)]*)\)(?![^(]*\()/u;

/**
 * 괄호 안 조각이 건물명이 아니라 법정동·지번 주기인지.
 * '운암동', '오치2동', '두암동 871-28', '수완동' 등.
 */
function isAreaNote(segment: string): boolean {
  return /^[가-힣]+\d*(동|가|리|읍|면)(\s+[\d-]+)?$/u.test(segment);
}

/** 공백 정리 + 꼬리 쉼표 제거. */
function tidy(s: string): string {
  return s.replace(/\s+/gu, ' ').trim().replace(/,+$/u, '').trim();
}

/**
 * 괄호 주기에 적힌 단지/건물 이름. 없으면 빈 문자열.
 *
 * ```
 * parenBuildingName('... 403호(두암동, 주공2단지@)') → '주공2단지@'
 * parenBuildingName('... 403호(두암동 871-28)')      → ''   (법정동·지번뿐)
 * parenBuildingName('광주 북구 매곡로 92')            → ''   (괄호 없음)
 * ```
 */
export function parenBuildingName(address: string): string {
  if (!address) return '';
  const m = LAST_PAREN_RE.exec(address);
  if (!m) return '';
  const parts = m[1]
    .split(',')
    .map((part) => part.trim())
    .filter((part) => part.length > 0 && !isAreaNote(part));
  return parts[parts.length - 1] ?? '';
}

/** 건물 동 표기('101동' · 'A동'). 없으면 빈 문자열. */
export function buildingDong(address: string): string {
  const { dongNum, dongTxt } = parseUnit(address);
  if (dongNum !== null) return `${dongNum}동`;
  return dongTxt ? `${dongTxt}동` : '';
}

/**
 * 단지/건물 이름(동·호 제외).
 *
 * 1. 괄호 주기 안에 건물명이 있으면 그것 — '(운암동, 광주첨단이지more)' → '광주첨단이지more'
 * 2. 없으면 괄호를 떼고 동·호·층을 지운 도로명 주소 — '광주 북구 매곡로 92'
 *
 * ```
 * complexName('광주 북구 삼정로 7, 207동 403호(두암동, 주공2단지@)') → '주공2단지@'
 * complexName('광주 북구 설죽로 507 호반리젠시빌 101동 1203호')      → '광주 북구 설죽로 507 호반리젠시빌'
 * ```
 */
export function complexName(address: string): string {
  if (!address) return '';
  const fromParen = parenBuildingName(address);
  if (fromParen) return fromParen;
  const fallback = tidy(stripUnit(address.replace(PAREN_RE, ' ')));
  return fallback || tidy(address);
}
