/**
 * core/index.ts — Python `src/core`를 옮긴 순수 로직 모음.
 * React·DOM 의존이 없고 `../types`의 계약 타입만 쓴다.
 */

export {
  buildingDong,
  complexName,
  parenBuildingName,
  parseUnit,
  complexKey,
  stripUnit,
  unitSortKey,
  compareUnit,
  compareUnitAddr,
} from './address';
export type { ParsedUnit, UnitSortKey } from './address';

export { haversineKm, haversineFallbackSec } from './geo';

export {
  BASE_HULL_RADIUS_M,
  MIN_HULL_RADIUS_M,
  bufferedHull,
  convexHull,
  hullRadiusM,
  spreadM,
} from './hull';

export { buildPrimaryGroups, buildClusters } from './grouping';

export { normalize, extractRoadPart, verifyAddress } from './verify';
export type { VerifyResult } from './verify';

export {
  buildBlocks,
  pairsNeeded,
  orderWithinCluster,
  suggestEntryExit,
} from './intraRoute';
export type {
  Block,
  TimeSecFn,
  IntraRouteResult,
  EntryExitSuggestion,
} from './intraRoute';
