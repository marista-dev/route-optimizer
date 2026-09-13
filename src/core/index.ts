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
  IntraRouteResult,
  EntryExitSuggestion,
} from './intraRoute';
// TimeSecFn은 intraRoute/clusterRoute가 공유하는 계약이라 `../types`에 산다(F13).
export type { TimeSecFn } from '../types';

export { orderClusters } from './clusterRoute';
