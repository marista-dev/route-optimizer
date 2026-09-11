import { useMemo } from 'react';

import { RouteLayer } from './RouteLayer';
import type { Cluster, LatLng } from '../types';

export interface OrderLinkLayerProps {
  /** 전체 클러스터 목록 */
  clusters: Cluster[];
  /** 사용자가 클릭한 방문 순서(클러스터 id 순서열) */
  clusterOrder: number[];
  /** 출발지. 있으면 첫 클러스터 앞에 붙인다 */
  origin?: LatLng | null;
}

/** 핸드오프의 순서 점선 색(브랜드 파랑). */
const DEFAULT_COLOR = '#1E4ED8';

/** S4에서 클릭한 순서대로 클러스터 중심을 잇는 점선. */
export function OrderLinkLayer({ clusters, clusterOrder, origin }: OrderLinkLayerProps) {
  const paths = useMemo<LatLng[][]>(() => {
    const centroidById = new Map(clusters.map((c) => [c.id, c.centroid]));
    const points: LatLng[] = [];
    if (origin) points.push({ lat: origin.lat, lon: origin.lon });
    for (const id of clusterOrder) {
      const centroid = centroidById.get(id);
      if (centroid) points.push(centroid);
    }
    return points.length >= 2 ? [points] : [];
  }, [clusters, clusterOrder, origin]);

  return <RouteLayer paths={paths} style="dashed" color={DEFAULT_COLOR} />;
}
