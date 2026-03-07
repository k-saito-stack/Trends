/**
 * Hook to fetch daily ranking data from Firestore.
 */
import { collection, doc, getDoc, getDocs, limit, orderBy, query } from "firebase/firestore";
import { useCallback, useEffect, useState } from "react";
import { db } from "../firebase";

const PUBLIC_RANKING_COLLECTION = import.meta.env.VITE_PUBLIC_RANKING_COLLECTION || "daily_rankings";

export interface BucketScore {
  bucket: string;
  score: number;
}

export interface EvidenceItem {
  sourceId: string;
  title: string;
  url: string;
  publishedAt?: string;
  metric?: string;
  snippet?: string;
}

export interface RankingItem {
  rank: number;
  candidateId: string;
  candidateType: string;
  candidateKind?: string;
  displayName: string;
  trendScore: number;
  comingScore?: number;
  massHeat?: number;
  primaryScore?: number;
  lane?: string;
  maturity?: number;
  sourceFamilies?: string[];
  breakdownBuckets: BucketScore[];
  sparkline7d: (number | null)[];
  evidenceTop3: EvidenceItem[];
  summary: string;
  power?: number;
}

export interface RankingMeta {
  date: string;
  generatedAt: string;
  runId: string;
  topK: number;
  degradeState: Record<string, unknown>;
  algorithmVersion?: string;
  status?: string;
  publishedAt?: string;
  latestPublishedRunId?: string;
}

export function useDailyRanking(date: string) {
  const [items, setItems] = useState<RankingItem[]>([]);
  const [meta, setMeta] = useState<RankingMeta | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchRanking = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      let activeMeta: RankingMeta | null = null;
      let itemsRef = collection(db, PUBLIC_RANKING_COLLECTION, date, "items");
      let hasPublishedRun = false;

      // Read the day document first for legacy fallback and status messaging.
      const metaSnap = await getDoc(doc(db, PUBLIC_RANKING_COLLECTION, date));
      if (metaSnap.exists()) {
        activeMeta = metaSnap.data() as RankingMeta;
      }

      // Prefer the latest published run snapshot only for the legacy public collection.
      if (PUBLIC_RANKING_COLLECTION === "daily_rankings") {
        try {
          const runsRef = collection(db, "daily_rankings", date, "runs");
          const latestPublishedRunQuery = query(runsRef, orderBy("publishedAt", "desc"), limit(1));
          const latestPublishedRunSnap = await getDocs(latestPublishedRunQuery);
          if (!latestPublishedRunSnap.empty) {
            const latestRunDoc = latestPublishedRunSnap.docs[0];
            activeMeta = {
              ...(latestRunDoc.data() as RankingMeta),
              runId: latestRunDoc.data().runId || latestRunDoc.id,
            };
            itemsRef = collection(db, "daily_rankings", date, "runs", latestRunDoc.id, "items");
            hasPublishedRun = true;
          }
        } catch (runQueryError) {
          console.warn(
            "Failed to query versioned daily ranking runs, falling back to legacy path.",
            runQueryError,
          );
        }
      }

      if (activeMeta) {
        setMeta(activeMeta);
      } else {
        setMeta(null);
      }

      // If no published snapshot exists yet, surface the current batch state.
      if (!hasPublishedRun && activeMeta?.status === "BUILDING") {
        setError("ランキングを更新中です。しばらくお待ちください。");
        setItems([]);
        return;
      }
      if (!hasPublishedRun && activeMeta?.status === "FAILED") {
        setError("ランキングの生成に失敗しました。");
        setItems([]);
        return;
      }

      // Fetch items for the latest published run, or fall back to the legacy path.
      const q = query(itemsRef, orderBy("rank"));
      const snapshot = await getDocs(q);
      const rankingItems: RankingItem[] = snapshot.docs.map((d) => mapRankingItem(d.data()));
      setItems(rankingItems);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to fetch ranking");
    } finally {
      setLoading(false);
    }
  }, [date]);

  useEffect(() => {
    fetchRanking();
  }, [fetchRanking]);

  return { items, meta, loading, error, refetch: fetchRanking };
}

function mapRankingItem(data: Record<string, unknown>): RankingItem {
  const evidenceTop3 = Array.isArray(data.evidenceTop3)
    ? (data.evidenceTop3 as EvidenceItem[])
    : Array.isArray(data.evidenceTop5)
      ? ((data.evidenceTop5 as EvidenceItem[]).slice(0, 3))
      : [];

  const primaryScore = asNumber(data.primaryScore);
  const trendScore = asNumber(data.trendScore) ?? primaryScore ?? 0;

  return {
    rank: asNumber(data.rank) ?? 0,
    candidateId: String(data.candidateId || ""),
    candidateType: String(data.candidateType || ""),
    candidateKind: data.candidateKind ? String(data.candidateKind) : undefined,
    displayName: String(data.displayName || ""),
    trendScore,
    comingScore: asNumber(data.comingScore),
    massHeat: asNumber(data.massHeat),
    primaryScore,
    lane: data.lane ? String(data.lane) : undefined,
    maturity: asNumber(data.maturity),
    sourceFamilies: Array.isArray(data.sourceFamilies) ? (data.sourceFamilies as string[]) : [],
    breakdownBuckets: Array.isArray(data.breakdownBuckets) ? (data.breakdownBuckets as BucketScore[]) : [],
    sparkline7d: Array.isArray(data.sparkline7d) ? (data.sparkline7d as (number | null)[]) : [],
    evidenceTop3,
    summary: String(data.summary || ""),
    power: asNumber(data.power),
  };
}

function asNumber(value: unknown): number | undefined {
  return typeof value === "number" ? value : undefined;
}
