/**
 * Hook to fetch daily ranking data from Firestore.
 */
import { collection, doc, getDoc, getDocs, limit, orderBy, query } from "firebase/firestore";
import { useCallback, useEffect, useState } from "react";
import { db } from "../firebase";

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
  displayName: string;
  trendScore: number;
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
      let itemsRef = collection(db, "daily_rankings", date, "items");
      let hasPublishedRun = false;

      // Read the day document first for legacy fallback and status messaging.
      const metaSnap = await getDoc(doc(db, "daily_rankings", date));
      if (metaSnap.exists()) {
        activeMeta = metaSnap.data() as RankingMeta;
      }

      // Prefer the latest published run snapshot for the selected day.
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
        console.warn("Failed to query versioned daily ranking runs, falling back to legacy path.", runQueryError);
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
      const rankingItems: RankingItem[] = snapshot.docs.map((d) => {
        const data = d.data();
        return {
          rank: data.rank,
          candidateId: data.candidateId,
          candidateType: data.candidateType,
          displayName: data.displayName,
          trendScore: data.trendScore,
          breakdownBuckets: data.breakdownBuckets || [],
          sparkline7d: data.sparkline7d || [],
          evidenceTop3: data.evidenceTop3 || [],
          summary: data.summary || "",
          power: data.power,
        };
      });
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
