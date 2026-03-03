/**
 * Hook to fetch daily ranking data from Firestore.
 */
import { collection, doc, getDoc, getDocs, orderBy, query } from "firebase/firestore";
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
      // Fetch metadata
      const metaSnap = await getDoc(doc(db, "daily_rankings", date));
      if (metaSnap.exists()) {
        setMeta(metaSnap.data() as RankingMeta);
      }

      // Fetch items subcollection
      const itemsRef = collection(db, "daily_rankings", date, "items");
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
