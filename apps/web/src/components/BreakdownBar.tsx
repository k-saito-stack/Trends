/**
 * Horizontal stacked bar — blue gradient design system.
 */
import type { BucketScore } from "../hooks/useDailyRanking";

const BUCKET_COLORS: Record<string, string> = {
  TRENDS: "#2563eb",
  YOUTUBE: "#1d4ed8",
  X: "#3b82f6",
  NEWS_RSS: "#60a5fa",
  RANKINGS_STREAM: "#1e40af",
  MUSIC: "#3b82f6",
  MAGAZINES: "#93c5fd",
  INSTAGRAM_BOOST: "#1e3a8a",
};

interface BreakdownBarProps {
  buckets: BucketScore[];
  totalScore: number;
}

export default function BreakdownBar({ buckets, totalScore }: BreakdownBarProps) {
  if (!buckets.length || totalScore <= 0) return null;

  return (
    <div>
      <div className="flex h-4 overflow-hidden bg-blue-100">
        {buckets.map((b) => {
          const pct = (b.score / totalScore) * 100;
          if (pct <= 0) return null;
          return (
            <div
              key={b.bucket}
              style={{
                width: `${pct}%`,
                backgroundColor: BUCKET_COLORS[b.bucket] || "#93c5fd",
              }}
              title={`${b.bucket}: ${b.score.toFixed(1)}`}
            />
          );
        })}
      </div>
      <div className="flex flex-wrap gap-x-3 gap-y-1 mt-2">
        {buckets.map((b) => (
          <div key={b.bucket} className="flex items-center">
            <div
              className="w-2 h-2 mr-1"
              style={{ backgroundColor: BUCKET_COLORS[b.bucket] || "#93c5fd" }}
            />
            <span className="text-blue-600 text-[10px]">
              {b.bucket} ({b.score.toFixed(1)})
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
