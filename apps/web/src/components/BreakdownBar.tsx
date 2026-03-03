/**
 * Horizontal stacked bar showing bucket score breakdown.
 */
import type { BucketScore } from "../hooks/useDailyRanking";

const BUCKET_COLORS: Record<string, string> = {
  TRENDS: "#3b82f6",
  YOUTUBE: "#ef4444",
  X: "#1d9bf0",
  NEWS_RSS: "#f59e0b",
  RANKINGS_STREAM: "#8b5cf6",
  MUSIC: "#ec4899",
  MAGAZINES: "#10b981",
  INSTAGRAM_BOOST: "#f97316",
};

interface BreakdownBarProps {
  buckets: BucketScore[];
  totalScore: number;
}

export default function BreakdownBar({ buckets, totalScore }: BreakdownBarProps) {
  if (!buckets.length || totalScore <= 0) return null;

  return (
    <div>
      <div className="flex h-3 rounded-full overflow-hidden bg-gray-100">
        {buckets.map((b) => {
          const pct = (b.score / totalScore) * 100;
          if (pct <= 0) return null;
          return (
            <div
              key={b.bucket}
              style={{
                width: `${pct}%`,
                backgroundColor: BUCKET_COLORS[b.bucket] || "#9ca3af",
              }}
              title={`${b.bucket}: ${b.score.toFixed(1)}`}
            />
          );
        })}
      </div>
      <div className="flex flex-wrap gap-2 mt-1">
        {buckets.map((b) => (
          <span key={b.bucket} className="text-xs text-gray-500">
            <span
              className="inline-block w-2 h-2 rounded-full mr-1"
              style={{ backgroundColor: BUCKET_COLORS[b.bucket] || "#9ca3af" }}
            />
            {b.bucket} ({b.score.toFixed(1)})
          </span>
        ))}
      </div>
    </div>
  );
}
