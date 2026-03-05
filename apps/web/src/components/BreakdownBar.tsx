/**
 * Breakdown bar — OCI blue monochrome palette.
 */
import type { BucketScore } from "../hooks/useDailyRanking";

/* Monochrome blue palette using opacity variations of #1925aa */
const BUCKET_COLORS: Record<string, string> = {
  TRENDS: "#1925aa",
  YOUTUBE: "#1925aacc",
  X: "#1925aa99",
  NEWS_RSS: "#1925aa80",
  RANKINGS_STREAM: "#1925aadd",
  MUSIC: "#1925aa99",
  MAGAZINES: "#1925aa66",
  INSTAGRAM_BOOST: "#1925aaee",
};

interface BreakdownBarProps {
  buckets: BucketScore[];
  totalScore: number;
}

export default function BreakdownBar({ buckets, totalScore }: BreakdownBarProps) {
  if (!buckets.length || totalScore <= 0) return null;

  return (
    <div>
      <div className="flex h-3 overflow-hidden bg-oci-mercury">
        {buckets.map((b) => {
          const pct = (b.score / totalScore) * 100;
          if (pct <= 0) return null;
          return (
            <div
              key={b.bucket}
              style={{
                width: `${pct}%`,
                backgroundColor: BUCKET_COLORS[b.bucket] || "#1925aa66",
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
              style={{ backgroundColor: BUCKET_COLORS[b.bucket] || "#1925aa66" }}
            />
            <span className="oci-label text-oci-blue/60 text-[0.625rem]">
              {b.bucket} ({b.score.toFixed(1)})
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
