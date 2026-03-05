/**
 * Breakdown bar — OCI blue monochrome.
 * Bar animates scaleX: 0→1 on mount via GSAP.
 */
import { useRef } from "react";
import type { BucketScore } from "../hooks/useDailyRanking";
import { gsap, useGSAP } from "../hooks/useGSAPSetup";

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
  const barRef = useRef<HTMLDivElement>(null);

  useGSAP(
    () => {
      gsap.fromTo(
        barRef.current,
        { scaleX: 0 },
        {
          scaleX: 1,
          duration: 0.5,
          ease: "power4.out",
          transformOrigin: "left center",
        },
      );
    },
    { scope: barRef },
  );

  if (!buckets.length || totalScore <= 0) return null;

  return (
    <div>
      <div
        ref={barRef}
        className="flex h-2 overflow-hidden bg-oci-mercury"
        style={{ transformOrigin: "left center" }}
      >
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
            <span className="oci-label-sm text-oci-blue/60">
              {b.bucket} ({b.score.toFixed(1)})
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
