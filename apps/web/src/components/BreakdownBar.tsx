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

const BUCKET_COLORS_INVERTED: Record<string, string> = {
  TRENDS: "#ffffff",
  YOUTUBE: "#ffffffcc",
  X: "#ffffff99",
  NEWS_RSS: "#ffffff80",
  RANKINGS_STREAM: "#ffffffdd",
  MUSIC: "#ffffff99",
  MAGAZINES: "#ffffff66",
  INSTAGRAM_BOOST: "#ffffffee",
};

interface BreakdownBarProps {
  buckets: BucketScore[];
  totalScore: number;
  inverted?: boolean;
}

export default function BreakdownBar({ buckets, totalScore, inverted = false }: BreakdownBarProps) {
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
        className="flex h-2 overflow-hidden"
        style={{
          transformOrigin: "left center",
          backgroundColor: inverted ? "rgba(255,255,255,0.15)" : "#e8e6e0",
          transition: "background-color 0.3s",
        }}
      >
        {buckets.map((b) => {
          const pct = (b.score / totalScore) * 100;
          if (pct <= 0) return null;
          const colors = inverted ? BUCKET_COLORS_INVERTED : BUCKET_COLORS;
          return (
            <div
              key={b.bucket}
              style={{
                width: `${pct}%`,
                backgroundColor: colors[b.bucket] || (inverted ? "#ffffff66" : "#1925aa66"),
                transition: "background-color 0.3s",
              }}
              title={`${b.bucket}: ${b.score.toFixed(1)}`}
            />
          );
        })}
      </div>
      <div className="flex flex-wrap gap-x-3 gap-y-1 mt-2">
        {buckets.map((b) => {
          const colors = inverted ? BUCKET_COLORS_INVERTED : BUCKET_COLORS;
          return (
            <div key={b.bucket} className="flex items-center">
              <div
                className="w-2 h-2 mr-1"
                style={{
                  backgroundColor: colors[b.bucket] || (inverted ? "#ffffff66" : "#1925aa66"),
                  transition: "background-color 0.3s",
                }}
              />
              <span
                className="oci-label-sm"
                style={{
                  color: inverted ? "rgba(255,255,255,0.6)" : "rgba(25,37,170,0.6)",
                  transition: "color 0.3s",
                }}
              >
                {b.bucket} ({b.score.toFixed(1)})
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}
