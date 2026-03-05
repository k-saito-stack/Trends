/**
 * Trend card — OCI style: white card, 1px blue border, mono labels.
 * Expandable detail with breakdown + evidence.
 */
import { useState } from "react";
import type { RankingItem } from "../hooks/useDailyRanking";
import BreakdownBar from "./BreakdownBar";
import Sparkline from "./Sparkline";

interface TrendCardProps {
  item: RankingItem;
}

export default function TrendCard({ item }: TrendCardProps) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="oci-card overflow-hidden">
      {/* Collapsed header */}
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full px-5 py-4 flex items-center gap-4 text-left
                   hover:bg-oci-mercury/50 transition-colors duration-200"
      >
        {/* Rank number */}
        <div className="flex flex-col items-center shrink-0 w-8">
          <span className="oci-heading text-oci-blue text-xl">{item.rank}</span>
        </div>

        {/* 1px vertical separator */}
        <div className="w-px h-8 bg-oci-blue/20 shrink-0" />

        {/* Name + type tag */}
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <span className="font-sans font-medium text-oci-blue truncate">
              {item.displayName}
            </span>
            <span className="oci-label text-oci-blue/40 border border-oci-blue/20 px-2 py-0.5 shrink-0 text-[0.625rem]">
              {item.candidateType}
            </span>
          </div>
        </div>

        {/* Sparkline + score + chevron */}
        <div className="flex items-center gap-3 shrink-0">
          <Sparkline data={item.sparkline7d} />
          <span className="font-mono font-normal text-oci-blue text-sm w-12 text-right">
            {item.trendScore.toFixed(1)}
          </span>
          <svg
            xmlns="http://www.w3.org/2000/svg"
            className={`h-4 w-4 text-oci-blue/30 transition-transform duration-200 ${expanded ? "rotate-180" : ""}`}
            fill="none" viewBox="0 0 24 24" stroke="currentColor"
          >
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M19 9l-7 7-7-7" />
          </svg>
        </div>
      </button>

      {/* Expanded detail */}
      {expanded && (
        <div className="px-5 pb-5 border-t border-oci-blue/10">
          {/* Summary */}
          {item.summary && (
            <p className="text-oci-blue text-xs leading-relaxed mt-4 mb-5 font-sans">
              {item.summary}
            </p>
          )}

          {/* Breakdown */}
          {item.breakdownBuckets.length > 0 && (
            <div className="mb-5">
              <h4 className="oci-label text-oci-blue/50 mb-2 text-[0.625rem]">
                Score Breakdown
              </h4>
              <BreakdownBar
                buckets={item.breakdownBuckets}
                totalScore={item.trendScore}
              />
            </div>
          )}

          {/* Evidence */}
          {item.evidenceTop3.length > 0 && (
            <div className="mb-3">
              <h4 className="oci-label text-oci-blue/50 mb-2 text-[0.625rem]">
                Evidence
              </h4>
              <div className="space-y-2">
                {item.evidenceTop3.map((ev, i) => (
                  <div key={i} className="flex items-start gap-3 pl-1">
                    <div className="w-1 bg-oci-blue/20 shrink-0 mt-0.5" style={{ minHeight: "1rem" }} />
                    <div className="text-xs font-sans">
                      <span className="oci-label text-oci-blue/30 mr-1 text-[0.625rem]">
                        {ev.sourceId}
                      </span>
                      {ev.url ? (
                        <a
                          href={ev.url}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-oci-blue underline underline-offset-2 oci-hover"
                        >
                          {ev.title || ev.url}
                        </a>
                      ) : (
                        <span className="text-oci-blue">{ev.title}</span>
                      )}
                      {ev.metric && (
                        <span className="text-oci-blue/30 ml-1">
                          ({ev.metric})
                        </span>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Power score */}
          {item.power != null && (
            <div className="flex items-center gap-2 mt-3 pt-3 border-t border-oci-blue/10">
              <span className="oci-label text-oci-blue/30 text-[0.625rem]">
                Power: {item.power.toFixed(1)}
              </span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
