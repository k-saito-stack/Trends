/**
 * Single trend candidate card with expandable detail.
 * Design: white card (#f0f1f3) on blue background, blue text, no border-radius.
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
    <div className="white-card overflow-hidden">
      {/* Collapsed header */}
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full px-5 py-4 flex items-center gap-3 text-left
                   hover:bg-blue-50/30 transition-colors"
      >
        {/* Rank with blue dot */}
        <div className="flex flex-col items-center shrink-0 w-8">
          <span className="text-blue-600 text-lg font-bold">{item.rank}</span>
        </div>

        {/* Name + type tag */}
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <span className="font-medium text-blue-600 truncate">
              {item.displayName}
            </span>
            <span className="text-[10px] text-blue-600/60 border border-blue-600/30 px-2 py-0.5 shrink-0 uppercase tracking-wider">
              {item.candidateType}
            </span>
          </div>
        </div>

        {/* Sparkline + score + arrow */}
        <div className="flex items-center gap-3 shrink-0">
          <Sparkline data={item.sparkline7d} />
          <span className="font-bold text-blue-600 text-sm w-12 text-right">
            {item.trendScore.toFixed(1)}
          </span>
          <svg
            xmlns="http://www.w3.org/2000/svg"
            className={`h-4 w-4 text-blue-600/50 transition-transform ${expanded ? "rotate-180" : ""}`}
            fill="none" viewBox="0 0 24 24" stroke="currentColor"
          >
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
          </svg>
        </div>
      </button>

      {/* Expanded detail */}
      {expanded && (
        <div className="px-5 pb-5 border-t border-blue-600/10">
          {/* Summary */}
          {item.summary && (
            <p className="text-blue-600 text-xs leading-relaxed mt-3 mb-4">
              {item.summary}
            </p>
          )}

          {/* Breakdown */}
          {item.breakdownBuckets.length > 0 && (
            <div className="mb-4">
              <div className="flex items-start mb-2">
                <div className="w-1 h-4 bg-blue-600 mr-2 mt-0.5" />
                <h4 className="text-blue-600 text-xs font-medium">スコア内訳</h4>
              </div>
              <BreakdownBar
                buckets={item.breakdownBuckets}
                totalScore={item.trendScore}
              />
            </div>
          )}

          {/* Evidence */}
          {item.evidenceTop3.length > 0 && (
            <div className="mb-3">
              <div className="flex items-start mb-2">
                <div className="w-1 h-4 bg-blue-600 mr-2 mt-0.5" />
                <h4 className="text-blue-600 text-xs font-medium">エビデンス</h4>
              </div>
              <div className="space-y-2 pl-3">
                {item.evidenceTop3.map((ev, i) => (
                  <div key={i} className="flex items-start gap-2">
                    <div className="w-2 h-2 bg-blue-600 rounded-full mt-1.5 shrink-0" />
                    <div className="text-xs">
                      <span className="text-blue-600/50 mr-1">
                        [{ev.sourceId}]
                      </span>
                      {ev.url ? (
                        <a
                          href={ev.url}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-blue-600 underline underline-offset-2"
                        >
                          {ev.title || ev.url}
                        </a>
                      ) : (
                        <span className="text-blue-600">{ev.title}</span>
                      )}
                      {ev.metric && (
                        <span className="text-blue-600/40 ml-1">
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
            <div className="flex items-center gap-2 mt-2">
              <div className="w-2 h-2 pattern-bg" />
              <span className="text-blue-600/60 text-xs">
                Power: {item.power.toFixed(1)}
              </span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
