/**
 * Single trend candidate card with expandable detail.
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
    <div className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
      {/* Collapsed view */}
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full px-4 py-3 flex items-center gap-3 text-left
                   hover:bg-gray-50 transition-colors"
      >
        <span className="text-lg font-bold text-gray-400 w-8 text-right">
          {item.rank}
        </span>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <span className="font-medium text-gray-800 truncate">
              {item.displayName}
            </span>
            <span className="text-xs text-gray-400 shrink-0">
              {item.candidateType}
            </span>
          </div>
        </div>
        <div className="flex items-center gap-3 shrink-0">
          <Sparkline data={item.sparkline7d} />
          <span className="font-semibold text-blue-600 text-sm w-14 text-right">
            {item.trendScore.toFixed(1)}
          </span>
          <span className="text-gray-400 text-sm">
            {expanded ? "\u25B2" : "\u25BC"}
          </span>
        </div>
      </button>

      {/* Expanded detail */}
      {expanded && (
        <div className="px-4 pb-4 border-t border-gray-100">
          {/* Summary */}
          {item.summary && (
            <p className="text-sm text-gray-600 mt-3 mb-3">
              {item.summary}
            </p>
          )}

          {/* Breakdown */}
          {item.breakdownBuckets.length > 0 && (
            <div className="mb-3">
              <h4 className="text-xs font-medium text-gray-500 mb-1">
                スコア内訳
              </h4>
              <BreakdownBar
                buckets={item.breakdownBuckets}
                totalScore={item.trendScore}
              />
            </div>
          )}

          {/* Evidence */}
          {item.evidenceTop3.length > 0 && (
            <div>
              <h4 className="text-xs font-medium text-gray-500 mb-1">
                エビデンス
              </h4>
              <ul className="space-y-1">
                {item.evidenceTop3.map((ev, i) => (
                  <li key={i} className="text-sm">
                    <span className="text-xs text-gray-400 mr-1">
                      [{ev.sourceId}]
                    </span>
                    {ev.url ? (
                      <a
                        href={ev.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-blue-600 hover:underline"
                      >
                        {ev.title || ev.url}
                      </a>
                    ) : (
                      <span className="text-gray-600">{ev.title}</span>
                    )}
                    {ev.metric && (
                      <span className="text-xs text-gray-400 ml-1">
                        ({ev.metric})
                      </span>
                    )}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {/* Power score */}
          {item.power != null && (
            <p className="text-xs text-gray-400 mt-2">
              Power: {item.power.toFixed(1)}
            </p>
          )}
        </div>
      )}
    </div>
  );
}
