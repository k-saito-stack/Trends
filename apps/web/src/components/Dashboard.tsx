/**
 * Main dashboard showing the daily ranking cards.
 */
import { useDailyRanking } from "../hooks/useDailyRanking";
import TrendCard from "./TrendCard";

interface DashboardProps {
  date: string;
}

export default function Dashboard({ date }: DashboardProps) {
  const { items, meta, loading, error } = useDailyRanking(date);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="text-gray-400">読み込み中...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="text-red-500 text-sm">
          エラー: {error}
        </div>
      </div>
    );
  }

  if (items.length === 0) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="text-gray-400 text-center">
          <p className="text-lg mb-2">{date} のデータがありません</p>
          <p className="text-sm">バッチが実行されるとデータが表示されます</p>
        </div>
      </div>
    );
  }

  return (
    <div>
      {meta && (
        <div className="text-xs text-gray-400 mb-3 px-1">
          生成: {new Date(meta.generatedAt).toLocaleString("ja-JP")}
          {" / "}Run: {meta.runId.slice(0, 8)}...
        </div>
      )}
      <div className="space-y-2">
        {items.map((item) => (
          <TrendCard key={item.candidateId} item={item} />
        ))}
      </div>
    </div>
  );
}
