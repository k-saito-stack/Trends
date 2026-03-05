/**
 * Main dashboard — OCI style: mercury bg, blue text, mono labels.
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
        <span className="oci-label text-oci-blue/40">Loading...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="oci-card p-6">
          <span className="oci-label text-oci-error">{error}</span>
        </div>
      </div>
    );
  }

  if (items.length === 0) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="oci-card p-10 text-center">
          <p className="oci-heading text-oci-blue text-xl mb-3">
            {date}
          </p>
          <p className="oci-label text-oci-blue/40">No data available</p>
        </div>
      </div>
    );
  }

  return (
    <div>
      {meta && (
        <div className="oci-label text-oci-blue/30 mb-4 text-[0.625rem]">
          Generated: {new Date(meta.generatedAt).toLocaleString("ja-JP")}
          {" / "}Run: {meta.runId.slice(0, 8)}...
        </div>
      )}
      <div className="space-y-3">
        {items.map((item) => (
          <TrendCard key={item.candidateId} item={item} />
        ))}
      </div>
    </div>
  );
}
