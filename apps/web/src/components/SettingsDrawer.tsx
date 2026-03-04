/**
 * Settings drawer — blue themed, slides from right.
 */
import { collection, getDocs, limit, orderBy, query } from "firebase/firestore";
import { useEffect, useState } from "react";
import { db } from "../firebase";

interface ChangeLogEntry {
  logId: string;
  documentPath: string;
  changedBy: string;
  changedAt: string;
}

interface SettingsDrawerProps {
  isOpen: boolean;
  onClose: () => void;
}

export default function SettingsDrawer({ isOpen, onClose }: SettingsDrawerProps) {
  const [changeLogs, setChangeLogs] = useState<ChangeLogEntry[]>([]);
  const [loadingLogs, setLoadingLogs] = useState(false);

  useEffect(() => {
    if (!isOpen) return;
    setLoadingLogs(true);
    const fetchLogs = async () => {
      try {
        const q = query(
          collection(db, "change_logs"),
          orderBy("changedAt", "desc"),
          limit(20)
        );
        const snap = await getDocs(q);
        const logs = snap.docs.map((d) => d.data() as ChangeLogEntry);
        setChangeLogs(logs);
      } catch {
        // Ignore errors (may not have data yet)
      } finally {
        setLoadingLogs(false);
      }
    };
    fetchLogs();
  }, [isOpen]);

  return (
    <>
      {/* Backdrop */}
      {isOpen && (
        <div
          className="fixed inset-0 z-40"
          style={{ backgroundColor: "rgba(0,57,214,0.7)" }}
          onClick={onClose}
        />
      )}

      {/* Drawer */}
      <div
        className={`fixed top-0 right-0 h-full w-80 white-card z-50
                     transform transition-transform duration-300
                     ${isOpen ? "translate-x-0" : "translate-x-full"}`}
      >
        {/* Header */}
        <div className="p-4 border-b border-blue-600/20 flex items-center justify-between">
          <div className="flex items-start">
            <div className="w-1 h-6 bg-blue-600 mr-3" />
            <h2 className="text-blue-600 font-medium text-lg">設定</h2>
          </div>
          <button
            onClick={onClose}
            className="text-blue-600/50 hover:text-blue-600"
          >
            <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        <div className="p-4 overflow-y-auto h-full pb-20 scroll-hide">
          {/* Info */}
          <div className="bg-blue-50 p-3 mb-6">
            <p className="text-blue-600 text-xs leading-relaxed">
              ソースのON/OFFやアルゴリズム設定は
              Firestore Console から変更できます。
              変更はchange_logsに記録されます。
            </p>
          </div>

          {/* Change Logs */}
          <div>
            <div className="flex items-start mb-3">
              <div className="w-1 h-4 bg-blue-600 mr-2 mt-0.5" />
              <h3 className="text-blue-600 text-xs font-medium">
                変更履歴（最新20件）
              </h3>
            </div>
            {loadingLogs ? (
              <p className="text-blue-600/40 text-xs">読み込み中...</p>
            ) : changeLogs.length === 0 ? (
              <p className="text-blue-600/40 text-xs">変更履歴はまだありません</p>
            ) : (
              <div className="space-y-2">
                {changeLogs.map((log) => (
                  <div
                    key={log.logId}
                    className="border border-blue-600/20 p-3"
                  >
                    <div className="flex items-center gap-2">
                      <div className="w-2 h-2 bg-blue-600 rounded-full shrink-0" />
                      <span className="text-blue-600 text-xs font-medium">
                        {log.documentPath}
                      </span>
                    </div>
                    <div className="text-blue-600/50 text-[10px] mt-1 pl-4">
                      {log.changedBy} / {new Date(log.changedAt).toLocaleString("ja-JP")}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>
    </>
  );
}
