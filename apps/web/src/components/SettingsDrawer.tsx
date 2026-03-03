/**
 * Settings drawer (slide from right).
 * Shows source ON/OFF toggles and recent change logs.
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
          className="fixed inset-0 bg-black/30 z-40"
          onClick={onClose}
        />
      )}

      {/* Drawer */}
      <div
        className={`fixed top-0 right-0 h-full w-80 bg-white shadow-xl z-50
                     transform transition-transform duration-300
                     ${isOpen ? "translate-x-0" : "translate-x-full"}`}
      >
        <div className="p-4 border-b border-gray-200 flex items-center justify-between">
          <h2 className="font-bold text-gray-800">設定</h2>
          <button
            onClick={onClose}
            className="text-gray-400 hover:text-gray-600 text-xl"
          >
            x
          </button>
        </div>

        <div className="p-4 overflow-y-auto h-full pb-20">
          {/* Info */}
          <div className="mb-6">
            <p className="text-sm text-gray-500">
              ソースのON/OFFやアルゴリズム設定は
              Firestore Console から変更できます。
              変更はchange_logsに記録されます。
            </p>
          </div>

          {/* Change Logs */}
          <div>
            <h3 className="text-sm font-medium text-gray-700 mb-2">
              変更履歴（最新20件）
            </h3>
            {loadingLogs ? (
              <p className="text-xs text-gray-400">読み込み中...</p>
            ) : changeLogs.length === 0 ? (
              <p className="text-xs text-gray-400">変更履歴はまだありません</p>
            ) : (
              <ul className="space-y-2">
                {changeLogs.map((log) => (
                  <li
                    key={log.logId}
                    className="text-xs border border-gray-100 rounded p-2"
                  >
                    <div className="font-medium text-gray-700">
                      {log.documentPath}
                    </div>
                    <div className="text-gray-400">
                      {log.changedBy} / {new Date(log.changedAt).toLocaleString("ja-JP")}
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </div>
      </div>
    </>
  );
}
