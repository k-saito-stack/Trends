/**
 * Settings drawer — OCI style: white panel, blue accents, mono labels.
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
          className="fixed inset-0 z-40 bg-oci-navy/50 transition-opacity duration-300"
          onClick={onClose}
        />
      )}

      {/* Drawer */}
      <div
        className={`fixed top-0 right-0 h-full w-80 bg-white border-l border-oci-blue z-50
                     transform transition-transform duration-300
                     ${isOpen ? "translate-x-0" : "translate-x-full"}`}
      >
        {/* Header */}
        <div className="p-5 border-b border-oci-blue/20 flex items-center justify-between">
          <h2 className="oci-heading text-oci-blue text-lg">Settings</h2>
          <button
            onClick={onClose}
            className="oci-hover text-oci-blue/50 p-1"
          >
            <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        <div className="p-5 overflow-y-auto h-full pb-20 scroll-hide">
          {/* Info */}
          <div className="bg-oci-mercury p-4 mb-6 border border-oci-blue/10">
            <p className="text-oci-blue text-xs leading-relaxed font-sans">
              ソースのON/OFFやアルゴリズム設定は
              Firestore Console から変更できます。
              変更はchange_logsに記録されます。
            </p>
          </div>

          {/* Change Logs */}
          <div>
            <h3 className="oci-label text-oci-blue/50 mb-3 text-[0.625rem]">
              Change Logs (Latest 20)
            </h3>

            {loadingLogs ? (
              <p className="oci-label text-oci-blue/30 text-[0.625rem]">Loading...</p>
            ) : changeLogs.length === 0 ? (
              <p className="oci-label text-oci-blue/30 text-[0.625rem]">No logs yet</p>
            ) : (
              <div className="space-y-2">
                {changeLogs.map((log) => (
                  <div
                    key={log.logId}
                    className="border border-oci-blue/15 p-3"
                  >
                    <span className="text-oci-blue text-xs font-sans">
                      {log.documentPath}
                    </span>
                    <div className="oci-label text-oci-blue/30 text-[0.5625rem] mt-1">
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
