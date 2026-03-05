/**
 * Settings drawer — OCI style.
 * GSAP timeline open/close, stagger log entries, scramble title.
 */
import { collection, getDocs, limit, orderBy, query } from "firebase/firestore";
import { useEffect, useState, useRef, useCallback } from "react";
import { db } from "../firebase";
import { gsap } from "../hooks/useGSAPSetup";
import { useScrambleText } from "../hooks/useScrambleText";

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

  const backdropRef = useRef<HTMLDivElement>(null);
  const drawerRef = useRef<HTMLDivElement>(null);
  const titleRef = useRef<HTMLHeadingElement>(null);
  const contentRef = useRef<HTMLDivElement>(null);
  const { scramble } = useScrambleText();

  useEffect(() => {
    if (!isOpen) return;
    setLoadingLogs(true);
    const fetchLogs = async () => {
      try {
        const q = query(
          collection(db, "change_logs"),
          orderBy("changedAt", "desc"),
          limit(20),
        );
        const snap = await getDocs(q);
        const logs = snap.docs.map((d) => d.data() as ChangeLogEntry);
        setChangeLogs(logs);
      } catch {
        // Ignore
      } finally {
        setLoadingLogs(false);
      }
    };
    fetchLogs();
  }, [isOpen]);

  // Animate open
  useEffect(() => {
    if (isOpen) {
      gsap.set(backdropRef.current, { display: "block" });
      gsap.set(drawerRef.current, { display: "block" });

      const tl = gsap.timeline();
      tl.fromTo(
        backdropRef.current,
        { opacity: 0 },
        { opacity: 1, duration: 0.3, ease: "power4.out" },
      ).fromTo(
        drawerRef.current,
        { xPercent: 100 },
        { xPercent: 0, duration: 0.5, ease: "power4.out" },
        0,
      );

      if (titleRef.current) {
        setTimeout(() => {
          scramble(titleRef.current!, "SETTINGS");
        }, 300);
      }
    }
  }, [isOpen, scramble]);

  const handleClose = useCallback(() => {
    const tl = gsap.timeline({
      onComplete: () => {
        gsap.set(backdropRef.current, { display: "none" });
        gsap.set(drawerRef.current, { display: "none" });
        onClose();
      },
    });
    tl.to(drawerRef.current, {
      xPercent: 100,
      duration: 0.4,
      ease: "power4.inOut",
    }).to(backdropRef.current, { opacity: 0, duration: 0.3 }, 0);
  }, [onClose]);

  // Stagger log entries
  useEffect(() => {
    if (!loadingLogs && changeLogs.length > 0 && contentRef.current) {
      const entries = contentRef.current.querySelectorAll(".log-entry");
      gsap.fromTo(
        entries,
        { opacity: 0, y: 10 },
        {
          opacity: 1,
          y: 0,
          duration: 0.3,
          ease: "power4.out",
          stagger: 0.04,
        },
      );
    }
  }, [loadingLogs, changeLogs]);

  return (
    <>
      {/* Backdrop */}
      <div
        ref={backdropRef}
        className="fixed inset-0 z-40 bg-oci-navy/50"
        style={{ display: "none" }}
        onClick={handleClose}
      />

      {/* Drawer */}
      <div
        ref={drawerRef}
        className="fixed top-0 right-0 h-full w-80 bg-white border-l border-oci-blue z-50"
        style={{ display: "none" }}
      >
        <div className="p-5 border-b border-oci-blue/20 flex items-center justify-between">
          <h2 ref={titleRef} className="oci-heading text-oci-blue text-lg">
            SETTINGS
          </h2>
          <button
            onClick={handleClose}
            className="text-oci-blue/30 hover:text-oci-blue transition-colors duration-300 p-1"
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              className="h-5 w-5"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={1.5}
                d="M6 18L18 6M6 6l12 12"
              />
            </svg>
          </button>
        </div>

        <div className="p-5 overflow-y-auto h-full pb-20 hide-native-scrollbar">
          <div className="bg-oci-mercury p-4 mb-6 border border-oci-blue/10">
            <p className="text-oci-blue text-xs leading-relaxed font-sans">
              ソースのON/OFFやアルゴリズム設定は Firestore Console
              から変更できます。変更はchange_logsに記録されます。
            </p>
          </div>

          <div ref={contentRef}>
            <h3 className="oci-label-sm text-oci-blue/50 mb-3">
              Change Logs (Latest 20)
            </h3>

            {loadingLogs ? (
              <p className="oci-label-sm text-oci-blue/30">Loading...</p>
            ) : changeLogs.length === 0 ? (
              <p className="oci-label-sm text-oci-blue/30">No logs yet</p>
            ) : (
              <div className="space-y-2">
                {changeLogs.map((log) => (
                  <div
                    key={log.logId}
                    className="log-entry border border-oci-blue/10 p-3"
                    style={{ opacity: 0 }}
                  >
                    <span className="text-oci-blue text-xs font-sans">
                      {log.documentPath}
                    </span>
                    <div className="oci-label-sm text-oci-blue/30 text-[0.5625rem] mt-1">
                      {log.changedBy} /{" "}
                      {new Date(log.changedAt).toLocaleString("ja-JP")}
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
