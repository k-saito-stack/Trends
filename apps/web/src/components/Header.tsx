/**
 * Header — OCI style.
 * Blue bg, 2 rows:
 *   Row 1: TRENDS + SETTINGS/LOGOUT buttons
 *   Row 2: Date nav + Generated + Run + email
 */
import { useRef, useEffect } from "react";
import { gsap } from "../hooks/useGSAPSetup";
import { useMagnetic } from "../hooks/useMagnetic";
import { useScrambleText } from "../hooks/useScrambleText";
import DatePicker from "./DatePicker";

interface HeaderProps {
  date: string;
  onDateChange: (date: string) => void;
  onSettingsClick: () => void;
  onLogout: () => void;
  userEmail: string | null;
  generatedAt: string | null;
  runId: string | null;
}

export default function Header({
  date,
  onDateChange,
  onSettingsClick,
  onLogout,
  userEmail,
  generatedAt,
  runId,
}: HeaderProps) {
  const titleRef = useRef<HTMLHeadingElement>(null);
  const logoutBgRef = useRef<HTMLDivElement>(null);
  const logoutTextRef = useRef<HTMLSpanElement>(null);
  const settingsBgRef = useRef<HTMLDivElement>(null);
  const settingsTextRef = useRef<HTMLSpanElement>(null);
  const { scramble } = useScrambleText();

  // Magnetic cursor for interactive elements
  const magLogout = useMagnetic(0.3);
  const magSettings = useMagnetic(0.3);
  const magPrev = useMagnetic(0.4);
  const magNext = useMagnetic(0.4);

  // Auto-scramble TRENDS title every 8 seconds
  useEffect(() => {
    const initTimer = setTimeout(() => {
      if (titleRef.current) scramble(titleRef.current, "TRENDS", 500);
    }, 800);

    const interval = setInterval(() => {
      if (titleRef.current) scramble(titleRef.current, "TRENDS", 500);
    }, 8000);

    return () => {
      clearTimeout(initTimer);
      clearInterval(interval);
    };
  }, [scramble]);

  const handlePrevDay = () => {
    const d = new Date(date);
    d.setDate(d.getDate() - 1);
    onDateChange(d.toISOString().split("T")[0]);
  };

  const handleNextDay = () => {
    const d = new Date(date);
    d.setDate(d.getDate() + 1);
    const today = new Date().toISOString().split("T")[0];
    const next = d.toISOString().split("T")[0];
    if (next <= today) onDateChange(next);
  };

  const handleTitleHover = () => {
    if (titleRef.current) scramble(titleRef.current, "TRENDS");
  };

  // Direction-aware hover helpers
  const getDirection = (e: React.MouseEvent, el: HTMLElement) => {
    const rect = el.getBoundingClientRect();
    const nx = ((e.clientX - rect.left) / rect.width - 0.5) * 2;
    const ny = ((e.clientY - rect.top) / rect.height - 0.5) * 2;
    if (Math.abs(nx) > Math.abs(ny)) {
      return nx > 0
        ? { from: { scaleX: 0, scaleY: 1 }, origin: "right center" }
        : { from: { scaleX: 0, scaleY: 1 }, origin: "left center" };
    }
    return ny > 0
      ? { from: { scaleY: 0, scaleX: 1 }, origin: "center bottom" }
      : { from: { scaleY: 0, scaleX: 1 }, origin: "center top" };
  };

  // Logout hover
  const handleLogoutEnter = (e: React.MouseEvent) => {
    const btn = (magLogout.ref as React.RefObject<HTMLButtonElement>).current;
    if (logoutBgRef.current && btn) {
      const { from, origin } = getDirection(e, btn);
      logoutBgRef.current.style.transformOrigin = origin;
      gsap.fromTo(logoutBgRef.current, { ...from }, {
        scaleX: 1, scaleY: 1, duration: 0.4, ease: "power4.out", overwrite: true,
      });
    }
    gsap.to(logoutTextRef.current, {
      color: "#1925aa", duration: 0.3, overwrite: true,
    });
  };

  const handleLogoutLeave = (e: React.MouseEvent) => {
    const btn = (magLogout.ref as React.RefObject<HTMLButtonElement>).current;
    if (logoutBgRef.current && btn) {
      const { from, origin } = getDirection(e, btn);
      logoutBgRef.current.style.transformOrigin = origin;
      gsap.to(logoutBgRef.current, {
        ...from, duration: 0.4, ease: "power4.out", overwrite: true,
      });
    }
    gsap.to(logoutTextRef.current, {
      color: "#e8e6e0", duration: 0.3, overwrite: true,
    });
  };

  // Settings hover (same pattern as Logout)
  const handleSettingsEnter = (e: React.MouseEvent) => {
    const btn = (magSettings.ref as React.RefObject<HTMLButtonElement>).current;
    if (settingsBgRef.current && btn) {
      const { from, origin } = getDirection(e, btn);
      settingsBgRef.current.style.transformOrigin = origin;
      gsap.fromTo(settingsBgRef.current, { ...from }, {
        scaleX: 1, scaleY: 1, duration: 0.4, ease: "power4.out", overwrite: true,
      });
    }
    gsap.to(settingsTextRef.current, {
      color: "#1925aa", duration: 0.3, overwrite: true,
    });
  };

  const handleSettingsLeave = (e: React.MouseEvent) => {
    const btn = (magSettings.ref as React.RefObject<HTMLButtonElement>).current;
    if (settingsBgRef.current && btn) {
      const { from, origin } = getDirection(e, btn);
      settingsBgRef.current.style.transformOrigin = origin;
      gsap.to(settingsBgRef.current, {
        ...from, duration: 0.4, ease: "power4.out", overwrite: true,
      });
    }
    gsap.to(settingsTextRef.current, {
      color: "#e8e6e0", duration: 0.3, overwrite: true,
    });
  };

  return (
    <header className="oci-section-blue">
      <div className="max-w-5xl mx-auto px-6 lg:px-10">
        {/* Row 1: TRENDS + SETTINGS / LOGOUT */}
        <div className="flex items-center justify-between py-5 border-b border-white/10">
          <h1
            ref={titleRef}
            className="oci-heading text-oci-mercury text-5xl cursor-default"
            onMouseEnter={handleTitleHover}
          >
            TRENDS
          </h1>

          <div className="flex items-center gap-3" style={{ lineHeight: 1 }}>
            {/* Settings button */}
            <button
              ref={magSettings.ref as React.RefObject<HTMLButtonElement>}
              onClick={onSettingsClick}
              onMouseEnter={handleSettingsEnter}
              onMouseMove={magSettings.onMouseMove}
              onMouseLeave={(e) => {
                handleSettingsLeave(e);
                magSettings.onMouseLeave();
              }}
              className="oci-btn border-oci-mercury/30"
            >
              <div
                ref={settingsBgRef}
                style={{
                  position: "absolute",
                  inset: 0,
                  backgroundColor: "#e8e6e0",
                  transform: "scaleY(0)",
                  transformOrigin: "center bottom",
                  zIndex: 0,
                  pointerEvents: "none",
                }}
              />
              <span
                ref={settingsTextRef}
                className="oci-btn__text oci-label-sm text-oci-mercury px-4 py-1.5"
              >
                Settings
              </span>
            </button>

            {/* Logout button */}
            <button
              ref={magLogout.ref as React.RefObject<HTMLButtonElement>}
              onClick={onLogout}
              onMouseEnter={handleLogoutEnter}
              onMouseMove={magLogout.onMouseMove}
              onMouseLeave={(e) => {
                handleLogoutLeave(e);
                magLogout.onMouseLeave();
              }}
              className="oci-btn border-oci-mercury/30"
            >
              <div
                ref={logoutBgRef}
                style={{
                  position: "absolute",
                  inset: 0,
                  backgroundColor: "#e8e6e0",
                  transform: "scaleY(0)",
                  transformOrigin: "center bottom",
                  zIndex: 0,
                  pointerEvents: "none",
                }}
              />
              <span
                ref={logoutTextRef}
                className="oci-btn__text oci-label-sm text-oci-mercury px-4 py-1.5"
              >
                Logout
              </span>
            </button>
          </div>
        </div>

        {/* Row 2: Date nav + Generated + Run + email */}
        <div className="flex items-center justify-between py-3">
          {/* Left: Date nav */}
          <div className="flex items-center">
            <button
              ref={magPrev.ref as React.RefObject<HTMLButtonElement>}
              onClick={handlePrevDay}
              onMouseMove={magPrev.onMouseMove}
              onMouseLeave={magPrev.onMouseLeave}
              className="text-oci-mercury/50 hover:text-oci-mercury transition-colors duration-300 p-1 -ml-6"
            >
              <svg
                xmlns="http://www.w3.org/2000/svg"
                className="h-4 w-4"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={1.5}
                  d="M15 19l-7-7 7-7"
                />
              </svg>
            </button>
            <DatePicker value={date} onChange={onDateChange} />
            <button
              ref={magNext.ref as React.RefObject<HTMLButtonElement>}
              onClick={handleNextDay}
              onMouseMove={magNext.onMouseMove}
              onMouseLeave={magNext.onMouseLeave}
              className="text-oci-mercury/50 hover:text-oci-mercury transition-colors duration-300 p-1 ml-1"
            >
              <svg
                xmlns="http://www.w3.org/2000/svg"
                className="h-4 w-4"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={1.5}
                  d="M9 5l7 7-7 7"
                />
              </svg>
            </button>
          </div>

          {/* Right: Generated + Run + email */}
          <div className="flex items-center gap-4">
            {generatedAt && (
              <span className="oci-label-sm text-oci-mercury/30 hidden sm:inline">
                Generated: {new Date(generatedAt).toLocaleString("ja-JP")}
              </span>
            )}
            {runId && (
              <span className="oci-label-sm text-oci-mercury/30 hidden sm:inline">
                Run: {runId.slice(0, 8)}
              </span>
            )}
            {userEmail && (
              <span className="oci-label-sm text-oci-mercury/30 hidden sm:inline">
                {userEmail}
              </span>
            )}
          </div>
        </div>
      </div>
    </header>
  );
}
