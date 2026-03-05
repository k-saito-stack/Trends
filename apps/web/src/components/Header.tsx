/**
 * Header — OCI style.
 * Blue bg, 2 rows (logo + nav), auto-scramble title, reveal-from-bottom logout.
 * Magnetic cursor on interactive elements.
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
}

export default function Header({
  date,
  onDateChange,
  onSettingsClick,
  onLogout,
  userEmail,
}: HeaderProps) {
  const titleRef = useRef<HTMLHeadingElement>(null);
  const logoutBgRef = useRef<HTMLDivElement>(null);
  const logoutTextRef = useRef<HTMLSpanElement>(null);
  const { scramble } = useScrambleText();

  // Magnetic cursor for interactive elements
  const magLogout = useMagnetic(0.3);
  const magPrev = useMagnetic(0.4);
  const magNext = useMagnetic(0.4);
  const magSettings = useMagnetic(0.3);

  // Auto-scramble TRENDS title every 8 seconds
  useEffect(() => {
    // Initial scramble on mount
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

  const handleLogoutEnter = () => {
    gsap.to(logoutBgRef.current, {
      scaleY: 1,
      duration: 0.5,
      ease: "power4.out",
      overwrite: true,
    });
    gsap.to(logoutTextRef.current, {
      color: "#1925aa",
      duration: 0.3,
      overwrite: true,
    });
  };

  const handleLogoutLeave = () => {
    gsap.to(logoutBgRef.current, {
      scaleY: 0,
      duration: 0.5,
      ease: "power4.out",
      overwrite: true,
    });
    gsap.to(logoutTextRef.current, {
      color: "#e8e6e0",
      duration: 0.3,
      overwrite: true,
    });
  };

  return (
    <header className="oci-section-blue">
      <div className="max-w-5xl mx-auto px-6 lg:px-10">
        {/* Row 1: Logo + user */}
        <div className="flex items-center justify-between py-5 border-b border-white/10">
          <h1
            ref={titleRef}
            className="oci-heading text-oci-mercury text-5xl cursor-default"
            onMouseEnter={handleTitleHover}
          >
            TRENDS
          </h1>

          <div className="flex items-center gap-5">
            {userEmail && (
              <span className="oci-label-sm text-oci-mercury/50 hidden sm:inline">
                {userEmail}
              </span>
            )}
            <button
              ref={magLogout.ref as React.RefObject<HTMLButtonElement>}
              onClick={onLogout}
              onMouseEnter={handleLogoutEnter}
              onMouseMove={magLogout.onMouseMove}
              onMouseLeave={() => {
                handleLogoutLeave();
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

        {/* Row 2: Date nav + settings */}
        <div className="flex items-center justify-between py-3">
          {/* Date nav — arrow sticks out left, date box aligns with card content */}
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

          <button
            ref={magSettings.ref as React.RefObject<HTMLButtonElement>}
            onClick={onSettingsClick}
            onMouseMove={magSettings.onMouseMove}
            onMouseLeave={magSettings.onMouseLeave}
            className="oci-link text-oci-mercury/50 gap-2"
          >
            <span className="oci-link__dot" style={{ backgroundColor: "#e8e6e0" }} />
            <span className="oci-label-sm">Settings</span>
          </button>
        </div>
      </div>
    </header>
  );
}
