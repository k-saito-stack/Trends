/**
 * Header — OCI style.
 * Blue bg, 2 rows:
 *   Row 1: TRENDS + SETTINGS/LOGOUT buttons
 *   Row 2: Date nav + Generated + Run + email
 */
import { useRef, useEffect, useCallback } from "react";
import { gsap } from "../hooks/useGSAPSetup";
import { useMagnetic } from "../hooks/useMagnetic";
import { useScrambleText } from "../hooks/useScrambleText";
import { addDaysToIsoDate, getTodayJstIsoDate } from "../utils/date";
import DatePicker from "./DatePicker";
import ScrambleBackground from "./ScrambleBackground";

/** Secondary fonts for TRENDS title alternation */
const SECONDARY_FONTS = [
  "Bangers",
  "Bitcount Prop Double",
  "Emilys Candy",
  "Jacquard 12",
  "Jost",
  "Kings",
  "Luckiest Guy",
  "Michroma",
  "Micro 5",
  "Pacifico",
  "Pinyon Script",
  "Quicksand",
  "Schoolbell",
  "Ballet",
  "Black Ops One",
  "Cinzel",
  "Henny Penny",
  "Jaro",
  "Metal Mania",
  "Monsieur La Doulaise",
  "Princess Sofia",
  "Rock Salt",
  "Rubik Distressed",
  "Special Elite",
];

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
  const generatedRef = useRef<HTMLSpanElement>(null);
  const runRef = useRef<HTMLSpanElement>(null);
  const emailRef = useRef<HTMLSpanElement>(null);
  const logoutBgRef = useRef<HTMLDivElement>(null);
  const logoutTextRef = useRef<HTMLSpanElement>(null);
  const settingsBgRef = useRef<HTMLDivElement>(null);
  const settingsTextRef = useRef<HTMLSpanElement>(null);
  const { scramble } = useScrambleText();
  const isPrimaryRef = useRef(true);

  // Display texts for Row 2 info
  const generatedText = generatedAt
    ? `Generated: ${new Date(generatedAt).toLocaleString("ja-JP")}`
    : "";
  const runText = runId ? `Run: ${runId.slice(0, 8)}` : "";
  const emailText = userEmail || "";

  // Magnetic cursor for interactive elements
  const magLogout = useMagnetic(0.3);
  const magSettings = useMagnetic(0.3);
  const magPrev = useMagnetic(0.4);
  const magNext = useMagnetic(0.4);

  // Chain scramble: TRENDS → Generated → Run → email
  // Use refs to always read latest text without recreating interval
  const generatedTextRef = useRef(generatedText);
  const runTextRef = useRef(runText);
  const emailTextRef = useRef(emailText);
  generatedTextRef.current = generatedText;
  runTextRef.current = runText;
  emailTextRef.current = emailText;

  const runChainScramble = useCallback(async () => {
    if (titleRef.current) {
      const usePrimary = isPrimaryRef.current;
      isPrimaryRef.current = !isPrimaryRef.current;

      if (usePrimary) {
        titleRef.current.style.fontFamily = '"Zen Kaku Gothic New", system-ui, sans-serif';
        titleRef.current.style.letterSpacing = "-0.02em";
        titleRef.current.style.textTransform = "uppercase";
        await scramble(titleRef.current, "TRENDS", 500);
      } else {
        const font = SECONDARY_FONTS[Math.floor(Math.random() * SECONDARY_FONTS.length)];
        titleRef.current.style.fontFamily = `"${font}", system-ui, sans-serif`;
        titleRef.current.style.letterSpacing = "normal";
        titleRef.current.style.textTransform = "none";
        await scramble(titleRef.current, "Trends", 500);
      }
    }
    if (generatedRef.current && generatedTextRef.current)
      await scramble(generatedRef.current, generatedTextRef.current, 400);
    if (runRef.current && runTextRef.current)
      await scramble(runRef.current, runTextRef.current, 300);
    if (emailRef.current && emailTextRef.current)
      await scramble(emailRef.current, emailTextRef.current, 400);
  }, [scramble]);

  useEffect(() => {
    const initTimer = setTimeout(() => runChainScramble(), 800);
    const interval = setInterval(() => runChainScramble(), 8000);
    return () => {
      clearTimeout(initTimer);
      clearInterval(interval);
    };
  }, [runChainScramble]);

  const handlePrevDay = () => {
    onDateChange(addDaysToIsoDate(date, -1));
  };

  const handleNextDay = () => {
    const today = getTodayJstIsoDate();
    const next = addDaysToIsoDate(date, 1);
    if (next <= today) onDateChange(next);
  };

  const handleTitleHover = () => {
    if (titleRef.current) {
      // Re-scramble with current text (preserve primary/secondary state)
      const currentText = titleRef.current.textContent || "TRENDS";
      scramble(titleRef.current, currentText);
    }
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
    <header className="oci-section-blue sticky top-0 z-30 relative overflow-hidden">
      <ScrambleBackground color={[232, 230, 224]} mode="inline" />
      <div className="max-w-5xl mx-auto px-6 lg:px-10 relative z-10">
        {/* Row 1: TRENDS + SETTINGS / LOGOUT */}
        <div className="flex items-center justify-between py-5 border-b border-white/10">
          <h1
            ref={titleRef}
            className="oci-heading text-oci-mercury text-6xl cursor-default"
            style={{ height: "1em", lineHeight: 1, display: "flex", alignItems: "center" }}
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
              <span ref={generatedRef} className="oci-label-sm text-oci-mercury hidden sm:inline">
                {generatedText}
              </span>
            )}
            {runId && (
              <span ref={runRef} className="oci-label-sm text-oci-mercury hidden sm:inline">
                {runText}
              </span>
            )}
            {userEmail && (
              <span ref={emailRef} className="oci-label-sm text-oci-mercury hidden sm:inline">
                {emailText}
              </span>
            )}
          </div>
        </div>
      </div>
    </header>
  );
}
